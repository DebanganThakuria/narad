package runtime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestWithProduceLockMutualExclusionAcrossCloseTopic pins the produce
// mutual-exclusion guarantee against concurrent CloseTopic calls on a
// LIVE topic (the UpdateTopicRetention path). Before lockProduce
// revalidated its mutex against the map, CloseTopic could delete a
// produceSync entry while a producer was inside its critical section:
// the next producer minted a fresh mutex for the same partition and
// both ran the append+sync+HWM section concurrently. The test hammers
// WithProduceLock against a CloseTopic loop and asserts no two
// goroutines are ever inside the section at once.
func TestWithProduceLockMutualExclusionAcrossCloseTopic(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 60_000}
	logs := newRuntimeTestLogs(t, ms)
	t.Cleanup(func() { _ = logs.CloseAll() })

	var (
		inSection  atomic.Int32
		violations atomic.Int32
	)

	const (
		producers  = 6
		iterations = 150
	)

	stop := make(chan struct{})
	var closerWG sync.WaitGroup
	closerWG.Add(1)
	go func() {
		defer closerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := logs.CloseTopic("orders"); err != nil {
				t.Errorf("CloseTopic() error = %v", err)
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	var producerWG sync.WaitGroup
	for range producers {
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			for range iterations {
				err := logs.WithProduceLock("orders", 0, func(*storage.Log) error {
					if inSection.Add(1) != 1 {
						violations.Add(1)
					}
					// Widen the critical section so a second entrant
					// (a produce running on a freshly minted mutex
					// while we still hold the retired one) is caught.
					time.Sleep(20 * time.Microsecond)
					inSection.Add(-1)
					return nil
				})
				if err != nil {
					t.Errorf("WithProduceLock() error = %v", err)
					return
				}
			}
		}()
	}

	producerWG.Wait()
	close(stop)
	closerWG.Wait()

	if got := violations.Load(); got != 0 {
		t.Fatalf("produce critical section entered concurrently %d times; CloseTopic retired a held produce mutex", got)
	}

	// The original leak must stay fixed: CloseTopic retires the topic's
	// produceSync entries, so churn leaves at most the entries recreated
	// by producers that ran after the last CloseTopic.
	if err := logs.CloseTopic("orders"); err != nil {
		t.Fatalf("final CloseTopic() error = %v", err)
	}
	if got := logs.ProduceSyncCount(); got != 0 {
		t.Fatalf("ProduceSyncCount() after final CloseTopic = %d, want 0 (produceSync entries leaked)", got)
	}
}
