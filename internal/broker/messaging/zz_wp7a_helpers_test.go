package messaging

import (
	"fmt"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Helpers the WP7a messaging tests share.

// zzWP7aStackCount counts the goroutines whose stack mentions fn.
func zzWP7aStackCount(fn string) int {
	buf := make([]byte, 1<<20)
	for {
		n := goruntime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), fn)
		}
		buf = make([]byte, 2*len(buf))
	}
}

// zzWP7aWaitStack waits until ok holds for the goroutine stacks, as
// read by count.
func zzWP7aWaitStack(t *testing.T, what string, ok func(count func(string) int) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok(zzWP7aStackCount) {
			// The frames are there; give the goroutines a moment to reach
			// the blocking call inside them.
			time.Sleep(5 * time.Millisecond)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// zzWP7aHoldProduceLock holds the partition's produce lock, as a commit
// inside its append and fsync does, until the returned release is
// called.
func zzWP7aHoldProduceLock(t *testing.T, e *Engine, topicName string, partition int) (release func()) {
	t.Helper()
	held := make(chan struct{})
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = e.logs.WithProduceLock(topicName, partition, func(*storage.Log) error {
			close(held)
			<-done
			return nil
		})
	}()
	<-held
	var once sync.Once
	release = func() {
		once.Do(func() {
			close(done)
			<-finished
		})
	}
	t.Cleanup(release)
	return release
}

func zzWP7aRecords(topicName, topicID string, partition, n int, tag string) []ingress.ProduceRecord {
	records := make([]ingress.ProduceRecord, n)
	for i := range records {
		records[i] = ingress.ProduceRecord{
			Topic: topicName, TopicID: topicID, TargetPartition: partition,
			Key: fmt.Sprintf("%s-%d", tag, i), Payload: []byte(fmt.Sprintf(`{"tag":%q,"i":%d}`, tag, i)),
		}
	}
	return records
}

func zzWP7aHWM(t *testing.T, e *Engine, topicName string, partition int) int64 {
	t.Helper()
	log, err := e.logs.Get(topicName, partition)
	if err != nil {
		t.Fatal(err)
	}
	return log.HighWatermark()
}
