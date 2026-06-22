package messaging

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

type gatedReplicator struct {
	mu       sync.Mutex
	gates    map[int64]chan struct{}
	released []int64
	started  chan int64
}

func newGatedReplicator() *gatedReplicator {
	return &gatedReplicator{
		gates:   make(map[int64]chan struct{}),
		started: make(chan int64, 16),
	}
}

func (r *gatedReplicator) gate(offset int64) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.gates[offset] = ch
	return ch
}

func (r *gatedReplicator) Replicate(_ context.Context, _ string, _ int, offset int64, _ []byte) error {
	select {
	case r.started <- offset:
	default:
	}
	r.mu.Lock()
	ch, ok := r.gates[offset]
	r.mu.Unlock()
	if ok {
		<-ch
	}
	r.mu.Lock()
	r.released = append(r.released, offset)
	r.mu.Unlock()
	return nil
}

func TestProduceAppendsNextOffsetWhilePreviousReplicationWaits(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := newGatedReplicator()
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)

	gate0 := replicator.gate(0)
	gate1 := replicator.gate(1)

	ctx := context.Background()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _, err := engine.Produce(ctx, "orders", "k1", []byte(`{"id":1}`))
		errCh <- err
	}()
	if got := <-replicator.started; got != 0 {
		t.Fatalf("first replicated offset = %d, want 0", got)
	}
	go func() {
		defer wg.Done()
		_, _, err := engine.Produce(ctx, "orders", "k2", []byte(`{"id":2}`))
		errCh <- err
	}()

	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	eventually(t, func() bool { return log.NextOffset() == 2 })
	if got := log.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() before releasing offset 0 = %d, want 0", got)
	}

	close(gate1)
	if got := log.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() after releasing offset 1 = %d, want 0", got)
	}

	close(gate0)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Produce() error = %v", err)
		}
	}

	if got := log.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() final = %d, want 2", got)
	}

	if got := engine.logs.ProduceSyncCount(); got != 1 {
		t.Fatalf("ProduceSyncCount() = %d, want 1", got)
	}
}

func eventually(t *testing.T, ok func() bool) {
	t.Helper()
	for range 100 {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}
