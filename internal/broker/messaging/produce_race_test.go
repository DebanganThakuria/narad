package messaging

import (
	"context"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

type gatedReplicator struct {
	mu       sync.Mutex
	gates    map[int64]chan struct{}
	released []int64
}

func newGatedReplicator() *gatedReplicator {
	return &gatedReplicator{gates: make(map[int64]chan struct{})}
}

func (r *gatedReplicator) gate(offset int64) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{})
	r.gates[offset] = ch
	return ch
}

func (r *gatedReplicator) Replicate(_ context.Context, _ string, _ int, offset int64, _ []byte) error {
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

func TestProduceSerializesPerPartitionHighWatermarkAdvance(t *testing.T) {
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
	go func() {
		defer wg.Done()
		_, _, err := engine.Produce(ctx, "orders", "k2", []byte(`{"id":2}`))
		errCh <- err
	}()

	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
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
