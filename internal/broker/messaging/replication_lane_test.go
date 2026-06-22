package messaging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/replication"
)

type recordingBatchReplicator struct {
	mu      sync.Mutex
	batches [][]replication.Record
}

func (r *recordingBatchReplicator) Replicate(_ context.Context, topic string, partition int, offset int64, payload []byte) error {
	return r.ReplicateBatch(context.Background(), topic, partition, []replication.Record{{Offset: offset, Payload: payload}})
}

func (r *recordingBatchReplicator) ReplicateBatch(_ context.Context, _ string, _ int, records []replication.Record) error {
	cp := make([]replication.Record, len(records))
	for i, record := range records {
		cp[i] = replication.Record{
			Offset:  record.Offset,
			Payload: append([]byte(nil), record.Payload...),
		}
	}
	r.mu.Lock()
	r.batches = append(r.batches, cp)
	r.mu.Unlock()
	return nil
}

func TestReplicationLaneCompletesOutOfOrderJobsButCommitsContiguously(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &recordingBatchReplicator{}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	offset0, err := log.Append([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	offset1, err := log.Append([]byte(`{"id":2}`))
	if err != nil {
		t.Fatalf("Append(1) error = %v", err)
	}
	lane := newReplicationLane(engine, "orders", 0)
	job0 := &replicationJob{log: log, offset: offset0, payload: []byte(`{"id":1}`), done: make(chan error, 1)}
	job1 := &replicationJob{log: log, offset: offset1, payload: []byte(`{"id":2}`), done: make(chan error, 1)}

	lane.enqueue(job1)
	select {
	case err := <-job1.done:
		if err != nil {
			t.Fatalf("job1 error = %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("job1 did not complete after follower acceptance")
	}
	if got := log.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() after offset 1 = %d, want 0", got)
	}

	lane.enqueue(job0)
	select {
	case err := <-job0.done:
		if err != nil {
			t.Fatalf("job0 error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("job0 did not complete")
	}
	if got := log.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() = %d, want 2", got)
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	if len(replicator.batches) != 2 {
		t.Fatalf("replications = %d, want 2", len(replicator.batches))
	}
	if got := replicator.batches[0]; len(got) != 1 || got[0].Offset != 1 {
		t.Fatalf("first replication = %+v, want offset 1", got)
	}
	if got := replicator.batches[1]; len(got) != 1 || got[0].Offset != 0 {
		t.Fatalf("second replication = %+v, want offset 0", got)
	}
}

func TestReplicationLaneBatchesContiguousQueuedJobs(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &recordingBatchReplicator{}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	offset0, err := log.Append([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	offset1, err := log.Append([]byte(`{"id":2}`))
	if err != nil {
		t.Fatalf("Append(1) error = %v", err)
	}

	lane := newReplicationLaneWithBatchLinger(engine, "orders", 0, 20*time.Millisecond)
	job0 := &replicationJob{log: log, offset: offset0, payload: []byte(`{"id":1}`), done: make(chan error, 1)}
	job1 := &replicationJob{log: log, offset: offset1, payload: []byte(`{"id":2}`), done: make(chan error, 1)}

	if err := lane.enqueue(job0); err != nil {
		t.Fatalf("enqueue(0) error = %v", err)
	}
	if err := lane.enqueue(job1); err != nil {
		t.Fatalf("enqueue(1) error = %v", err)
	}
	for i, job := range []*replicationJob{job0, job1} {
		select {
		case err := <-job.done:
			if err != nil {
				t.Fatalf("job%d error = %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("job%d did not complete", i)
		}
	}

	if got := log.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() = %d, want 2", got)
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	if len(replicator.batches) != 1 {
		t.Fatalf("replication batches = %d, want 1", len(replicator.batches))
	}
	got := replicator.batches[0]
	if len(got) != 2 || got[0].Offset != 0 || got[1].Offset != 1 {
		t.Fatalf("replication batch = %+v, want offsets [0 1]", got)
	}
}

type contextCapturingBatchReplicator struct {
	deadline    time.Time
	hasDeadline bool
}

func (r *contextCapturingBatchReplicator) Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error {
	return r.ReplicateBatch(ctx, topic, partition, []replication.Record{{Offset: offset, Payload: payload}})
}

func (r *contextCapturingBatchReplicator) ReplicateBatch(ctx context.Context, _ string, _ int, _ []replication.Record) error {
	r.deadline, r.hasDeadline = ctx.Deadline()
	return nil
}

func TestReplicationLaneUsesBoundedReplicationContext(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &contextCapturingBatchReplicator{}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	offset0, err := log.Append([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	lane := newReplicationLane(engine, "orders", 0)
	job := &replicationJob{log: log, offset: offset0, payload: []byte(`{"id":1}`), done: make(chan error, 1)}
	lane.process(job)

	if !replicator.hasDeadline {
		t.Fatal("replication context has no deadline")
	}
	remaining := time.Until(replicator.deadline)
	if remaining <= 0 || remaining > replicationOperationTimeout {
		t.Fatalf("replication context deadline remaining = %v, want within %v", remaining, replicationOperationTimeout)
	}
}

type blockingBatchReplicator struct {
	ctxCh chan context.Context
}

func (r *blockingBatchReplicator) Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error {
	return r.ReplicateBatch(ctx, topic, partition, []replication.Record{{Offset: offset, Payload: payload}})
}

func (r *blockingBatchReplicator) ReplicateBatch(ctx context.Context, _ string, _ int, _ []replication.Record) error {
	r.ctxCh <- ctx
	<-ctx.Done()
	return ctx.Err()
}

func TestEngineCloseCancelsReplicationLaneContext(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &blockingBatchReplicator{ctxCh: make(chan context.Context, 1)}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	offset0, err := log.Append([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	job := &replicationJob{log: log, offset: offset0, payload: []byte(`{"id":1}`), done: make(chan error, 1)}
	lane := newReplicationLane(engine, "orders", 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lane.process(job)
	}()

	select {
	case <-replicator.ctxCh:
	case <-time.After(time.Second):
		t.Fatal("replicator was not called")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replication lane did not stop after engine close")
	}
	err = <-job.done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("job error = %v, want context.Canceled", err)
	}
}
