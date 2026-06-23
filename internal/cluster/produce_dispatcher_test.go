package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeProduceCommitter struct {
	records []ingress.ProduceRecord
	err     error
}

func (f *fakeProduceCommitter) CommitAcceptedProduce(_ context.Context, record ingress.ProduceRecord) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.records = append(f.records, record)
	return int64(len(f.records) - 1), nil
}

func (f *fakeProduceCommitter) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	offsets := make([]int64, len(records))
	for i, record := range records {
		f.records = append(f.records, record)
		offsets[i] = int64(len(f.records) - 1)
	}
	return offsets, nil
}

type failAfterProduceCommitter struct {
	records []ingress.ProduceRecord
	failAt  int
	err     error
}

func (f *failAfterProduceCommitter) CommitAcceptedProduce(_ context.Context, record ingress.ProduceRecord) (int64, error) {
	if len(f.records) == f.failAt {
		return 0, f.err
	}
	f.records = append(f.records, record)
	return int64(len(f.records) - 1), nil
}

func TestProduceDispatcherCommitsLocalOwnerAndCheckpoints(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	manager := newDispatchIngressManager(t)
	accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if len(committer.records) != 1 {
		t.Fatalf("committed records = %d, want 1", len(committer.records))
	}
	if committer.records[0].MessageID != accepted.MessageID || string(committer.records[0].Payload) != `{"id":1}` {
		t.Fatalf("committed record = %+v", committer.records[0])
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

func TestProduceDispatcherStopsAtBatchSize(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	manager := newDispatchIngressManager(t)
	for i := range 2 {
		if _, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{BatchSize: 1})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}

	processed, err = dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("second DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("second processed = %d, want 1", processed)
	}
	nextSeq, err = manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("second LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 2 {
		t.Fatalf("second checkpoint = %d, want 2", nextSeq)
	}
}

func TestProduceDispatcherCheckpointsSuccessfulPrefixBeforeFailure(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	manager := newDispatchIngressManager(t)
	for i := range 2 {
		if _, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
	}
	commitErr := errors.New("commit failed")
	committer := &failAfterProduceCommitter{failAt: 1, err: commitErr}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if !errors.Is(err, commitErr) {
		t.Fatalf("DispatchAvailable() error = %v, want %v", err, commitErr)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

func TestProduceDispatcherCommitsRemoteOwnerAndCheckpoints(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	manager := newDispatchIngressManager(t)
	accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	var gotAddr string
	var gotReq nodewire.CommitProduceBatchRequest
	dispatcher := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, fakePeerClient{
		commitProduceBatchFn: func(_ context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			gotAddr = addr
			gotReq = req
			return nodewire.Response{Status: http.StatusOK}, nil
		},
	}, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if gotAddr != "remote.example:7942" {
		t.Fatalf("remote addr = %q, want remote.example:7942", gotAddr)
	}
	if len(gotReq.Records) != 1 {
		t.Fatalf("remote batch records = %d, want 1", len(gotReq.Records))
	}
	gotRecord := gotReq.Records[0]
	if gotRecord.MessageID != accepted.MessageID ||
		gotRecord.Topic != "orders" ||
		gotRecord.Key != "customer-1" ||
		gotRecord.TargetPartition != 0 ||
		string(gotRecord.Payload) != `{"id":1}` {
		t.Fatalf("remote request = %+v", gotReq)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

func TestProduceDispatcherDoesNotCheckpointFailedRecord(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	manager := newDispatchIngressManager(t)
	if _, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, fakePeerClient{
		commitProduceBatchFn: func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			return nodewire.Response{Status: http.StatusServiceUnavailable}, nil
		},
	}, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err == nil {
		t.Fatal("DispatchAvailable() error = nil, want error")
	}
	if processed != 0 {
		t.Fatalf("processed = %d, want 0", processed)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 0 {
		t.Fatalf("checkpoint = %d, want 0", nextSeq)
	}
}

// TestProduceDispatcherDiscardsRecordsForDeletedTopic verifies that
// undispatched WAL records whose topic was deleted are discarded (the
// checkpoint advances past them) instead of head-of-line-blocking the
// shard forever — delete also removes the assignment, so dispatchTarget
// fails for these records.
func TestProduceDispatcherDiscardsRecordsForDeletedTopic(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	manager := newDispatchIngressManager(t)
	if _, err := manager.AcceptProduce(context.Background(), "orders", "c1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if err := store.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v, want nil (records discarded)", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1 (discarded)", processed)
	}
	if len(committer.records) != 0 {
		t.Fatalf("committed = %d, want 0 (topic deleted, nothing committed)", len(committer.records))
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1 (advanced past discarded record)", nextSeq)
	}
}

// TestProduceDispatcherRetriesCommitFailureWhileTopicExists is the
// no-data-loss guard: a commit failure for a topic that STILL EXISTS must
// never be discarded — the checkpoint must not advance, so the record is
// retried.
func TestProduceDispatcherRetriesCommitFailureWhileTopicExists(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	manager := newDispatchIngressManager(t)
	if _, err := manager.AcceptProduce(context.Background(), "orders", "c1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	committer := &fakeProduceCommitter{err: errors.New("commit boom")}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	if _, err := dispatcher.DispatchAvailable(context.Background()); err == nil {
		t.Fatal("DispatchAvailable() error = nil, want error (topic exists -> retry, not discard)")
	}
	if len(committer.records) != 0 {
		t.Fatalf("committed = %d, want 0", len(committer.records))
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 0 {
		t.Fatalf("checkpoint = %d, want 0 (record retained for retry)", nextSeq)
	}
}

func TestProduceDispatcherReplaysAcceptedWALAfterRestart(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()
	manager := newDispatchIngressManagerAt(t, dir)
	accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := newDispatchIngressManagerAt(t, dir)
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(reopened, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	if len(committer.records) != 1 {
		t.Fatalf("committed records = %d, want 1", len(committer.records))
	}
	if committer.records[0].MessageID != accepted.MessageID || string(committer.records[0].Payload) != `{"id":1}` {
		t.Fatalf("committed record = %+v", committer.records[0])
	}
	nextSeq, err := reopened.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

func TestProduceDispatcherRetriesFailedRemoteRecordAfterRestart(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	dir := t.TempDir()
	manager := newDispatchIngressManagerAt(t, dir)
	accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, fakePeerClient{
		commitProduceBatchFn: func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			return nodewire.Response{Status: http.StatusServiceUnavailable}, nil
		},
	}, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err == nil {
		t.Fatal("DispatchAvailable() error = nil, want error")
	}
	if processed != 0 {
		t.Fatalf("processed = %d, want 0", processed)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := newDispatchIngressManagerAt(t, dir)
	var gotReq nodewire.CommitProduceBatchRequest
	retryDispatcher := NewProduceDispatcher(reopened, store, "node-self", &fakeProduceCommitter{}, fakePeerClient{
		commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			gotReq = req
			return nodewire.Response{Status: http.StatusOK}, nil
		},
	}, nil, ProduceDispatcherConfig{})

	processed, err = retryDispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("retry DispatchAvailable() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("retry processed = %d, want 1", processed)
	}
	if len(gotReq.Records) != 1 || gotReq.Records[0].MessageID != accepted.MessageID || string(gotReq.Records[0].Payload) != `{"id":1}` {
		t.Fatalf("retried request = %+v", gotReq)
	}
	nextSeq, err := reopened.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

func TestProduceDispatcherDoesNotReplayCheckpointedRecordAfterRestart(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()
	manager := newDispatchIngressManagerAt(t, dir)
	if _, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})
	if processed, err := dispatcher.DispatchAvailable(context.Background()); err != nil || processed != 1 {
		t.Fatalf("DispatchAvailable() = processed %d err %v, want processed 1 err nil", processed, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := newDispatchIngressManagerAt(t, dir)
	afterRestartCommitter := &fakeProduceCommitter{}
	afterRestart := NewProduceDispatcher(reopened, store, "node-self", afterRestartCommitter, nil, nil, ProduceDispatcherConfig{})
	processed, err := afterRestart.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("after restart DispatchAvailable() error = %v", err)
	}
	if processed != 0 {
		t.Fatalf("after restart processed = %d, want 0", processed)
	}
	if len(afterRestartCommitter.records) != 0 {
		t.Fatalf("after restart committed records = %d, want 0", len(afterRestartCommitter.records))
	}
}

func TestProduceDispatcherDispatchesAllWALShards(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 4)
	manager := newShardedDispatchIngressManager(t, 4)
	acceptedByID := make(map[string]ingress.AcceptedProduce)
	recordsPerShard := make(map[int]int)
	for i := range 16 {
		partition := i % 4
		accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", partition, []byte(`{"id":1}`))
		if err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
		acceptedByID[accepted.MessageID] = accepted
		recordsPerShard[accepted.WALShard]++
	}
	if len(recordsPerShard) < 2 {
		t.Fatalf("records only used %d WAL shard(s), want multiple", len(recordsPerShard))
	}
	for shard := range 4 {
		if got := recordsPerShard[shard]; got != 4 {
			t.Fatalf("records on shard %d = %d, want 4", shard, got)
		}
	}

	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})
	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != len(acceptedByID) {
		t.Fatalf("processed = %d, want %d", processed, len(acceptedByID))
	}
	if len(committer.records) != len(acceptedByID) {
		t.Fatalf("committed records = %d, want %d", len(committer.records), len(acceptedByID))
	}
	for _, record := range committer.records {
		accepted, ok := acceptedByID[record.MessageID]
		if !ok {
			t.Fatalf("committed unknown record %+v", record)
		}
		if record.WALShard != accepted.WALShard {
			t.Fatalf("committed shard = %d, want %d", record.WALShard, accepted.WALShard)
		}
	}
	for shard, want := range recordsPerShard {
		checkpoint, err := manager.LoadProduceCheckpointForShard(shard)
		if err != nil {
			t.Fatalf("LoadProduceCheckpointForShard(%d) error = %v", shard, err)
		}
		if checkpoint != uint64(want) {
			t.Fatalf("LoadProduceCheckpointForShard(%d) = %d, want %d", shard, checkpoint, want)
		}
	}
}

func seedProduceDispatchTopic(t *testing.T, store *metastore.Store, ownerID string) {
	t.Helper()
	seedProduceDispatchTopicPartitions(t, store, ownerID, 1)
}

func seedProduceDispatchTopicPartitions(t *testing.T, store *metastore.Store, ownerID string, partitions int) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: partitions}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	for partition := range partitions {
		if err := store.AssignPartition(ctx, "orders", partition, ownerID); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", partition, err)
		}
	}
}

func newDispatchIngressManager(t *testing.T) *ingress.Manager {
	t.Helper()
	return newDispatchIngressManagerAt(t, t.TempDir())
}

func newDispatchIngressManagerAt(t *testing.T, dir string) *ingress.Manager {
	t.Helper()
	manager, err := ingress.OpenManager(dir, wal.Options{
		SegmentBytes: 1024,
		SyncInterval: time.Hour,
		SyncBytes:    1,
		MaxRecord:    1024,
	})
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func newShardedDispatchIngressManager(t *testing.T, shards int) *ingress.Manager {
	t.Helper()
	manager, err := ingress.OpenManagerWithOptions(t.TempDir(), ingress.Options{
		WAL: wal.Options{
			SegmentBytes: 1024,
			SyncInterval: time.Hour,
			SyncBytes:    1,
			MaxRecord:    1024,
		},
		Shards: shards,
	})
	if err != nil {
		t.Fatalf("OpenManagerWithOptions() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}
