package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeProduceCommitter struct {
	mu         sync.Mutex
	records    []ingress.ProduceRecord
	batchSizes []int
	err        error
}

func (f *fakeProduceCommitter) CommitAcceptedProduce(_ context.Context, record ingress.ProduceRecord) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	f.records = append(f.records, record)
	return int64(len(f.records) - 1), nil
}

func (f *fakeProduceCommitter) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.batchSizes = append(f.batchSizes, len(records))
	offsets := make([]int64, len(records))
	for i, record := range records {
		f.records = append(f.records, record)
		offsets[i] = int64(len(f.records) - 1)
	}
	return offsets, nil
}

func (f *fakeProduceCommitter) committed() []ingress.ProduceRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ingress.ProduceRecord(nil), f.records...)
}

func (f *fakeProduceCommitter) batchCalls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.batchSizes...)
}

// perPartitionCommitter commits everything except records whose partition is
// listed in failPartitions (or whose topic is listed in failTopics), for
// which it returns the configured error. It counts commit attempts per
// partition, including failed ones. It is thread-safe for the parallel
// commit path.
type perPartitionCommitter struct {
	mu             sync.Mutex
	records        []ingress.ProduceRecord
	failPartitions map[int]error
	failTopics     map[string]error
	attempts       map[int]int
}

func (c *perPartitionCommitter) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(records) > 0 {
		if c.attempts == nil {
			c.attempts = map[int]int{}
		}
		c.attempts[records[0].TargetPartition]++
		if err, ok := c.failTopics[records[0].Topic]; ok {
			return nil, err
		}
		if err, ok := c.failPartitions[records[0].TargetPartition]; ok {
			return nil, err
		}
	}
	offsets := make([]int64, len(records))
	for i, record := range records {
		c.records = append(c.records, record)
		offsets[i] = int64(len(c.records) - 1)
	}
	return offsets, nil
}

func (c *perPartitionCommitter) CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (int64, error) {
	offsets, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{record})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (c *perPartitionCommitter) committedPartitions() map[int]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := map[int]int{}
	for _, r := range c.records {
		counts[r.TargetPartition]++
	}
	return counts
}

func (c *perPartitionCommitter) committedTopics() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := map[string]int{}
	for _, r := range c.records {
		counts[r.Topic]++
	}
	return counts
}

func (c *perPartitionCommitter) attemptCount(partition int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts[partition]
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
	if committer.records[0].WAL != accepted.WAL || string(committer.records[0].Payload) != `{"id":1}` {
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

// A bucket that fails to commit bounds the checkpoint at its lowest seq:
// records before it commit and advance, records from it onward are retried.
// seq0->p0 (commits), seq1->p1 (fails), seq2->p0 (commits but is beyond the
// checkpoint, so it re-commits next pass — an accepted at-least-once dup).
func TestProduceDispatcherCheckpointsToFirstUncommittedPartition(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 3)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	for _, p := range []int{0, 1, 0} {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	commitErr := errors.New("p1 owner down")
	committer := &perPartitionCommitter{failPartitions: map[int]error{1: commitErr}}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(ctx)
	if !errors.Is(err, commitErr) {
		t.Fatalf("error = %v, want %v", err, commitErr)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1 (only seq0 before the failed seq1)", processed)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("checkpoint = %d, want 1", nextSeq)
	}
}

// After the failing partition recovers, the dispatcher drains everything
// (no data loss). It may re-commit higher-seq records that committed before
// the failure — the accepted at-least-once duplicate — but every record is
// delivered at least once and the checkpoint reaches the end.
func TestProduceDispatcherConvergesAfterPartitionRecovers(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 3)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	for _, p := range []int{0, 1, 0} {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	committer := &perPartitionCommitter{failPartitions: map[int]error{1: errors.New("p1 owner down")}}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
		t.Fatal("first pass: want commit error while p1 is down")
	}

	// p1 recovers.
	committer.mu.Lock()
	committer.failPartitions = nil
	committer.mu.Unlock()

	for i := range 5 {
		if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
			t.Fatalf("drain pass %d error = %v", i, err)
		}
		if n, _ := manager.LoadProduceCheckpoint(); n >= 3 {
			break
		}
	}
	if nextSeq, _ := manager.LoadProduceCheckpoint(); nextSeq != 3 {
		t.Fatalf("checkpoint = %d, want 3 after recovery", nextSeq)
	}
	// Exactly-once in steady state: the skip-set prevented p0's seq2 (which
	// committed before p1 recovered) from being re-committed.
	counts := committer.committedPartitions()
	if counts[0] != 2 || counts[1] != 1 {
		t.Fatalf("committed partitions = %v, want exactly p0=2, p1=1 (no duplicate)", counts)
	}
}

// While a head partition is stuck, neighbours that already committed must
// not be re-committed on subsequent passes — the skip-set is what keeps
// healthy operation duplicate-free.
func TestProduceDispatcherDoesNotRecommitAheadOfStuckPartition(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 3)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	// seq0->p1 (stuck head), seq1->p0, seq2->p0.
	for _, p := range []int{1, 0, 0} {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	committer := &perPartitionCommitter{failPartitions: map[int]error{1: errors.New("p1 down")}}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	for range 3 {
		if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
			t.Fatal("want error while p1 is down")
		}
	}
	if got := committer.committedPartitions()[0]; got != 2 {
		t.Fatalf("p0 commits = %d across 3 passes, want 2 (no re-commit while p1 stuck)", got)
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != 0 {
		t.Fatalf("checkpoint = %d, want 0 (stuck head p1 blocks it)", n)
	}
}

// The core throughput fix: interleaved records across many partitions must
// be grouped into a few large per-partition batches (one fsync each), not a
// batch-per-record.
func TestProduceDispatcherGroupsInterleavedRecordsIntoFewBatches(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 4)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	const records = 40
	for i := range records {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", i%4, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(ctx)
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != records {
		t.Fatalf("processed = %d, want %d", processed, records)
	}
	calls := committer.batchCalls()
	if len(calls) > 4 {
		t.Fatalf("batch calls = %d (sizes %v), want <= 4 (one per partition)", len(calls), calls)
	}
	for _, n := range calls {
		if n <= 1 {
			t.Fatalf("batch sizes = %v, want each > 1 (records grouped)", calls)
		}
	}
	if got := len(committer.committed()); got != records {
		t.Fatalf("committed = %d, want %d", got, records)
	}
}

// Records committed for a partition must preserve WAL (produce) order even
// though buckets commit in parallel.
func TestProduceDispatcherPreservesPerPartitionOrder(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 3)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	const perPartition = 8
	seqOf := map[int]int{}
	for i := range perPartition * 3 {
		p := i % 3
		payload := fmt.Appendf(nil, `{"p":%d,"n":%d}`, p, seqOf[p])
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, payload); err != nil {
			t.Fatalf("AcceptProduce error = %v", err)
		}
		seqOf[p]++
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})
	if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}

	last := map[int]int{0: -1, 1: -1, 2: -1}
	for _, r := range committer.committed() {
		var pp struct {
			P int `json:"p"`
			N int `json:"n"`
		}
		if err := json.Unmarshal(r.Payload, &pp); err != nil {
			t.Fatalf("unmarshal %s: %v", r.Payload, err)
		}
		if pp.N != last[pp.P]+1 {
			t.Fatalf("partition %d out of order: got n=%d after %d", pp.P, pp.N, last[pp.P])
		}
		last[pp.P] = pp.N
	}
	for p, n := range last {
		if n != perPartition-1 {
			t.Fatalf("partition %d committed up to n=%d, want %d", p, n, perPartition-1)
		}
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
	if gotRecord.Topic != "orders" ||
		gotRecord.Key != "customer-1" ||
		gotRecord.TargetPartition != 0 ||
		string(gotRecord.Payload) != `{"id":1}` ||
		gotRecord.CreatedAtUnixMs != accepted.CreatedAtUnixMs {
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

func TestProduceDispatcherDispatchTargetInvalidatesOnAssignmentChange(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	dispatcher := NewProduceDispatcher(newDispatchIngressManager(t), store, "node-self", &fakeProduceCommitter{}, nil, nil, ProduceDispatcherConfig{})
	record := ingress.ProduceRecord{Topic: "orders", TargetPartition: 0}

	target, err := dispatcher.dispatchTarget(record)
	if err != nil {
		t.Fatalf("dispatchTarget() before reassignment error = %v", err)
	}
	if target.local || target.addr != "remote.example:7942" {
		t.Fatalf("dispatchTarget() before reassignment = %+v, want remote target", target)
	}

	if err := store.AssignPartition(context.Background(), "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	target, err = dispatcher.dispatchTarget(record)
	if err != nil {
		t.Fatalf("dispatchTarget() after reassignment error = %v", err)
	}
	if !target.local {
		t.Fatalf("dispatchTarget() after reassignment = %+v, want local target", target)
	}
}

func TestProduceDispatcherDispatchTargetInvalidatesOnMemberDeath(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	dispatcher := NewProduceDispatcher(newDispatchIngressManager(t), store, "node-self", &fakeProduceCommitter{}, nil, nil, ProduceDispatcherConfig{})
	record := ingress.ProduceRecord{Topic: "orders", TargetPartition: 0}

	if target, err := dispatcher.dispatchTarget(record); err != nil || target.addr != "remote.example:7942" {
		t.Fatalf("dispatchTarget() before death = %+v, %v; want remote target", target, err)
	}
	if err := store.MarkMemberDead(context.Background(), "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}

	_, err := dispatcher.dispatchTarget(record)
	if err == nil || !strings.Contains(err.Error(), `owner "node-remote" is unavailable`) {
		t.Fatalf("dispatchTarget() after death error = %v, want owner unavailable", err)
	}
}

func TestProduceDispatcherDispatchTargetKeepsCacheOnHeartbeatOnlyMemberUpdate(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	dispatcher := NewProduceDispatcher(newDispatchIngressManager(t), store, "node-self", &fakeProduceCommitter{}, nil, nil, ProduceDispatcherConfig{})
	record := ingress.ProduceRecord{Topic: "orders", TargetPartition: 0}

	if _, err := dispatcher.dispatchTarget(record); err != nil {
		t.Fatalf("dispatchTarget() initial error = %v", err)
	}
	dispatcher.targetMu.RLock()
	before := dispatcher.targetCache["orders"]
	dispatcher.targetMu.RUnlock()

	if err := store.RegisterMember(context.Background(), metastore.Member{
		ID:            "node-remote",
		Addr:          "remote.example:7942",
		Status:        metastore.MemberAlive,
		LastHeartbeat: 1234,
	}); err != nil {
		t.Fatalf("RegisterMember() heartbeat-only update error = %v", err)
	}

	target, err := dispatcher.dispatchTarget(record)
	if err != nil {
		t.Fatalf("dispatchTarget() after heartbeat-only update error = %v", err)
	}
	if target.addr != "remote.example:7942" {
		t.Fatalf("dispatchTarget() after heartbeat-only update = %+v, want same remote addr", target)
	}
	dispatcher.targetMu.RLock()
	after := dispatcher.targetCache["orders"]
	dispatcher.targetMu.RUnlock()
	if after.assignmentVersion != before.assignmentVersion || after.routingMembersVersion != before.routingMembersVersion {
		t.Fatalf("cache versions after heartbeat-only update = (%d,%d), want (%d,%d)",
			after.assignmentVersion, after.routingMembersVersion, before.assignmentVersion, before.routingMembersVersion)
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
	if committer.records[0].WAL != accepted.WAL || string(committer.records[0].Payload) != `{"id":1}` {
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
	if len(gotReq.Records) != 1 ||
		gotReq.Records[0].Topic != "orders" ||
		gotReq.Records[0].Key != "customer-1" ||
		gotReq.Records[0].TargetPartition != 0 ||
		string(gotReq.Records[0].Payload) != `{"id":1}` ||
		gotReq.Records[0].CreatedAtUnixMs != accepted.CreatedAtUnixMs {
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

func TestProduceDispatcherDispatchesSingleWALAcrossPartitions(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 4)
	manager := newDispatchIngressManager(t)
	acceptedByWAL := make(map[wal.RecordID]ingress.AcceptedProduce)
	for i := range 16 {
		partition := i % 4
		accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", partition, []byte(`{"id":1}`))
		if err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
		if accepted.WAL.Seq != uint64(i) {
			t.Fatalf("accepted WAL seq = %d, want %d", accepted.WAL.Seq, i)
		}
		acceptedByWAL[accepted.WAL] = accepted
	}

	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})
	processed, err := dispatcher.DispatchAvailable(context.Background())
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != len(acceptedByWAL) {
		t.Fatalf("processed = %d, want %d", processed, len(acceptedByWAL))
	}
	if len(committer.records) != len(acceptedByWAL) {
		t.Fatalf("committed records = %d, want %d", len(committer.records), len(acceptedByWAL))
	}
	for _, record := range committer.records {
		accepted, ok := acceptedByWAL[record.WAL]
		if !ok {
			t.Fatalf("committed unknown record %+v", record)
		}
		if record.TargetPartition != accepted.TargetPartition {
			t.Fatalf("committed partition = %d, want %d", record.TargetPartition, accepted.TargetPartition)
		}
	}
	checkpoint, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if checkpoint != uint64(len(acceptedByWAL)) {
		t.Fatalf("LoadProduceCheckpoint() = %d, want %d", checkpoint, len(acceptedByWAL))
	}
}

// A checkpoint-store failure after a successful commit window must not
// forget the window's commits: the next pass replays the same seqs from the
// old checkpoint and has to skip them, not deliver duplicates.
func TestProduceDispatcherDoesNotRecommitWindowAfterCheckpointStoreFailure(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()
	manager := newDispatchIngressManagerAt(t, dir)
	ctx := context.Background()
	for i := range 2 {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	// Fail the checkpoint write: its temp file cannot be created in a
	// read-only WAL directory.
	produceDir := filepath.Join(dir, "ingress", "produce")
	if err := os.Chmod(produceDir, 0o555); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	restore := func() {
		if err := os.Chmod(produceDir, 0o755); err != nil {
			t.Fatalf("Chmod() error = %v", err)
		}
	}
	t.Cleanup(restore)

	if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
		t.Fatal("want checkpoint store error on read-only WAL dir")
	}
	if got := len(committer.committed()); got != 2 {
		t.Fatalf("committed = %d records on failed-checkpoint pass, want 2", got)
	}

	restore()
	if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
		t.Fatalf("second DispatchAvailable() error = %v", err)
	}
	if got := len(committer.committed()); got != 2 {
		t.Fatalf("committed = %d records total, want 2 (window re-committed after checkpoint failure)", got)
	}
	if nextSeq, _ := manager.LoadProduceCheckpoint(); nextSeq != 2 {
		t.Fatalf("checkpoint = %d, want 2", nextSeq)
	}
}

func seedProduceDispatchTopic(t *testing.T, store *metastore.Store, ownerID string) {
	t.Helper()
	seedProduceDispatchTopicPartitions(t, store, ownerID, 1)
}

// seedNamedProduceDispatchTopic creates an extra topic with all partitions
// assigned to ownerID. Members must already be registered (e.g. via
// seedProduceDispatchTopicPartitions).
func seedNamedProduceDispatchTopic(t *testing.T, store *metastore.Store, name, ownerID string, partitions int) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: partitions}); err != nil {
		t.Fatalf("CreateTopic(%s) error = %v", name, err)
	}
	for partition := range partitions {
		if err := store.AssignPartition(ctx, name, partition, ownerID); err != nil {
			t.Fatalf("AssignPartition(%s/%d) error = %v", name, partition, err)
		}
	}
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
		MaxRecord:    1024,
	})
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

// newDispatchIngressManagerLargeSegments is for tests that write thousands
// of records: large segments avoid creating one file per couple of records.
func newDispatchIngressManagerLargeSegments(t *testing.T) *ingress.Manager {
	t.Helper()
	manager, err := ingress.OpenManager(t.TempDir(), wal.Options{
		SegmentBytes: 1 << 20,
		SyncInterval: time.Hour,
		MaxRecord:    1024,
	})
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

// clampWindow bounds the adaptive window to [base, BatchSize cap], with the
// cap winning when it is below the base (so a tiny configured BatchSize still
// hard-caps the window — the StopsAtBatchSize contract).
func TestProduceDispatcherClampWindow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batchSize int
		target    int
		want      int
	}{
		{"below base clamps up to base", 1 << 16, 100, produceDispatchBaseWindow},
		{"within range passes through", 1 << 16, 6400, 6400},
		{"above cap clamps to cap", 1 << 16, 200000, 1 << 16},
		{"tiny cap wins over base", 1, 64, 1},
		{"zero cap treated as one", 0, 64, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &ProduceDispatcher{batchSize: tc.batchSize}
			if got := d.clampWindow(tc.target); got != tc.want {
				t.Fatalf("clampWindow(%d) with batchSize=%d = %d, want %d",
					tc.target, tc.batchSize, got, tc.want)
			}
		})
	}
}

// The adaptive window grows to ~targetPerPartition * (distinct partitions
// seen) after a pass, so per-partition commit batches stay fat under fan-out.
func TestProduceDispatcherWindowGrowsWithFanout(t *testing.T) {
	const partitions = 100
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", partitions)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	for p := range partitions {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	// First pass starts at the base window (>= 100 records), so it sees the
	// full fan-out and sizes the next window to it.
	processed, err := dispatcher.DispatchAvailable(ctx)
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if processed != partitions {
		t.Fatalf("processed = %d, want %d", processed, partitions)
	}
	want := dispatcher.clampWindow(produceDispatchTargetPerPartition * partitions)
	if want <= produceDispatchBaseWindow {
		t.Fatalf("test setup: expected window above base, got want=%d base=%d", want, produceDispatchBaseWindow)
	}
	if got := dispatcher.state.windowLimit; got != want {
		t.Fatalf("windowLimit after fan-out = %d, want %d (target %d * %d partitions, clamped)",
			got, want, produceDispatchTargetPerPartition, partitions)
	}
}

// A remote commit must carry an explicit, generous deadline: the
// dispatcher's own context has none, so without one the peer transport's
// short (~5s) default reply timeout applies and a slow-but-successful
// remote commit would be retried as duplicates (and eventually rerouted).
func TestProduceDispatcherRemoteCommitCarriesGenerousDeadline(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	manager := newDispatchIngressManager(t)
	if _, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	var deadline time.Time
	var hasDeadline bool
	dispatcher := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, fakePeerClient{
		commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			deadline, hasDeadline = ctx.Deadline()
			return nodewire.Response{Status: http.StatusOK}, nil
		},
	}, nil, ProduceDispatcherConfig{})

	start := time.Now()
	if _, err := dispatcher.DispatchAvailable(context.Background()); err != nil {
		t.Fatalf("DispatchAvailable() error = %v", err)
	}
	if !hasDeadline {
		t.Fatal("remote commit has no deadline; the transport's short fallback timeout would apply")
	}
	remaining := deadline.Sub(start)
	if remaining < produceCommitRPCTimeout-5*time.Second {
		t.Fatalf("remote commit deadline is %s away, want ~%s (far above the transport's 5s fallback)", remaining, produceCommitRPCTimeout)
	}
	if remaining > produceCommitRPCTimeout+5*time.Second {
		t.Fatalf("remote commit deadline is %s away, want it bounded near %s", remaining, produceCommitRPCTimeout)
	}
}
