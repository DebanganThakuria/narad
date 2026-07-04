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

// fakeProduceCommitter is thread-safe: the dispatcher now commits buckets
// concurrently, so multiple goroutines may call into it at once. It also
// records the size of each batch call so tests can assert that interleaved
// records were grouped into few large batches rather than many size-1 ones.
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

// One dead partition owner must not freeze visibility of every newer record
// this node accepted: all destinations share the single ingress WAL, so
// before the bounded skip-ahead the drain window pinned at the stuck seq and
// records beyond the window boundary were never examined. The stuck topic
// has a single partition owned by a dead remote — no live-owner sibling
// exists, so dispatch-time rerouting cannot rescue it and the record truly
// pins the checkpoint. The healthy topic is local, with records far beyond
// produceDispatchBaseWindow: they must all commit while the checkpoint stays
// pinned at the stuck seq (so the stuck record survives crashes/compaction),
// and after the owner recovers everything drains exactly once.
func TestProduceDispatcherSkipsPastStuckOwnerBeyondBaseWindow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	seedNamedProduceDispatchTopic(t, store, "stuck", "node-remote", 1)

	manager := newDispatchIngressManagerLargeSegments(t)
	// seq0 targets the stuck topic, whose only owner is about to die.
	if _, err := manager.AcceptProduce(ctx, "stuck", "k", 0, []byte(`{"stuck":true}`)); err != nil {
		t.Fatalf("AcceptProduce(stuck) error = %v", err)
	}
	const healthy = produceDispatchBaseWindow + 512
	for i := range healthy {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(orders #%d) error = %v", i, err)
		}
	}
	if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}

	committer := &fakeProduceCommitter{}
	var peerMu sync.Mutex
	var peerRecords []nodewire.CommitProduceRequest
	peer := fakePeerClient{
		commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			peerMu.Lock()
			peerRecords = append(peerRecords, req.Records...)
			peerMu.Unlock()
			return nodewire.Response{Status: http.StatusOK}, nil
		},
	}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, peer, nil, ProduceDispatcherConfig{})

	// While the stuck topic's owner is dead every pass reports the stuck
	// owner and the checkpoint stays pinned at seq0, but the healthy topic's
	// records keep committing — including the ones beyond the base window
	// boundary.
	for pass := range 10 {
		if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
			t.Fatalf("pass %d: error = nil, want owner-unavailable while the stuck owner is dead", pass)
		}
		if n, err := manager.LoadProduceCheckpoint(); err != nil || n != 0 {
			t.Fatalf("pass %d: checkpoint = %d (err %v), want pinned at 0", pass, n, err)
		}
		if len(committer.committed()) == healthy {
			break
		}
	}
	got := committer.committed()
	if len(got) != healthy {
		t.Fatalf("healthy commits while owner dead = %d, want %d (records beyond the old window are frozen)", len(got), healthy)
	}
	for _, r := range got {
		if r.Topic != "orders" {
			t.Fatalf("committed a %q record locally while its owner is dead", r.Topic)
		}
	}

	// Owner recovers: everything drains, the stuck record commits exactly
	// once on its ORIGINAL partition, and none of the healthy records
	// re-commit.
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(revive) error = %v", err)
	}
	for pass := range 10 {
		if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
			t.Fatalf("recovery pass %d error = %v", pass, err)
		}
		if n, _ := manager.LoadProduceCheckpoint(); n == uint64(healthy)+1 {
			break
		}
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != uint64(healthy)+1 {
		t.Fatalf("checkpoint after recovery = %d, want %d", n, healthy+1)
	}
	peerMu.Lock()
	defer peerMu.Unlock()
	if len(peerRecords) != 1 || peerRecords[0].Topic != "stuck" || peerRecords[0].TargetPartition != 0 {
		t.Fatalf("remote commits after recovery = %d records %+v, want exactly the one stuck record", len(peerRecords), peerRecords)
	}
	if n := len(committer.committed()); n != healthy {
		t.Fatalf("local commits after recovery = %d, want %d (no duplicates)", n, healthy)
	}
}

// The skip-ahead is bounded: while a low seq is stuck, records more than
// windowLimit*produceDispatchLookaheadWindows seqs above the checkpoint stay
// frozen (that bound is what keeps committedAhead and per-pass scan work
// finite). The stuck record belongs to a single-partition topic whose only
// owner keeps failing commits — no live-owner sibling exists, so rerouting
// cannot rescue it and the horizon genuinely binds. Once the stuck topic
// recovers, everything — including the beyond-horizon records — drains
// exactly once.
func TestProduceDispatcherLookaheadHorizonBoundsSkipAhead(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	seedNamedProduceDispatchTopic(t, store, "stuck", "node-self", 1)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	// seq0 -> topic "stuck" (commits fail); seqs 1..total -> topic "orders"
	// (healthy), most beyond the horizon of a BatchSize=2 window.
	if _, err := manager.AcceptProduce(ctx, "stuck", "k", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce(stuck) error = %v", err)
	}
	const window = 2
	horizon := window * produceDispatchLookaheadWindows
	total := horizon + 8
	for i := range total {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce(orders #%d) error = %v", i, err)
		}
	}
	committer := &perPartitionCommitter{failTopics: map[string]error{"stuck": errors.New("stuck owner down")}}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{BatchSize: window})

	// Drain far more passes than the horizon needs: healthy commits creep up
	// to the horizon (seqs 1..horizon-1) and stop there.
	for range horizon + 10 {
		if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
			t.Fatal("want error while the stuck topic is down")
		}
	}
	if got := committer.committedTopics()["orders"]; got != horizon-1 {
		t.Fatalf("healthy commits while stuck = %d, want %d (skip-ahead bounded by the lookahead horizon)", got, horizon-1)
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != 0 {
		t.Fatalf("checkpoint = %d, want pinned at 0", n)
	}

	// The stuck topic recovers: the full backlog drains with no duplicates.
	committer.mu.Lock()
	committer.failTopics = nil
	committer.mu.Unlock()
	for range 20 {
		if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
			t.Fatalf("recovery pass error = %v", err)
		}
		if n, _ := manager.LoadProduceCheckpoint(); n == uint64(total)+1 {
			break
		}
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != uint64(total)+1 {
		t.Fatalf("checkpoint after recovery = %d, want %d", n, total+1)
	}
	counts := committer.committedTopics()
	if counts["stuck"] != 1 || counts["orders"] != total {
		t.Fatalf("committed topics after recovery = %v, want exactly stuck=1, orders=%d (no duplicates)", counts, total)
	}
}

// A record whose target partition owner is dead per membership (for a topic
// that still exists) must not pin the checkpoint: at dispatch time it is
// rerouted to a live-owner partition of the same topic — matching the
// accept-time dead-owner skip — the checkpoint advances, each record commits
// exactly once, and the rerouted records keep their WAL-seq order.
func TestProduceDispatcherReroutesDeadOwnerRecordsAtDispatch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	manager := newDispatchIngressManager(t)
	// Interleave doomed p0 records (ordered payloads) with healthy p1 ones.
	n := 0
	for _, p := range []int{0, 1, 0, 0, 1, 0} {
		payload := []byte(`{"healthy":true}`)
		if p == 0 {
			payload = fmt.Appendf(nil, `{"n":%d}`, n)
			n++
		}
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, payload); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}

	// peer == nil: a commit wrongly routed to the dead remote owner would
	// fail loudly instead of silently succeeding.
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	processed, err := dispatcher.DispatchAvailable(ctx)
	if err != nil {
		t.Fatalf("DispatchAvailable() error = %v, want nil (dead-owner records rerouted)", err)
	}
	if processed != 6 {
		t.Fatalf("processed = %d, want 6", processed)
	}
	if nextSeq, _ := manager.LoadProduceCheckpoint(); nextSeq != 6 {
		t.Fatalf("checkpoint = %d, want 6 (no pinning on the dead owner)", nextSeq)
	}
	got := committer.committed()
	if len(got) != 6 {
		t.Fatalf("committed = %d, want 6", len(got))
	}
	lastN := -1
	for _, r := range got {
		if r.TargetPartition != 1 {
			t.Fatalf("committed record on partition %d, want everything on live-owner partition 1", r.TargetPartition)
		}
		var pp struct {
			N *int `json:"n"`
		}
		if err := json.Unmarshal(r.Payload, &pp); err != nil {
			t.Fatalf("unmarshal %s: %v", r.Payload, err)
		}
		if pp.N == nil {
			continue
		}
		if *pp.N != lastN+1 {
			t.Fatalf("rerouted records out of order: got n=%d after %d", *pp.N, lastN)
		}
		lastN = *pp.N
	}
	if lastN != 3 {
		t.Fatalf("rerouted records committed up to n=%d, want 3", lastN)
	}

	// Exactly once: a second pass finds nothing to commit.
	processed, err = dispatcher.DispatchAvailable(ctx)
	if err != nil || processed != 0 {
		t.Fatalf("second DispatchAvailable() = processed %d err %v, want 0, nil", processed, err)
	}
	if len(committer.committed()) != 6 {
		t.Fatalf("committed after second pass = %d, want 6 (no duplicates)", len(committer.committed()))
	}
}

// A commit failure while membership still says the owner is alive is treated
// as transient at first: the records retry on their ORIGINAL partition and
// must not scatter. Only after the destination has stayed stuck for
// produceDispatchRerouteAfterPasses consecutive passes are its records
// rerouted to a live-owner partition — and once the owner recovers, new
// records flow to the original partition again (rerouting is per-pass, not
// sticky).
func TestProduceDispatcherReroutesAfterConsecutiveCommitFailurePasses(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 2)
	manager := newDispatchIngressManager(t)
	ctx := context.Background()
	// seq0, seq1 -> p0 (about to fail commits, ordered payloads); seq2 -> p1.
	for i, p := range []int{0, 0, 1} {
		payload := fmt.Appendf(nil, `{"n":%d}`, i)
		if p == 1 {
			payload = []byte(`{"healthy":true}`)
		}
		if _, err := manager.AcceptProduce(ctx, "orders", "k", p, payload); err != nil {
			t.Fatalf("AcceptProduce(p%d) error = %v", p, err)
		}
	}
	committer := &perPartitionCommitter{failPartitions: map[int]error{0: errors.New("p0 owner unreachable")}}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{})

	// For the first produceDispatchRerouteAfterPasses passes the destination
	// gets its grace: commits are attempted against p0 itself, nothing is
	// rerouted, and the checkpoint stays pinned.
	for pass := 1; pass <= produceDispatchRerouteAfterPasses; pass++ {
		if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
			t.Fatalf("pass %d: error = nil, want commit failure", pass)
		}
		counts := committer.committedPartitions()
		if counts[0] != 0 || counts[1] != 1 {
			t.Fatalf("pass %d: committed partitions = %v, want p0=0, p1=1 (no reroute during the grace)", pass, counts)
		}
		if got := committer.attemptCount(0); got != pass {
			t.Fatalf("pass %d: p0 commit attempts = %d, want %d (retried on the original partition)", pass, got, pass)
		}
		if n, _ := manager.LoadProduceCheckpoint(); n != 0 {
			t.Fatalf("pass %d: checkpoint = %d, want pinned at 0", pass, n)
		}
	}

	// Next pass: the destination exceeded its grace, so its records are
	// rerouted to p1 and the checkpoint advances past them.
	if _, err := dispatcher.DispatchAvailable(ctx); err == nil {
		t.Fatal("reroute pass: error = nil, want the still-failing p0 probe error")
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != 3 {
		t.Fatalf("checkpoint after reroute = %d, want 3", n)
	}
	counts := committer.committedPartitions()
	if counts[0] != 0 || counts[1] != 3 {
		t.Fatalf("committed partitions after reroute = %v, want p0=0, p1=3 (both stuck records rerouted)", counts)
	}
	lastN := -1
	for _, r := range committer.records {
		var pp struct {
			N *int `json:"n"`
		}
		if err := json.Unmarshal(r.Payload, &pp); err != nil {
			t.Fatalf("unmarshal %s: %v", r.Payload, err)
		}
		if pp.N == nil {
			continue
		}
		if *pp.N != lastN+1 {
			t.Fatalf("rerouted records out of order: got n=%d after %d", *pp.N, lastN)
		}
		lastN = *pp.N
	}
	if lastN != 1 {
		t.Fatalf("rerouted records committed up to n=%d, want 1", lastN)
	}

	// p0 recovers: a NEW record flows to the original partition again.
	committer.mu.Lock()
	committer.failPartitions = nil
	committer.mu.Unlock()
	if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"recovered":true}`)); err != nil {
		t.Fatalf("AcceptProduce(recovered) error = %v", err)
	}
	if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
		t.Fatalf("recovery pass error = %v", err)
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != 4 {
		t.Fatalf("checkpoint after recovery = %d, want 4", n)
	}
	counts = committer.committedPartitions()
	if counts[0] != 1 || counts[1] != 3 {
		t.Fatalf("committed partitions after recovery = %v, want p0=1, p1=3 (new record back on its original partition)", counts)
	}
}

// Dispatch-time rerouting is a per-pass decision keyed off current
// membership: once a dead owner returns, new records for its partition go to
// the original partition again instead of sticking to the reroute target.
func TestProduceDispatcherRoutesToOriginalPartitionAfterOwnerRecovers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	manager := newDispatchIngressManager(t)
	if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"n":0}`)); err != nil {
		t.Fatalf("AcceptProduce(first) error = %v", err)
	}
	if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}

	committer := &fakeProduceCommitter{}
	var peerMu sync.Mutex
	var peerRecords []nodewire.CommitProduceRequest
	peer := fakePeerClient{
		commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
			peerMu.Lock()
			peerRecords = append(peerRecords, req.Records...)
			peerMu.Unlock()
			return nodewire.Response{Status: http.StatusOK}, nil
		},
	}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, peer, nil, ProduceDispatcherConfig{})

	// Dead owner: the record is rerouted to the local live-owner partition.
	if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable() while owner dead error = %v", err)
	}
	if got := committer.committed(); len(got) != 1 || got[0].TargetPartition != 1 {
		t.Fatalf("local commits while owner dead = %+v, want the record rerouted to partition 1", got)
	}

	// Owner returns: a NEW record for p0 goes to the recovered remote owner.
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(revive) error = %v", err)
	}
	if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("AcceptProduce(second) error = %v", err)
	}
	if _, err := dispatcher.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable() after recovery error = %v", err)
	}
	peerMu.Lock()
	defer peerMu.Unlock()
	if len(peerRecords) != 1 || peerRecords[0].TargetPartition != 0 || string(peerRecords[0].Payload) != `{"n":1}` {
		t.Fatalf("remote commits after recovery = %+v, want exactly the new record on partition 0", peerRecords)
	}
	if got := committer.committed(); len(got) != 1 {
		t.Fatalf("local commits after recovery = %d, want 1 (nothing new rerouted)", len(got))
	}
	if n, _ := manager.LoadProduceCheckpoint(); n != 2 {
		t.Fatalf("checkpoint = %d, want 2", n)
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
		SyncBytes:    1,
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
		SyncBytes:    1,
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
