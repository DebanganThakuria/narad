package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

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
