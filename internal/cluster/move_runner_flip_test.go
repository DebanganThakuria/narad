package cluster

// A CompleteMove that returns an error did not necessarily fail: a
// deposed leader answers ErrLeadershipLost for an entry the next leader
// commits, and a forwarded flip's reply can be lost after the leader
// applied it. These tests pin how the destination resolves such a flip
// with the leader before it undoes anything, and how it keeps the source
// from staying frozen or the worker from wedging while it does.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// ---- end to end: real store, real engines, real runner and sweep ----

// raftFlipStore is the destination's view of a real single-node store:
// it leads, and when ambiguous its flip commits through Raft and then
// reports what a deposed leader reports (store.apply classifies
// raft.ErrLeadershipLost as errs.ErrUnavailable).
type raftFlipStore struct {
	*metastore.Store
	ambiguous bool
}

func (s raftFlipStore) IsLeader() bool { return true }

func (s raftFlipStore) CompleteMove(ctx context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	if err := s.Store.CompleteMove(ctx, topicName, partition, expectedOwner, targetID); err != nil {
		return err
	}
	if s.ambiguous {
		return fmt.Errorf("%w: leadership lost while committing log", errs.ErrUnavailable)
	}
	return nil
}

// followerView is another node's view of the same store: a follower,
// which asks the leader over peer RPC.
type followerView struct{ *metastore.Store }

func (s followerView) IsLeader() bool { return false }

// enginePeer routes each peer RPC to what the RPC server would call on
// the node at addr.
type enginePeer struct {
	store   *metastore.Store
	engines map[string]*messaging.Engine // addr -> engine
}

func (p enginePeer) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := p.engines[addr].PartitionTransferInfo(ctx, topicName, partition)
	if err != nil {
		return info, err
	}
	// Round-trip it as PeerClient decodes it off the wire.
	body, err := json.Marshal(info)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	var decoded messaging.PartitionTransferInfo
	err = json.Unmarshal(body, &decoded)
	return decoded, err
}

func (p enginePeer) FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, baseOffset, at, length int64) ([]byte, error) {
	return p.engines[addr].ReadPartitionSegment(ctx, topicName, partition, baseOffset, at, length)
}

func (p enginePeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if token != "" {
		return p.engines[addr].ConfirmHandoff(ctx, topicName, partition, ttl, token)
	}
	return p.engines[addr].PrepareHandoff(ctx, topicName, partition, ttl)
}

func (p enginePeer) CompleteMove(ctx context.Context, _, topicName string, partition int, expectedOwner, targetID string) error {
	return p.store.CompleteMove(ctx, topicName, partition, expectedOwner, targetID)
}

func (p enginePeer) AbortMove(ctx context.Context, _, topicName string, partition int, expectedTarget string) error {
	return p.store.AbortMove(ctx, topicName, partition, expectedTarget)
}

// GetAssignment answers as the leader does: behind a barrier.
func (p enginePeer) GetAssignment(_ context.Context, _, topicName string, partition int) (metastore.Assignment, error) {
	if err := p.store.Barrier(); err != nil {
		return metastore.Assignment{}, err
	}
	return p.store.GetAssignment(topicName, partition)
}

func (p enginePeer) GetTopic(ctx context.Context, _, topicName string) (nodewire.Response, error) {
	t, err := p.store.GetTopic(ctx, topicName)
	if errors.Is(err, errs.ErrNotFound) {
		return nodewire.Response{Status: http.StatusNotFound}, nil
	}
	if err != nil {
		return nodewire.Response{}, err
	}
	body, err := json.Marshal(t)
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}, err
}

// newTestEngine is one node's broker over the shared store, with its own
// data directory.
func newTestEngine(t *testing.T, store metastore.Metastore, selfID string) (*messaging.Engine, *runtime.Logs, string) {
	t.Helper()
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	engine := messaging.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, discardLogger(), selfID)
	return engine, logs, dataDir
}

// produceRecords commits n records to orders/0 on engine.
func produceRecords(t *testing.T, store *metastore.Store, engine *messaging.Engine, n int) {
	t.Helper()
	ctx := context.Background()
	rec, err := store.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	var records []ingress.ProduceRecord
	for i := range n {
		records = append(records, ingress.ProduceRecord{
			Topic: "orders", TopicID: rec.ID, Key: fmt.Sprintf("k%d", i), TargetPartition: 0,
			Payload: fmt.Appendf(nil, `{"seq":%d}`, i),
		})
	}
	if _, err := engine.CommitAcceptedProduceBatch(ctx, records); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// seedMoveCluster registers the leader and two members, creates orders
// with one partition owned by narad-src, and returns the store.
func seedMoveCluster(t *testing.T) *metastore.Store {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	for _, m := range []metastore.Member{
		{ID: "node-self", Addr: "leader-addr", Status: metastore.MemberAlive},
		{ID: "narad-src", Addr: "src-addr", Status: metastore.MemberAlive},
		{ID: "narad-dst", Addr: "dst-addr", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember: %v", err)
		}
	}
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: "orders", Partitions: 1, RetentionMs: 7_200_000,
		VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "narad-src"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	return store
}

// runUntilMoved drives r's reconcile passes until owner is the
// partition's owner and r has no worker left, or the deadline passes.
func runUntilMoved(t *testing.T, ctx context.Context, r *MoveRunner, owner func() string, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		r.Reconcile(ctx)
		r.mu.Lock()
		idle := len(r.workers) == 0
		r.mu.Unlock()
		if idle && owner() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMoveKeepsThePartitionWhenTheFlipReplyIsLost moves orders/0 (10
// committed records) from narad-src to narad-dst with real code except
// the network: the destination's flip commits through Raft and then
// returns the error a deposed leader returns. The new owner must keep
// its installed copy, and the old owner's stale-copy sweep must not
// leave the partition with no copy.
func TestMoveKeepsThePartitionWhenTheFlipReplyIsLost(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprintf("ambiguous=%v", ambiguous), func(t *testing.T) {
			ctx := context.Background()
			store := seedMoveCluster(t)
			srcEngine, _, srcData := newTestEngine(t, store, "narad-src")
			dstEngine, dstLogs, dstData := newTestEngine(t, store, "narad-dst")
			produceRecords(t, store, srcEngine, 10)
			if err := store.SetAssignmentTarget(ctx, "orders", 0, "narad-dst"); err != nil {
				t.Fatal(err)
			}
			peer := enginePeer{store: store, engines: map[string]*messaging.Engine{"src-addr": srcEngine, "dst-addr": dstEngine}}
			dst := NewMoveRunner(raftFlipStore{Store: store, ambiguous: ambiguous}, "narad-dst", dstData, peer, dstEngine, nil, discardLogger(),
				MoveConfig{RetryBackoff: 20 * time.Millisecond, FreezeTTL: 2 * time.Second})
			runCtx, cancel := context.WithCancel(ctx)
			owner := func() string { a, _ := store.GetAssignment("orders", 0); return a.OwnerID }
			runUntilMoved(t, runCtx, dst, owner, "narad-dst", 10*time.Second)
			cancel()
			dst.wg.Wait()
			if got := owner(); got != "narad-dst" {
				t.Fatalf("the move did not flip: owner %q", got)
			}

			// The old owner's stale-copy sweep.
			src := NewMoveRunner(followerView{store}, "narad-src", srcData, peer, srcEngine, nil, discardLogger(), MoveConfig{})
			src.sweepStaleCopies(ctx)

			dstDir := topicPartitionDirT(t, dstData, "orders", 0)
			_, statErr := os.Stat(dstDir)
			_, marked, _ := messaging.ReadMoveMarker(dstDir)
			next := int64(-1)
			if l, err := dstLogs.Get("orders", 0); err == nil {
				next = l.NextOffset()
			}
			_, srcErr := os.Stat(topicPartitionDirT(t, srcData, "orders", 0))
			t.Logf("dst dir err=%v marker=%v next=%d; src copy err after the sweep=%v", statErr, marked, next, srcErr)
			if statErr != nil || !marked || next != 10 {
				t.Fatalf("DATA LOSS: narad-dst owns orders/0 but its copy is gone (dir err %v, marker %v, next offset %d; 10 records were committed)", statErr, marked, next)
			}
		})
	}
}

// ---- unit forms on the fake store ----

// commitThenFailStore commits the flip (owner := target) and reports an
// error, as a forwarded CompleteMove does when the leader applied it and
// the reply timed out.
type commitThenFailStore struct {
	*fakeMoveStore
	flips int
}

func (s *commitThenFailStore) CompleteMove(_ context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flips++
	s.completeArgs = []string{topicName, expectedOwner, targetID}
	if s.assignment.OwnerID != expectedOwner || s.assignment.TargetID != targetID {
		return fmt.Errorf("%w: complete-move owner is %q, expected %q", errs.ErrInvalidArgument, s.assignment.OwnerID, expectedOwner)
	}
	s.assignment.OwnerID = targetID
	s.assignment.TargetID = ""
	return errors.New("peer rpc: reply timeout")
}

func requireInstalledCopy(t *testing.T, dataDir string, wantHWM int64, payloads map[int64][]byte) {
	t.Helper()
	log, err := storage.NewLog(topicPartitionDirT(t, dataDir, "orders", 0), storage.Options{})
	if err != nil {
		t.Fatalf("open the installed copy: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("DATA LOSS: the partition's path recovers next offset %d, want %d", log.NextOffset(), wantHWM)
	}
	for off, want := range payloads {
		if _, _, got, err := log.ReadKeyed(off); err != nil || string(got) != string(want) {
			t.Fatalf("offset %d = %q (err %v), want %q", off, got, err, want)
		}
	}
}

func TestMoveRunnerKeepsInstallWhenTheFlipCommitsButItsReplyFails(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	store := &commitThenFailStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()
	if ctx.Err() != nil {
		t.Error("the worker never learned that its flip committed (it ran until the test's timeout)")
	}
	store.mu.Lock()
	flips := store.flips
	store.mu.Unlock()
	if flips != 1 {
		t.Errorf("flips proposed = %d, want 1 (the committed flip needs no retry)", flips)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// dirInstaller is a partitionInstaller that counts directory
// replacements (the install is one, any rollback another), runs an
// optional hook before each, and replaces through a real runtime.Logs
// when one is set.
type dirInstaller struct {
	logs *runtime.Logs

	mu       sync.Mutex
	replaces []time.Time
	before   func(n int)
}

func (d *dirInstaller) ReclaimMovedPartition(context.Context, string, int) error { return nil }

func (d *dirInstaller) InstallPartitionDir(topicName string, partition int, swap func() error) error {
	d.mu.Lock()
	d.replaces = append(d.replaces, time.Now())
	n := len(d.replaces)
	before := d.before
	d.mu.Unlock()
	if before != nil {
		before(n)
	}
	if d.logs != nil {
		return d.logs.ReplacePartitionDir(topicName, partition, swap)
	}
	return swap()
}

func (d *dirInstaller) replacements() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.replaces...)
}

// keepingEngine adds what the move runner asks of a destination broker
// that prepares topic directories for an incarnation.
type keepingEngine struct {
	*dirInstaller
}

func (k keepingEngine) EnsureTopicIncarnation(topicName, id string) error {
	return k.logs.EnsureTopicIncarnation(topicName, id)
}

// unknownFlipStore answers every flip with err and never commits it.
type unknownFlipStore struct {
	*fakeMoveStore
	err   error
	flips atomic.Int32
}

func (s *unknownFlipStore) CompleteMove(context.Context, string, int, string, string) error {
	s.flips.Add(1)
	return s.err
}

// Every flip times out without committing and the leader keeps reading
// "not flipped yet". The install must stay at the partition's path, and
// the flip be proposed again under the freeze the worker holds.
func TestMoveRunnerKeepsInstallOnAnUnknownFlipOutcome(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	store := &unknownFlipStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}, err: context.DeadlineExceeded}
	freeze := &fakeFreeze{}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: freeze}
	installer := &dirInstaller{}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, installer, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond, FreezeTTL: time.Minute})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	for store.flips.Load() < 3 && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	r.wg.Wait()

	if n := store.flips.Load(); n < 3 {
		t.Errorf("flip proposed %d times, want it proposed again (at least 3 in all)", n)
	}
	freeze.mu.Lock()
	minted, rearms, lapsed := freeze.minted, freeze.rearms, freeze.lapsed
	freeze.mu.Unlock()
	if minted != 1 || lapsed != 0 || rearms < 2 {
		t.Errorf("freeze minted %d, re-armed %d, refused %d: want every proposal under re-arms of the one freeze", minted, rearms, lapsed)
	}
	if got := len(installer.replacements()); got != 1 {
		t.Errorf("partition directory replaced %d times, want only the install (the copy must never leave the path)", got)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// lateCommitStore answers the first flip with an unknown outcome without
// committing it: the proposal is still on its way into the leader's log.
// It commits during the worker's commitOnRead-th leader read after that
// reply (or at commit, if the worker stops asking first). Every leader
// read takes readTakes on the worker's clock, so the store, not the wall
// clock, decides when the commit lands within the settle window.
type lateCommitStore struct {
	*fakeMoveStore
	clock        *testClock
	readTakes    time.Duration
	commitOnRead int32

	calls     atomic.Int32
	reads     atomic.Int32 // leader reads since the unknown reply
	unknownAt time.Time    // the unknown reply, on clock; set once under mu
	flip      [2]string    // the first proposal's expected owner and target
	landed    bool         // the first proposal committed; under mu
}

func (s *lateCommitStore) CompleteMove(_ context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls.Add(1) == 1 {
		s.flip = [2]string{expectedOwner, targetID}
		s.unknownAt = s.clock.Now()
		return fmt.Errorf("%w: peer rpc: reply timeout", errs.ErrUnavailable)
	}
	if s.assignment.OwnerID != expectedOwner || s.assignment.TargetID != targetID {
		return fmt.Errorf("%w: complete-move owner is %q, expected %q", errs.ErrInvalidArgument, s.assignment.OwnerID, expectedOwner)
	}
	s.assignment.OwnerID, s.assignment.TargetID = targetID, ""
	return nil
}

// Barrier is the start of the worker's leader read (it is its own
// leader): the read takes readTakes, and the late proposal commits
// during the commitOnRead-th one.
func (s *lateCommitStore) Barrier() error {
	if s.calls.Load() == 0 {
		return nil
	}
	s.clock.Advance(s.readTakes)
	if s.reads.Add(1) == s.commitOnRead {
		s.commit()
	}
	return nil
}

// commit lands the first proposal, if it has not landed yet.
func (s *lateCommitStore) commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.landed || s.calls.Load() == 0 {
		return
	}
	s.landed = true
	if s.assignment.OwnerID == s.flip[0] && s.assignment.TargetID == s.flip[1] {
		s.assignment.OwnerID, s.assignment.TargetID = s.flip[1], ""
	}
}

// oneFreezePeer lets the source be frozen once: every later fresh
// (token-less) PrepareHandoff fails, so a worker that undid its install
// can never copy and install the partition again.
type oneFreezePeer struct {
	movePeerFake
	fresh *atomic.Int32
}

func (p oneFreezePeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if token == "" && p.fresh.Add(1) > 1 {
		return messaging.PartitionTransferInfo{}, errors.New("source refuses a second freeze")
	}
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

// The flip's reply is an unknown outcome, the leader reads "not flipped
// yet", and the source's freeze has lapsed, so the install cannot be
// flipped as it is. The proposal commits during the fourth leader read,
// three quarters of the way into the settle window on the worker's
// clock. Moving the install back before that proposal could no longer
// commit would leave the new owner with nothing under the partition's
// path: the worker must wait out the settle window.
func TestMoveRunnerWaitsOutTheSettleWindowBeforeUndoingAnInstall(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	const settle = time.Second
	clock := newTestClock()
	store := &lateCommitStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}, clock: clock, readTakes: settle / 4, commitOnRead: 4}
	// Fenced re-arm 1 is the fence before the flip; re-arm 2, the
	// resolution's, finds the freeze gone. The freeze's own clock never
	// moves, so nothing else lapses it.
	freeze := &fakeFreeze{lapseOn: 2, virtual: true}
	var fresh atomic.Int32
	peer := oneFreezePeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: freeze},
		fresh:        &fresh,
	}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, nil, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	var (
		mu         sync.Mutex
		replacedAt []time.Time // on the worker's clock
	)
	installer := &dirInstaller{logs: logs, before: func(int) {
		mu.Lock()
		replacedAt = append(replacedAt, clock.Now())
		mu.Unlock()
	}}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, installer, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond, FreezeTTL: time.Minute})
	r.now = clock.Now
	r.flipSettle = settle
	ctx, cancel := context.WithCancel(context.Background())
	owner := func() string {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.assignment.OwnerID
	}
	runUntilMoved(t, ctx, r, owner, "narad-dst", 10*time.Second)
	cancel()
	r.wg.Wait()
	// The proposal commits whatever the worker did meanwhile; a worker
	// that stopped asking before it landed undid the install first.
	store.commit()

	if store.calls.Load() == 0 {
		t.Fatal("setup: the worker never proposed the flip")
	}
	if got := owner(); got != "narad-dst" {
		t.Fatalf("setup: the late flip never landed (owner %s)", got)
	}
	if got := store.reads.Load(); got < store.commitOnRead {
		t.Errorf("the worker stopped asking the leader after %d reads, before the late flip landed on read %d", got, store.commitOnRead)
	}
	mu.Lock()
	replaces := append([]time.Time(nil), replacedAt...)
	mu.Unlock()
	for i, at := range replaces[1:] {
		if at.Sub(store.unknownAt) < settle {
			t.Errorf("directory replacement %d ran %v after the unknown reply on the worker's clock, inside the %v settle window", i+2, at.Sub(store.unknownAt), settle)
		}
	}
	if len(replaces) != 1 {
		t.Errorf("partition directory replaced %d times, want only the install", len(replaces))
	}
	dir := topicPartitionDirT(t, dataDir, "orders", 0)
	if _, err := os.Stat(dir); err != nil {
		_, serr := os.Stat(r.stagingDir("orders", 0))
		t.Fatalf("DATA LOSS: narad-dst owns orders/0 but nothing is under its path (%v); staging stat: %v", err, serr)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
	if _, err := os.Stat(r.stagingDir("orders", 0)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging left behind after the move (stat err %v)", err)
	}
}

// goneTopicStore reports no assignment once gone is set: the topic was
// deleted.
type goneTopicStore struct {
	*fakeMoveStore
	gone atomic.Bool
}

func (s *goneTopicStore) GetAssignment(topicName string, partition int) (metastore.Assignment, error) {
	if s.gone.Load() {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	return s.fakeMoveStore.GetAssignment(topicName, partition)
}

// The topic is deleted as the flip is proposed: the leader confirms the
// flip cannot happen, the install goes back to staging, and the worker
// ends. A partition with no assignment has no owner, so the worker
// removes its staging copy rather than keeping it for an owner that
// will never come.
func TestMoveRunnerRemovesItsStagingWhenTheTopicIsGone(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	store := &goneTopicStore{fakeMoveStore: &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		completeErr: fmt.Errorf("%w: assignment orders/0", errs.ErrNotFound),
	}}
	store.completeHook = func() { store.gone.Store(true) }
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("the worker kept running after the leader confirmed the topic is gone")
	}
	if segs, _ := storage.ListPartitionSegments(topicPartitionDirT(t, dataDir, "orders", 0)); len(segs) != 0 {
		t.Fatalf("install left at the partition's path: %d segments", len(segs))
	}
	if _, err := os.Stat(r.stagingDir("orders", 0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the worker kept its staging copy of a deleted topic's partition (stat err %v)", err)
	}
}

// chunkHookPeer runs onChunk before every segment chunk fetch.
type chunkHookPeer struct {
	movePeerFake
	onChunk func()
}

func (p chunkHookPeer) FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, base, at, length int64) ([]byte, error) {
	p.onChunk()
	return p.movePeerFake.FetchSegmentChunk(ctx, addr, topicName, partition, base, at, length)
}

// Worker A installs and proposes the flip, the reply is unknown, and the
// node restarts with the flip pending, which cancels A. The target is
// still this node, so worker B starts a fresh copy into staging; A's
// flip then commits, A's install becomes the partition, and the
// reconcile cancels B. B's staging holds only its re-copy: it must go,
// without the "operator action required" alarm, and the partition's
// path keeps A's install.
func TestMoveRunnerDropsARecopyWhenAnEarlierWorkersInstallFlipped(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	store := &unknownFlipStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}, err: fmt.Errorf("%w: peer rpc: reply timeout", errs.ErrUnavailable)}
	fetcher := dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}
	dataDir := t.TempDir()
	cfg := MoveConfig{RetryBackoff: 5 * time.Millisecond, FreezeTTL: time.Minute}

	a := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{dirFetcher: fetcher, freeze: &fakeFreeze{}}, nil, nil, nil, cfg)
	ctxA, cancelA := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelA()
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		a.runMove(ctxA, "orders", 0, "narad-src")
	}()
	for store.flips.Load() < 1 && ctxA.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	cancelA()
	<-doneA
	dir := topicPartitionDirT(t, dataDir, "orders", 0)
	if m, ok, err := messaging.ReadMoveMarker(dir); err != nil || !ok || m.Source != "narad-src" {
		t.Fatalf("setup: worker A's install is not at the partition's path (marker %+v, found %v, err %v)", m, ok, err)
	}

	ctxB, cancelB := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelB()
	var commitA sync.Once
	peerB := chunkHookPeer{movePeerFake: movePeerFake{dirFetcher: fetcher, freeze: &fakeFreeze{}}, onChunk: func() {
		commitA.Do(func() {
			store.mu.Lock()
			store.assignment.OwnerID, store.assignment.TargetID = "narad-dst", ""
			store.mu.Unlock()
			cancelB()
		})
	}}
	b := NewMoveRunner(store, "narad-dst", dataDir, peerB, nil, nil, nil, cfg)
	b.runMove(ctxB, "orders", 0, "narad-src")
	if errors.Is(ctxB.Err(), context.DeadlineExceeded) {
		t.Fatal("setup: worker B never fetched a chunk")
	}

	if _, err := os.Stat(b.stagingDir("orders", 0)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("worker B kept its re-copy in staging although the partition's path holds the install that flipped (stat err %v)", err)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// What finish does with the staging copy of a worker that ended without
// a flip on a node that owns the partition by then: only a path holding
// an install from the move's source, with no install of this worker
// moved back, makes staging redundant.
func TestMoveWorkerSetsAsideAnOwnedStagingCopyOnlyWhenItMayHoldTheRecords(t *testing.T) {
	for _, tc := range []struct {
		name         string
		movedBack    bool
		pathSource   string // "" for no move marker at the path
		pathLog      bool   // the path holds a log with a record
		ownerUnknown bool   // the assignment read fails
		wantSetAside bool
	}{
		{name: "an earlier install from the source is the partition", pathSource: "narad-src"},
		{name: "this worker moved its install back", movedBack: true, pathLog: true, wantSetAside: true},
		{name: "nothing under the partition's path", wantSetAside: true},
		{name: "the path holds records that came from no move", pathLog: true, wantSetAside: true},
		{name: "the path holds an install from another source", pathSource: "narad-other", wantSetAside: true},
		{name: "the owner cannot be read after this worker moved its install back", movedBack: true, ownerUnknown: true, wantSetAside: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeMoveStore{assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-dst"}}
			if tc.ownerUnknown {
				store.assignErr = errors.New("metastore unavailable")
			}
			dataDir := t.TempDir()
			r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, nil, nil, nil, MoveConfig{})
			w := &moveWorker{r: r, topic: "orders", partition: 0, source: "narad-src", staging: r.stagingDir("orders", 0), movedBack: tc.movedBack}
			buildSourcePartition(t, w.staging, 3)
			dir := topicPartitionDirT(t, dataDir, "orders", 0)
			if tc.pathLog {
				buildSourcePartition(t, dir, 1)
			}
			if tc.pathSource != "" {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := messaging.WriteMoveMarker(dir, messaging.MoveMarker{Source: tc.pathSource, HighWatermark: 3}); err != nil {
					t.Fatal(err)
				}
			}
			w.finish()
			if _, err := os.Stat(w.staging); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the staging copy is still at the staging path (stat err %v): the next worker's start clears that path", err)
			}
			q := quarantinesOf(t, w.staging)
			if got := len(q) == 1; got != tc.wantSetAside {
				t.Fatalf("staging set aside = %v (quarantines %v), want %v", got, q, tc.wantSetAside)
			}
			if !tc.wantSetAside {
				return
			}
			// The next move of the partition onto this node starts by
			// clearing the staging path; the copy set aside survives it.
			if err := os.RemoveAll(r.stagingDir("orders", 0)); err != nil {
				t.Fatal(err)
			}
			if n := nextOffsetAt(t, q[0]); n != 3 {
				t.Fatalf("the copy set aside recovers next offset %d, want 3", n)
			}
		})
	}
}

// The error line for a moved-back install set aside while the owner
// cannot be read names the partition's path and the lookup error, and
// carries no path error field when the path formed fine: an empty error
// field on the line asking an operator to act reads as a second failure.
func TestMoveWorkerMovedBackSetAsideLineHasNoEmptyPathError(t *testing.T) {
	store := &fakeMoveStore{assignErr: errors.New("metastore unavailable")}
	dataDir := t.TempDir()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, nil, nil, logger, MoveConfig{})
	w := &moveWorker{r: r, topic: "orders", partition: 0, source: "narad-src", staging: r.stagingDir("orders", 0), movedBack: true}
	buildSourcePartition(t, w.staging, 1)
	w.finish()
	out := logs.String()
	if !strings.Contains(out, "moved back") {
		t.Fatalf("no set-aside line logged:\n%s", out)
	}
	if strings.Contains(out, "partition_dir_err") {
		t.Errorf("the set-aside line carries a path error although the path formed:\n%s", out)
	}
	if !strings.Contains(out, "partition_dir="+topicPartitionDirT(t, dataDir, "orders", 0)) {
		t.Errorf("the set-aside line does not name the partition's path:\n%s", out)
	}
	if !strings.Contains(out, "metastore unavailable") {
		t.Errorf("the set-aside line does not carry the lookup error:\n%s", out)
	}
}

// readerPeer answers the leader read with a fixed assignment, as the
// given kind of node.
type readerPeer struct {
	movePeerFake
	a          metastore.Assignment
	found      bool
	leaderRead bool
}

func (p readerPeer) leaderAssignment(context.Context, string, string, int) (metastore.Assignment, bool, bool, error) {
	return p.a, p.found, p.leaderRead, nil
}

// plainPeer answers GetAssignment with a fixed assignment, as a client
// that cannot tell a leader read from any other would.
type plainPeer struct {
	movePeerFake
	a metastore.Assignment
}

func (p plainPeer) GetAssignment(context.Context, string, string, int) (metastore.Assignment, error) {
	return p.a, nil
}

// A follower destination resolves a flip with the leader's answer. Only
// a barriered leader read may say the flip cannot happen (and so
// authorize undoing the install): a lagging replica or an older leader
// can report a state the flip has already left. That this node owns the
// partition is proof from any read.
func TestMoveRunnerTrustsOnlyLeaderReadsToUndoAnInstall(t *testing.T) {
	ctx := context.Background()
	store := &fakeMoveStore{
		notLeader: true, leaderID: "narad-leader",
		member: metastore.Member{ID: "narad-leader", Addr: "leaderaddr", Status: metastore.MemberAlive},
	}
	notYet := metastore.Assignment{Topic: "orders", OwnerID: "narad-src", TargetID: "narad-dst"}
	replanned := metastore.Assignment{Topic: "orders", OwnerID: "narad-src", TargetID: "narad-other"}
	flipped := metastore.Assignment{Topic: "orders", OwnerID: "narad-dst"}
	for _, tc := range []struct {
		name       string
		peer       movePeer
		want       flipOutcome
		leaderRead bool
	}{
		{"leader read: replanned", readerPeer{a: replanned, found: true, leaderRead: true}, flipRejected, true},
		{"not a leader read: replanned", readerPeer{a: replanned, found: true}, flipUnknown, false},
		{"leader read: missing", readerPeer{leaderRead: true}, flipRejected, true},
		{"not a leader read: missing", readerPeer{}, flipUnknown, false},
		{"leader read: not yet", readerPeer{a: notYet, found: true, leaderRead: true}, flipNotYet, true},
		{"not a leader read: not yet", readerPeer{a: notYet, found: true}, flipNotYet, false},
		{"leader read: flipped", readerPeer{a: flipped, found: true, leaderRead: true}, flipDone, true},
		{"not a leader read: flipped", readerPeer{a: flipped, found: true}, flipDone, false},
		{"older client: replanned", plainPeer{a: replanned}, flipUnknown, false},
		{"older client: not yet", plainPeer{a: notYet}, flipNotYet, false},
		{"older client: flipped", plainPeer{a: flipped}, flipDone, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewMoveRunner(store, "narad-dst", t.TempDir(), tc.peer, nil, nil, nil, MoveConfig{})
			v := r.resolveFlip(ctx, "orders", 0, "narad-src", "")
			if v.outcome != tc.want || v.leaderRead != tc.leaderRead {
				t.Fatalf("verdict %+v, want outcome %d leader_read %v", v, tc.want, tc.leaderRead)
			}
		})
	}
}

// chunkCountingPeer counts segment chunk fetches.
type chunkCountingPeer struct {
	movePeerFake
	chunks *atomic.Int32
}

func (p chunkCountingPeer) FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, base, at, length int64) ([]byte, error) {
	p.chunks.Add(1)
	return p.movePeerFake.FetchSegmentChunk(ctx, addr, topicName, partition, base, at, length)
}

// The flip's outcome is unknown, the leader confirms it did not happen,
// and the source's freeze has lapsed. Once no proposal can still commit,
// the install is renamed back to staging and the next attempt resumes
// from it: it fetches nothing it already has, and the move flips.
func TestMoveRunnerResumesTheCopyAfterMovingTheInstallBack(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 12)
	calls := 0
	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		completeErr: context.DeadlineExceeded,
	}
	// Runs under store.mu inside CompleteMove, before the error check:
	// the first flip fails, every later one commits.
	store.completeHook = func() {
		calls++
		if calls >= 2 {
			store.completeErr = nil
		}
	}
	// Fenced re-arm 1 is the fence before the flip; re-arm 2, the
	// resolution's, finds the freeze gone.
	freeze := &fakeFreeze{lapseOn: 2}
	var chunks atomic.Int32
	peer := chunkCountingPeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: freeze},
		chunks:       &chunks,
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond, FreezeTTL: time.Minute})
	r.flipSettle = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	store.mu.Lock()
	gotCalls := calls
	store.mu.Unlock()
	if gotCalls != 2 {
		t.Errorf("CompleteMove calls = %d, want 2 (one failed, one after the re-drain)", gotCalls)
	}
	freeze.mu.Lock()
	minted, lapsed := freeze.minted, freeze.lapsed
	freeze.mu.Unlock()
	if minted != 2 || lapsed != 1 {
		t.Errorf("freezes minted %d, refused %d; want the lapsed freeze replaced by one fresh freeze", minted, lapsed)
	}
	segs, err := storage.ListPartitionSegments(src)
	if err != nil {
		t.Fatal(err)
	}
	nonEmpty := 0
	for _, s := range segs {
		if s.SizeBytes > 0 {
			nonEmpty++
		}
	}
	if got := int(chunks.Load()); got != nonEmpty {
		t.Errorf("chunk fetches = %d, want %d (one per segment): the moved-back copy was fetched again", got, nonEmpty)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// testClock is a settable clock for the runner's own durations.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: time.Unix(1_800_000_000, 0)} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// slowLeaderPeer answers the leader read like readerPeer, and the read
// takes `took` on the worker's clock.
type slowLeaderPeer struct {
	readerPeer
	clock *testClock
	took  time.Duration
}

func (p slowLeaderPeer) leaderAssignment(ctx context.Context, addr, topicName string, partition int) (metastore.Assignment, bool, bool, error) {
	p.clock.Advance(p.took)
	return p.readerPeer.leaderAssignment(ctx, addr, topicName, partition)
}

// The settle window is judged by when the leader read starts. A read
// that starts inside the window and returns after it may carry a
// barrier taken inside it, which a proposal still on its way to the
// leader can follow, so it must not undo the install. The next read,
// started after the window, may.
func TestMoveRunnerUndoesAnInstallOnlyOnAReadStartedAfterTheSettleWindow(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	store := &fakeMoveStore{
		notLeader: true, leaderID: "narad-leader",
		member: metastore.Member{ID: "narad-leader", Addr: "leaderaddr", Status: metastore.MemberAlive},
	}
	replanned := metastore.Assignment{Topic: "orders", OwnerID: "narad-src", TargetID: "narad-other"}
	const readTakes = 2 * time.Second
	peer := slowLeaderPeer{readerPeer: readerPeer{a: replanned, found: true, leaderRead: true}, clock: clock, took: readTakes}
	installer := &dirInstaller{}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, installer, nil, nil, MoveConfig{})
	r.now = clock.Now
	dir := topicPartitionDirT(t, dataDir, "orders", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := messaging.MoveMarker{Source: "narad-src", HighWatermark: 3, InstalledAtUnixMs: 1, DurableAtUnixMs: 1}
	if err := messaging.WriteMoveMarker(dir, marker); err != nil {
		t.Fatal(err)
	}
	installed, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	w := &moveWorker{r: r, topic: "orders", partition: 0, source: "narad-src", staging: r.stagingDir("orders", 0), started: time.Now()}
	w.pending = &pendingFlip{installed: installed, marker: marker, since: clock.Now(), unknownAt: clock.Now()}

	// The read starts a second before the window closes and returns a
	// second after it.
	clock.Advance(r.flipSettle - readTakes/2)
	w.resolvePending(ctx)
	if n := len(installer.replacements()); n != 0 || w.exit || w.pending == nil {
		t.Fatalf("a read started %v after the unknown reply (window %v) undid the install: replacements %d, exit %v",
			r.flipSettle-readTakes/2, r.flipSettle, n, w.exit)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the install left the partition's path: %v", err)
	}

	// The next read starts after the window: the refusal it reads may
	// undo the install, which goes back to staging.
	w.resolvePending(ctx)
	if n := len(installer.replacements()); n != 1 || !w.exit {
		t.Fatalf("a read started after the window did not undo the install: replacements %d, exit %v", n, w.exit)
	}
	if _, err := os.Stat(w.staging); err != nil {
		t.Fatalf("the install was not moved back to staging: %v", err)
	}
}

// freezeLogPeer counts fresh (token-less) freezes and records, for every
// fenced re-arm, how many fresh freezes had been asked for by then and
// the worker's clock.
type freezeLogPeer struct {
	movePeerFake
	clock *testClock

	mu     *sync.Mutex
	fresh  *int
	rearms *[]rearmAt
}

type rearmAt struct {
	fresh int
	at    time.Time
}

func (p freezeLogPeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	p.mu.Lock()
	if token == "" {
		*p.fresh++
	} else {
		*p.rearms = append(*p.rearms, rearmAt{fresh: *p.fresh, at: p.clock.Now()})
	}
	p.mu.Unlock()
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

// Every flip's outcome is unknown and the leader keeps reading "not
// flipped yet". The worker re-proposes under re-arms of the freeze it
// holds, but only for movePendingFreezeLimit on its own clock: past it it
// stops re-arming, moves the install back once the leader confirms it
// did not flip and the settle window passed, and starts a fresh drain.
// One pending flip never holds the source frozen past the limit.
func TestMoveRunnerBoundsFlipRetriesUnderTheFreeze(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	store := &unknownFlipStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}, err: fmt.Errorf("%w: raft apply timed out", errs.ErrUnavailable)}
	clock := newTestClock()
	var (
		mu     sync.Mutex
		fresh  int
		rearms []rearmAt
	)
	// The freeze's own clock never moves: it holds for as long as the
	// worker keeps re-arming it, so only the worker's limit can stop that.
	peer := freezeLogPeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: &fakeFreeze{virtual: true}},
		clock:        clock, mu: &mu, fresh: &fresh, rearms: &rearms,
	}
	installer := &dirInstaller{}
	r := NewMoveRunner(store, "narad-dst", t.TempDir(), peer, installer, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	r.now = clock.Now
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	defer func() { cancel(); r.wg.Wait() }()
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for !cond() {
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for %s (flips %d, replacements %d)", what, store.flips.Load(), len(installer.replacements()))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	freshFreezes := func() int {
		mu.Lock()
		defer mu.Unlock()
		return fresh
	}
	start := clock.Now()
	waitFor("three flip proposals", func() bool { return store.flips.Load() >= 3 })
	if got := freshFreezes(); got != 1 {
		t.Fatalf("fresh freezes %d while re-proposing the flip, want the one it holds", got)
	}
	if got := len(installer.replacements()); got != 1 {
		t.Fatalf("partition directory replaced %d times while re-proposing, want only the install", got)
	}

	// Pass the limit on the worker's clock, and keep the clock moving:
	// the settle window runs from the last proposal, which may land
	// after the jump.
	clock.Advance(movePendingFreezeLimit + time.Second)
	waitFor("a fresh drain after the flip retries stopped", func() bool {
		clock.Advance(time.Second)
		return freshFreezes() >= 2
	})
	if got := len(installer.replacements()); got < 2 {
		t.Fatalf("partition directory replaced %d times, want the install moved back before the fresh drain", got)
	}
	mu.Lock()
	defer mu.Unlock()
	late := 0
	for _, re := range rearms {
		if re.fresh == 1 && re.at.Sub(start) > movePendingFreezeLimit {
			late++
		}
	}
	// One re-arm can be in flight as the clock jumps; no more.
	if late > 1 {
		t.Fatalf("the pending flip re-armed the source's freeze %d times past the %v limit", late, movePendingFreezeLimit)
	}
}

// electionStore is a follower destination whose first look at the
// leader lands inside an election (LeaderID() == ""), so the flip is
// never sent. Every later look finds a leader.
type electionStore struct {
	*fakeMoveStore
	noLeaderLeft atomic.Int32
}

func (s *electionStore) IsLeader() bool { return false }

func (s *electionStore) LeaderID() string {
	if s.noLeaderLeft.Add(-1) >= 0 {
		return ""
	}
	return "narad-leader"
}

// countingPeer counts fresh freezes and forwarded flips; every
// forwarded flip commits.
type countingPeer struct {
	movePeerFake
	prepares *atomic.Int32
	flips    *atomic.Int32
}

func (p countingPeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if token == "" {
		p.prepares.Add(1)
	}
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

func (p countingPeer) CompleteMove(context.Context, string, string, int, string, string) error {
	p.flips.Add(1)
	return nil
}

// A follower destination's first flip finds no leader (an election), so
// it is never sent; the leader found afterwards never answers the
// assignment read. The worker keeps the install and forwards the flip
// again under the freeze it holds.
func TestMoveRunnerRetriesTheFlipWhenNoLeaderWasKnown(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	store := &electionStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}}
	store.noLeaderLeft.Store(1)
	var prepares, flips atomic.Int32
	peer := countingPeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: &fakeFreeze{}},
		prepares:     &prepares, flips: &flips,
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 20 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if flips.Load() == 0 {
		t.Fatalf("WEDGED: after one 'no known leader' the worker never forwarded the flip (fresh freezes %d)", prepares.Load())
	}
	if prepares.Load() != 1 {
		t.Errorf("fresh freezes = %d, want 1: the retried flip must reuse the freeze it holds", prepares.Load())
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// dyingSourcePeer freezes the source once, then cannot reach it.
type dyingSourcePeer struct {
	movePeerFake
	dead *atomic.Bool
}

func (p dyingSourcePeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if p.dead.Load() {
		return messaging.PartitionTransferInfo{}, errors.New("dial srcaddr: connection refused")
	}
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

// dyingSourceStore answers the first flip with an unknown outcome
// without committing it, and kills the source at that moment; later
// flips commit.
type dyingSourceStore struct {
	*fakeMoveStore
	dead  *atomic.Bool
	calls atomic.Int32
}

func (s *dyingSourceStore) CompleteMove(ctx context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	if s.calls.Add(1) == 1 {
		s.dead.Store(true)
		s.mu.Lock()
		s.member.Status, s.member.LastHeartbeat = metastore.MemberDead, 0
		s.mu.Unlock()
		return fmt.Errorf("%w: peer rpc: reply timeout", errs.ErrUnavailable)
	}
	return s.fakeMoveStore.CompleteMove(ctx, topicName, partition, expectedOwner, targetID)
}

// The source dies right as the flip is proposed and the reply is lost.
// Its freeze cannot be re-armed any more, so the pending install can
// never be flipped as a live move; once the source has been dead long
// enough it is flipped as a force-promote, the whole copy as of the
// fence.
func TestMoveRunnerFlipsAPendingInstallAsForcePromoteWhenTheSourceDies(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 10)
	var dead atomic.Bool
	store := &dyingSourceStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
	}, dead: &dead}
	peer := dyingSourcePeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, freeze: &fakeFreeze{}},
		dead:         &dead,
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{
		RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()
	if ctx.Err() != nil {
		t.Fatalf("WEDGED: the worker never flipped the installed copy of a dead source (flips proposed %d)", store.calls.Load())
	}
	store.mu.Lock()
	owner := store.assignment.OwnerID
	store.mu.Unlock()
	if owner != "narad-dst" {
		t.Fatalf("owner %s, want narad-dst", owner)
	}
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// On Linux a directory removed and created again can get the freed
// inode number back at once, so the partition's path can name a
// successor directory that os.SameFile takes for the install. The
// rollback must still leave it alone: it moves a directory only when it
// also holds the move marker the install wrote. The successor's own
// file identity is passed as the install's, which is what inode reuse
// produces.
func TestRollbackLeavesASuccessorThatReusedTheInstallsInode(t *testing.T) {
	dataDir := t.TempDir()
	r := NewMoveRunner(&fakeMoveStore{}, "narad-dst", dataDir, movePeerFake{}, nil, nil, nil, MoveConfig{})
	dir := topicPartitionDirT(t, dataDir, "orders", 0)
	marker := messaging.MoveMarker{Source: "narad-src", HighWatermark: 10, InstalledAtUnixMs: 1, DurableAtUnixMs: 1}

	for _, tc := range []struct {
		name      string
		successor func(t *testing.T)
		wantMoved bool
	}{
		{"a successor with no marker (fresh produce)", func(t *testing.T) { buildSourcePartition(t, dir, 7) }, false},
		{"a successor installed by another move", func(t *testing.T) {
			buildSourcePartition(t, dir, 7)
			other := marker
			other.InstalledAtUnixMs = 2
			if err := messaging.WriteMoveMarker(dir, other); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"control: the install itself", func(t *testing.T) {
			buildSourcePartition(t, dir, 7)
			if err := messaging.WriteMoveMarker(dir, marker); err != nil {
				t.Fatal(err)
			}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = os.RemoveAll(dir)
			_ = os.RemoveAll(r.stagingDir("orders", 0))
			tc.successor(t)
			now, err := os.Stat(dir)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := r.rollbackInstall("orders", 0, "", now, marker, r.stagingDir("orders", 0))
			if err != nil {
				t.Fatalf("rollbackInstall: %v", err)
			}
			if restored != tc.wantMoved {
				t.Fatalf("rollback moved the directory = %v, want %v", restored, tc.wantMoved)
			}
			if tc.wantMoved {
				return
			}
			if n := nextOffsetAt(t, dir); n != 7 {
				t.Fatalf("LOSS: the successor at the partition's path recovers next offset %d, want 7", n)
			}
		})
	}
}

const (
	oldIncarnation = "1111111111111111"
	newIncarnation = "2222222222222222"
)

// A move's rollback runs after the leader's verdict and acts on the
// partition's path. In between, the path can come to belong to someone
// else: the name deleted and recreated with this node serving the
// successor's partition, the directory replaced under the same
// incarnation, or the topic directory taken over by another incarnation
// with the installed directory still in it. The rename back to staging
// must leave that directory, its records and its consumer state alone
// (the worker removes its staging on exit, so a directory moved there
// would be lost). The incarnation and same-directory checks each guard
// one of these.
func TestMoveRunnerRollbackLeavesARecreatedTopicsPartitionAlone(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string // "recreated", "replaced" or "adopted"
		wantMarker string
		wantNext   int64
	}{
		{"topic deleted and recreated", "recreated", newIncarnation, 7},
		{"directory replaced under the same incarnation", "replaced", oldIncarnation, 7},
		{"topic directory taken over by another incarnation", "adopted", newIncarnation, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			real := newTestStore(t)
			if err := real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: oldIncarnation, Partitions: 1}); err != nil {
				t.Fatal(err)
			}
			src := t.TempDir()
			hwm, _ := buildSourcePartition(t, src, 10)
			dataDir := t.TempDir()
			logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, real, nil)
			t.Cleanup(func() { _ = logs.CloseAll() })

			store := &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
				member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
				topics:     []topic.Topic{{Name: "orders", ID: oldIncarnation, Partitions: 1}},
			}
			if tc.mode == "recreated" {
				// The topic is deleted as the flip is proposed: the CAS
				// finds no assignment, and the leader confirms it.
				store.completeErr = fmt.Errorf("%w: assignment orders/0", errs.ErrNotFound)
				store.completeHook = func() { store.topics = []topic.Topic{} }
			} else {
				// The move is re-planned away as the flip is proposed.
				store.completeHook = func() {
					store.assignment.TargetID = "narad-other"
					store.completeErr = fmt.Errorf("%w: complete-move target is %q, expected %q", errs.ErrInvalidArgument, "narad-other", "narad-dst")
				}
			}
			var hookErr error
			produced := int64(-1)
			produce := func() {
				l, err := logs.Get("orders", 0)
				if err != nil {
					hookErr = err
					return
				}
				for range 7 {
					if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte("successor"))); err != nil {
						hookErr = err
						return
					}
				}
				if hookErr = l.CommitDurable(0, 6); hookErr != nil {
					return
				}
				produced = l.HighWatermark()
			}
			installer := &dirInstaller{logs: logs}
			installer.before = func(n int) {
				if n != 2 {
					return // the first replacement is the install
				}
				// The rollback, after the verdict.
				switch tc.mode {
				case "recreated":
					store.topics = []topic.Topic{{Name: "orders", ID: newIncarnation, Partitions: 1}}
					if hookErr = real.DeleteTopic(ctx, "orders"); hookErr != nil {
						return
					}
					if hookErr = real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: newIncarnation, Partitions: 1}); hookErr != nil {
						return
					}
					produce()
				case "replaced":
					if hookErr = logs.ReplacePartitionDir("orders", 0, func() error {
						return os.RemoveAll(topicPartitionDirT(t, dataDir, "orders", 0))
					}); hookErr != nil {
						return
					}
					produce()
				case "adopted":
					hookErr = storage.WriteTopicIncarnation(topicDirT(t, dataDir, "orders"), newIncarnation)
				}
			}
			peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm, committed: 5, hasCommitted: true}, incarnation: oldIncarnation}
			r := NewMoveRunner(store, "narad-dst", dataDir, peer, keepingEngine{installer}, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
			runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			r.Reconcile(runCtx)
			r.wg.Wait()
			if runCtx.Err() != nil {
				t.Fatal("the worker did not end after the leader confirmed the flip cannot commit")
			}
			if n := len(installer.replacements()); n != 2 {
				t.Fatalf("setup: %d directory replacements, want the install and the rollback", n)
			}
			if hookErr != nil {
				t.Fatalf("setup: the hook failed: %v", hookErr)
			}
			if tc.mode != "adopted" && produced != 7 {
				t.Fatalf("setup: the new directory committed hwm %d, want 7", produced)
			}
			if err := logs.CloseAll(); err != nil {
				t.Logf("close: %v", err)
			}
			if id, marked, _ := storage.ReadTopicIncarnation(topicDirT(t, dataDir, "orders")); !marked || id != tc.wantMarker {
				t.Fatalf("topic marker %q (marked %v), want %s", id, marked, tc.wantMarker)
			}
			l, err := storage.NewLog(topicPartitionDirT(t, dataDir, "orders", 0), storage.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			if got := l.NextOffset(); got != tc.wantNext {
				t.Fatalf("LOSS: the partition's directory recovers next offset %d after the rollback, want %d", got, tc.wantNext)
			}
			if _, err := os.Stat(r.stagingDir("orders", 0)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the rollback moved the partition's directory into the move's staging (stat err %v)", err)
			}
		})
	}
}

// sourceRecordStore answers the move source's member record with status,
// or with errs.ErrNotFound when gone is set.
type sourceRecordStore struct {
	*fakeMoveStore
	status metastore.MemberStatus
	gone   bool
}

func (s sourceRecordStore) GetMember(id string) (metastore.Member, error) {
	if id != "narad-src" {
		return s.fakeMoveStore.GetMember(id)
	}
	if s.gone {
		return metastore.Member{}, fmt.Errorf("member %s: %w", id, errs.ErrNotFound)
	}
	return metastore.Member{ID: id, Addr: "srcaddr", Status: s.status}, nil
}

const deadSourceStagingLog = "move: set aside the staging copy of a move that ended while its source is dead"

// A move that ends without a flip while its source is dead (its target
// was cleared, or the node is shutting down) holds a copy of a
// partition whose other copy is on a node that may never come back: the
// worker sets it aside and says so at error instead of deleting it. A
// live source still has the partition and an empty copy holds nothing,
// so those are removed as before.
func TestMoveEndedWhileTheSourceIsDeadSetsItsStagingAside(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       metastore.MemberStatus
		gone         bool
		records      int
		wantSetAside bool
	}{
		{name: "the source is dead", status: metastore.MemberDead, records: 3, wantSetAside: true},
		{name: "the source has no member record", gone: true, records: 3, wantSetAside: true},
		{name: "the source is alive", status: metastore.MemberAlive, records: 3},
		{name: "the copy holds no records", status: metastore.MemberDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The move was aborted: the source still owns the partition and
			// nothing targets it.
			store := sourceRecordStore{fakeMoveStore: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
			}, status: tc.status, gone: tc.gone}
			logs := &recordedLog{}
			dataDir := t.TempDir()
			r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, nil, nil, slog.New(logs), MoveConfig{})
			w := &moveWorker{r: r, topic: "orders", partition: 0, source: "narad-src", staging: r.stagingDir("orders", 0)}
			if tc.records > 0 {
				buildSourcePartition(t, w.staging, tc.records)
			} else if err := os.MkdirAll(w.staging, 0o755); err != nil {
				t.Fatal(err)
			}
			w.finish()
			if _, err := os.Stat(w.staging); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the staging copy is still at the staging path (stat err %v): the next worker's start clears that path", err)
			}
			q := quarantinesOf(t, w.staging)
			if got := len(q) == 1; got != tc.wantSetAside {
				t.Fatalf("staging set aside = %v (quarantines %v), want %v", got, q, tc.wantSetAside)
			}
			if got := logs.count(slog.LevelError, deadSourceStagingLog); (got == 1) != tc.wantSetAside {
				t.Fatalf("logged %q %d times at error, want set aside %v", deadSourceStagingLog, got, tc.wantSetAside)
			}
			if tc.wantSetAside {
				if n := nextOffsetAt(t, q[0]); n != int64(tc.records) {
					t.Fatalf("the copy set aside recovers next offset %d, want %d", n, tc.records)
				}
			}
		})
	}
}

const deadSourceBehindLog = "cannot force-promote"

// A destination whose copy is behind a dead source's last high
// watermark cannot force-promote and waits for the source. It says so
// once at error each time the source dies, not at warn every
// RetryBackoff.
func TestMoveLogsADeadSourceWithACopyBehindOnce(t *testing.T) {
	logs := &recordedLog{}
	store, clock, _, _, _, _, stop := startDeadSourceScenario(t, 3, slog.New(logs))
	defer stop()
	died := func() {
		t.Helper()
		store.dead.Store(true)
		store.awaitReads(t, 3)
		clock.Advance(3 * time.Minute)
		store.awaitReads(t, 10) // each read past ForcePromoteAfter tries a force-promote
	}
	died()
	if got := logs.count(slog.LevelWarn, deadSourceBehindLog); got != 0 {
		t.Fatalf("logged %q %d times at warn while the source stayed dead, want none", deadSourceBehindLog, got)
	}
	if got := logs.count(slog.LevelError, deadSourceBehindLog); got != 1 {
		t.Fatalf("logged %q %d times at error while the source stayed dead, want once", deadSourceBehindLog, got)
	}
	store.dead.Store(false)
	store.awaitReads(t, 3)
	died()
	if got := logs.count(slog.LevelError, deadSourceBehindLog); got != 2 {
		t.Fatalf("logged %q %d times at error after the source died a second time, want twice", deadSourceBehindLog, got)
	}
	if store.flipped() {
		t.Fatal("force-promoted a copy behind the source's last high watermark")
	}
}
