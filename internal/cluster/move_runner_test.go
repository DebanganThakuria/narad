package cluster

// The move runner is the destination-side reconcile loop. These tests
// drive one full move to completion (copy → install → guarded flip) and
// the abort path (a rejected flip rolls the install back and clears the
// target), against a fake store and a source served off a real partition
// directory — the same bytes the RPC serve side would stream.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// moveForwardRec records leader-forwarded ownership writes.
type moveForwardRec struct {
	completeAddr string
	completeArgs []string
	abortAddr    string
}

// movePeerFake serves a source partition dir and freezes as a no-op (the
// source dir is already static in the test). fwd, when set, records the
// leader-forwarded CompleteMove/AbortMove calls. freeze, when set,
// simulates the source's fenced freeze (TTL + token); without it the
// fake behaves like a pre-token source and reports no token.
type movePeerFake struct {
	dirFetcher
	fwd         *moveForwardRec
	prepareErr  error                 // when set, PrepareHandoff fails (simulates a dead source)
	freeze      *fakeFreeze           // when set, PrepareHandoff fences like the engine does
	marker      *messaging.MoveMarker // reported by ListPartitionSegments (the sweep's owner lookup)
	leaderTopic *topic.Topic          // answered by GetTopic (the sweep's leader confirmation); nil errors
	incarnation string                // reported by ListPartitionSegments as the copy's incarnation
	onList      func()                // when set, runs inside every listing (a drain that outlives the freeze TTL)
	lists       *atomic.Int32         // when set, counts listings
}

func (f movePeerFake) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	if f.lists != nil {
		f.lists.Add(1)
	}
	if f.onList != nil {
		f.onList()
	}
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	if err != nil {
		return info, err
	}
	info.MoveMarker = f.marker
	info.IncarnationID = f.incarnation
	return info, nil
}

func (f movePeerFake) PrepareHandoff(_ context.Context, _, _ string, _ int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	if f.prepareErr != nil {
		return messaging.PartitionTransferInfo{}, f.prepareErr
	}
	info := messaging.PartitionTransferInfo{
		Segments:        mustSegs(f.dir),
		HighWatermark:   f.hwm,
		CommittedOffset: f.committed,
		HasCommitted:    f.hasCommitted,
	}
	if f.freeze != nil {
		got, err := f.freeze.arm(ttl, token)
		if err != nil {
			return messaging.PartitionTransferInfo{}, err
		}
		info.FreezeToken = got
	}
	return info, nil
}

// fakeFreeze mirrors the engine's fenced freeze: a token is minted when
// a freeze is armed, a re-arm with the token extends it, and a re-arm
// with a token whose freeze lapsed fails. Tests can force a lapse.
type fakeFreeze struct {
	mu       sync.Mutex
	token    string
	deadline time.Time
	minted   int
	rearms   int
	lapsed   int // fenced re-arms refused
	lapseOn  int // when >0, the Nth fenced re-arm finds the freeze lapsed
	// virtual judges the TTL on the freeze's own clock (vnow), which
	// only elapse moves, instead of the wall clock. The outcome then
	// depends on the order of re-arms and listings alone, not on how
	// quickly a loaded machine gets round to them.
	virtual bool
	vnow    time.Time
}

// nowLocked is the freeze's clock. Called with f.mu held.
func (f *fakeFreeze) nowLocked() time.Time {
	if f.virtual {
		return f.vnow
	}
	return time.Now()
}

// elapse moves a virtual freeze's clock on by d, as if the source spent
// d serving a request, and reports how many fenced re-arms had landed
// by then. It holds f.mu, so a concurrent re-arm lands wholly before
// the move (and is counted) or wholly after it (and sees the new time).
// Nothing moves before the first freeze is armed: the pre-copy listings
// run unfrozen, and frozen=false says so.
func (f *fakeFreeze) elapse(d time.Duration) (rearms int, frozen bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.minted == 0 {
		return f.rearms, false
	}
	f.vnow = f.vnow.Add(d)
	return f.rearms, true
}

// awaitRearm waits, up to timeout of wall time, for a fenced re-arm
// after the first `after` ones. It reports whether one landed.
func (f *fakeFreeze) awaitRearm(after int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		f.mu.Lock()
		n := f.rearms
		f.mu.Unlock()
		if n > after {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *fakeFreeze) arm(ttl time.Duration, token string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.nowLocked()
	active := f.token != "" && now.Before(f.deadline)
	if token != "" {
		f.rearms++
		if f.lapseOn > 0 && f.rearms == f.lapseOn {
			active = false
			f.token = ""
		}
		if !active || f.token != token {
			f.lapsed++
			return "", messaging.ErrHandoffFreezeLapsed
		}
	}
	if !active {
		f.minted++
		f.token = "tok-" + strconv.Itoa(f.minted)
	}
	f.deadline = now.Add(ttl)
	return f.token, nil
}

// activeToken reports the freeze's current token if it is armed and unexpired.
func (f *fakeFreeze) activeToken() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.token == "" || !f.nowLocked().Before(f.deadline) {
		return "", false
	}
	return f.token, true
}

func (f movePeerFake) CompleteMove(_ context.Context, addr, topicName string, partition int, expectedOwner, targetID string) error {
	if f.fwd != nil {
		f.fwd.completeAddr = addr
		f.fwd.completeArgs = []string{topicName, expectedOwner, targetID}
	}
	return nil
}

func (f movePeerFake) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	if f.leaderTopic != nil {
		body, err := json.Marshal(f.leaderTopic)
		return nodewire.Response{Status: http.StatusOK, Body: body}, err
	}
	return nodewire.Response{}, errors.New("no leader topic view")
}

func (f movePeerFake) GetAssignment(context.Context, string, string, int) (metastore.Assignment, error) {
	return metastore.Assignment{}, context.DeadlineExceeded
}

func (f movePeerFake) AbortMove(_ context.Context, addr, _ string, _ int, _ string) error {
	if f.fwd != nil {
		f.fwd.abortAddr = addr
	}
	return nil
}

func mustSegs(dir string) []storage.SegmentInfo {
	segs, _ := storage.ListPartitionSegments(dir)
	return segs
}

type fakeMoveStore struct {
	mu           sync.Mutex
	assignment   metastore.Assignment
	member       metastore.Member
	completeErr  error
	completeArgs []string
	abortArgs    []string
	notLeader    bool   // when true, IsLeader() is false → runner must forward
	leaderID     string // resolved via GetMember to the leader's addr
	completeHook func() // called on every CompleteMove (e.g. to cancel the worker)
	deadAfter    int    // >0: GetMember(source) reports MemberDead from this call on
	memberCalls  int
	topics       []topic.Topic // nil: a single "orders" topic
}

func (s *fakeMoveStore) AppliedCaughtUp() bool { return true }
func (s *fakeMoveStore) Barrier() error        { return nil }
func (s *fakeMoveStore) GetAssignment(string, int) (metastore.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.assignment, nil
}
func (s *fakeMoveStore) IsLeader() bool { return !s.notLeader }
func (s *fakeMoveStore) LeaderID() string {
	if s.leaderID != "" {
		return s.leaderID
	}
	return "narad-dst"
}

func (s *fakeMoveStore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	if s.topics != nil {
		return s.topics, "", nil
	}
	return []topic.Topic{{Name: "orders", Partitions: 1}}, "", nil
}

func (s *fakeMoveStore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	topics, _, _ := s.ListTopics(ctx, metastore.ListOptions{})
	for _, t := range topics {
		if t.Name == name {
			return t, nil
		}
	}
	return topic.Topic{}, errs.ErrNotFound
}

func (s *fakeMoveStore) ListAssignments(string) ([]metastore.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []metastore.Assignment{s.assignment}, nil
}

func (s *fakeMoveStore) GetMember(id string) (metastore.Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.member
	if id == s.member.ID && s.deadAfter > 0 {
		s.memberCalls++
		if s.memberCalls >= s.deadAfter {
			m.Status = metastore.MemberDead
			m.LastHeartbeat = 0 // epoch — long dead, past any ForcePromoteAfter
		}
	}
	return m, nil
}

func (s *fakeMoveStore) CompleteMove(_ context.Context, topicName string, partition int, expectedOwner, targetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeArgs = []string{topicName, expectedOwner, targetID}
	if s.completeHook != nil {
		s.completeHook()
	}
	if s.completeErr == nil {
		s.assignment.OwnerID = targetID
		s.assignment.TargetID = ""
	}
	return s.completeErr
}

func (s *fakeMoveStore) AbortMove(_ context.Context, topicName string, partition int, expectedTarget string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortArgs = []string{topicName, expectedTarget}
	return nil
}

func newMoveTestRunner(t *testing.T, store *fakeMoveStore, srcDir string, hwm int64) (*MoveRunner, string, *metrics.Metrics) {
	t.Helper()
	peer := movePeerFake{dirFetcher: dirFetcher{dir: srcDir, hwm: hwm, committed: 5, hasCommitted: true}}
	dataDir := t.TempDir()
	m := metrics.New(prometheus.NewRegistry())
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, m, nil, MoveConfig{})
	return r, dataDir, m
}

// moveCounter reads a MovesTotal outcome counter's current value.
func moveCounter(t *testing.T, m *metrics.Metrics, outcome string) float64 {
	t.Helper()
	var pb dto.Metric
	if err := m.MovesTotal.WithLabelValues(outcome).Write(&pb); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return pb.GetCounter().GetValue()
}

func TestMoveRunnerCompletesMove(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 20)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	r, dataDir, m := newMoveTestRunner(t, store, src, wantHWM)

	r.Reconcile(context.Background())
	r.wg.Wait()

	if got := store.completeArgs; len(got) != 3 || got[0] != "orders" || got[1] != "narad-src" || got[2] != "narad-dst" {
		t.Fatalf("CompleteMove args = %v, want [orders narad-src narad-dst]", got)
	}
	// The completed move is observed: outcome counter up, in-flight back to 0.
	if got := moveCounter(t, m, "completed"); got != 1 {
		t.Fatalf("moves_total{completed} = %v, want 1", got)
	}
	// The partition is installed at its real location and recovers to the
	// source's HWM with byte-identical records.
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatalf("recover installed partition: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("installed NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	for off, want := range payloads {
		if _, _, got, err := log.ReadKeyed(off); err != nil || string(got) != string(want) {
			t.Fatalf("offset %d = %q (err %v), want %q", off, got, err, want)
		}
	}
}

// A flip the leader confirms cannot happen (the CAS refused it because
// the move was re-planned to another node) is rolled back: the install
// is taken off the partition's path, so a non-owner keeps no phantom
// copy, the worker ends (the re-plan's own worker takes over), and it
// leaves no staging copy behind.
func TestMoveRunnerRollsBackOnConfirmedFlipReject(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		completeErr: fmt.Errorf("%w: complete-move target is %q, expected %q", errs.ErrInvalidArgument, "narad-other", "narad-dst"),
	}
	// The re-plan landed just before the flip: the CAS refuses it.
	store.completeHook = func() { store.assignment.TargetID = "narad-other" }
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if len(store.completeArgs) == 0 {
		t.Fatal("flip was never attempted")
	}
	if ctx.Err() != nil {
		t.Fatal("the worker kept running after the leader confirmed the flip was rejected")
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if segs, _ := storage.ListPartitionSegments(dir); len(segs) != 0 {
		t.Fatalf("install not rolled back: %d segments remain at %s", len(segs), dir)
	}
	if _, err := os.Stat(r.stagingDir("orders", 0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the worker left its staging copy behind (stat err %v)", err)
	}
}

// When the source dies mid-move and stays dead past ForcePromoteAfter, the
// destination promotes the copy it already caught up — completing the move
// without the source rather than stalling forever. Gated: it only promotes
// because the copy reached the source's last-known HWM.
func TestMoveRunnerForcePromotesDeadSource(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 15)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		deadAfter:  2, // alive for the copy pass, dead from the 2nd lookup on
	}
	// PrepareHandoff fails (the source just died), so the normal cutover can't
	// finish; the next loop sees the source dead and force-promotes.
	peer := movePeerFake{
		dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		prepareErr: context.DeadlineExceeded,
	}
	dataDir := t.TempDir()
	m := metrics.New(prometheus.NewRegistry())
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, m, nil, MoveConfig{
		RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if got := store.completeArgs; len(got) != 3 || got[2] != "narad-dst" {
		t.Fatalf("force-promote did not complete the move: %v", got)
	}
	if got := moveCounter(t, m, "force_promoted"); got != 1 {
		t.Fatalf("moves_total{force_promoted} = %v, want 1", got)
	}
	// The promoted copy recovers to the source's last-known HWM, records intact.
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatalf("recover promoted partition: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("promoted NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	for off, want := range payloads {
		if _, _, got, err := log.ReadKeyed(off); err != nil || string(got) != string(want) {
			t.Fatalf("offset %d = %q (err %v), want %q", off, got, err, want)
		}
	}
}

// Force-promote is REFUSED when the copy never caught up to the source's
// last-known HWM — promoting a truncated copy would drop records the source
// had already made visible. The move must stall (wait), not lose data.
func TestMoveRunnerForcePromoteRefusesBehindCopy(t *testing.T) {
	src := t.TempDir()
	// Source HWM is well beyond what the fetcher will serve: make the fetcher
	// report a hwm the staged copy can't reach.
	wantHWM, _ := buildSourcePartition(t, src, 8)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Status: metastore.MemberDead, LastHeartbeat: 0},
	}
	// Source already dead from the start → no copy ever runs → sawInfo false →
	// ForcePromote refuses. Nothing should be installed or flipped.
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{
		RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: time.Millisecond,
	})
	// Need an address so a session is begun.
	store.member.Addr = "srcaddr"

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if len(store.completeArgs) != 0 {
		t.Fatalf("force-promote flipped without ever reaching the source: %v", store.completeArgs)
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if segs, _ := storage.ListPartitionSegments(dir); len(segs) != 0 {
		t.Fatalf("a partition was installed despite a refused force-promote: %d segments", len(segs))
	}
}

// When this node is a FOLLOWER, the ownership flip is a Raft write that only
// the leader can apply — the runner must forward CompleteMove to the leader's
// address, not call the local store (which would fail with not-leader). This
// pins the fix for the bug the docker e2e surfaced: a destination that is not
// the leader could never complete a move.
func TestMoveRunnerForwardsFlipToLeaderWhenFollower(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 12)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		notLeader:  true,
		leaderID:   "narad-leader",
	}
	rec := &moveForwardRec{}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}, fwd: rec}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{})

	// The runner resolves the leader's address via GetMember(leaderID); make
	// that return a routable addr.
	store.member = metastore.Member{ID: "narad-leader", Addr: "leaderaddr", Status: metastore.MemberAlive}
	// GetMember is used for BOTH the source and the leader here; point source
	// resolution at a live addr too by giving the assignment a self-consistent
	// owner the fake resolves. (fakeMoveStore.GetMember returns s.member for
	// any id, so source and leader share one addr — fine for this assertion.)

	r.Reconcile(context.Background())
	r.wg.Wait()

	if rec.completeAddr != "leaderaddr" {
		t.Fatalf("flip forwarded to %q, want leader addr \"leaderaddr\"", rec.completeAddr)
	}
	if got := rec.completeArgs; len(got) != 3 || got[0] != "orders" || got[1] != "narad-src" || got[2] != "narad-dst" {
		t.Fatalf("forwarded CompleteMove args = %v, want [orders narad-src narad-dst]", got)
	}
	// The local store's CompleteMove must NOT have been used on a follower.
	if store.completeArgs != nil {
		t.Fatalf("follower called local store.CompleteMove (%v) instead of forwarding", store.completeArgs)
	}
}

// A drain that outlives the freeze TTL must keep the source frozen: the
// worker re-arms the freeze with its token while Finalize runs and fences
// the flip with it. Here every listing under the freeze takes longer than
// the TTL, so the freeze would lapse (and the source would resume commits
// behind the copy) without the re-arm; the move must still flip, and only
// while the freeze is active.
//
// The TTL is judged on the freeze's own clock, not the wall's. With a
// wall-clock TTL the test raced the runner's re-arm ticker, and then the
// install, against the TTL: one stalled tick, or an install slower than
// the TTL on a loaded two-core -race runner, lapsed the freeze and failed
// it at random. Here a listing under the freeze spends 1.5 TTLs of that
// clock, in two halves of 3/4, and before each half it waits for one of
// the runner's periodic re-arms to land while it is still running. Each
// half fits inside the TTL a re-arm just renewed, so the freeze holds;
// re-arms that only land between listings leave it at most one TTL from
// the listing's start, and it lapses before the listing ends. Nothing
// else moves the clock, so the install and the flip run under the
// fence's renewal whatever the machine's speed.
func TestMoveRunnerRearmsFreezeAcrossTTLAndFencesFlip(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	freeze := &fakeFreeze{virtual: true}
	var activeAtFlip atomic.Bool
	var missedRearms atomic.Int32
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	store.completeHook = func() {
		_, active := freeze.activeToken()
		activeAtFlip.Store(active)
	}
	const ttl = time.Second
	peer := movePeerFake{
		dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		freeze:     freeze,
		onList: func() {
			for range 2 {
				before, frozen := freeze.elapse(0)
				if !frozen {
					return
				}
				if !freeze.awaitRearm(before, 30*time.Second) {
					missedRearms.Add(1)
				}
				freeze.elapse(ttl * 3 / 4)
			}
		},
	}
	// The re-arm ticker runs on the wall clock; it only has to tick
	// while a listing waits for it, so its period sets the test's speed,
	// not its outcome.
	r := NewMoveRunner(store, "narad-dst", t.TempDir(), peer, nil, nil, nil, MoveConfig{
		FreezeTTL: ttl, FreezeRearmEvery: 10 * time.Millisecond, RetryBackoff: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if n := missedRearms.Load(); n != 0 {
		t.Fatalf("%d listing(s) under the freeze saw no re-arm while they ran", n)
	}
	if got := store.completeArgs; len(got) != 3 || got[2] != "narad-dst" {
		t.Fatalf("move did not flip: %v", got)
	}
	if !activeAtFlip.Load() {
		t.Fatal("the flip was proposed while the source's freeze was NOT active")
	}
	freeze.mu.Lock()
	minted, rearms, lapsed := freeze.minted, freeze.rearms, freeze.lapsed
	freeze.mu.Unlock()
	if minted != 1 || lapsed != 0 {
		t.Fatalf("freeze minted %d times, refused %d re-arms; want a single freeze held continuously", minted, lapsed)
	}
	if rearms < 2 {
		t.Fatalf("fenced re-arms = %d, want at least a periodic re-arm plus the fence", rearms)
	}
}

// When the freeze DOES lapse mid-cutover (the source refuses the token),
// the worker must not flip on the copy it drained: it re-freezes under a
// fresh token, drains again, and only then proposes the flip.
func TestMoveRunnerRefusesFlipOnLapsedFreezeAndRefreezes(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	freeze := &fakeFreeze{lapseOn: 1} // the first fenced re-arm finds the freeze gone
	var tokenAtFlip atomic.Value
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	store.completeHook = func() {
		tok, active := freeze.activeToken()
		if !active {
			tok = "<inactive>"
		}
		tokenAtFlip.Store(tok)
	}
	peer := movePeerFake{
		dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		freeze:     freeze,
	}
	r := NewMoveRunner(store, "narad-dst", t.TempDir(), peer, nil, nil, nil, MoveConfig{
		FreezeTTL: time.Minute, RetryBackoff: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if got := store.completeArgs; len(got) != 3 || got[2] != "narad-dst" {
		t.Fatalf("move did not flip: %v", got)
	}
	freeze.mu.Lock()
	minted, lapsed := freeze.minted, freeze.lapsed
	freeze.mu.Unlock()
	if lapsed != 1 || minted != 2 {
		t.Fatalf("freeze refused %d re-arms and was minted %d times; want 1 refusal then one fresh freeze", lapsed, minted)
	}
	if got := tokenAtFlip.Load(); got != "tok-2" {
		t.Fatalf("flip proposed under freeze token %v, want the fresh freeze tok-2", got)
	}
}

// The installed copy carries a move marker: the promoted HWM the old
// owner's sweep compares against, and the parent's child links at
// install time, which the fan-out reconciler uses to tell a lost cursor
// from a fresh attach.
func TestMoveRunnerWritesMoveMarker(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 6)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics: []topic.Topic{
			{Name: "orders", Partitions: 1, Role: topic.RoleParent, Children: []string{"audit", "other"}},
			{Name: "audit", Partitions: 1, Role: topic.RoleChild, Parent: "orders", AttachEpoch: "e1"},
			{Name: "other", Partitions: 1, Role: topic.RoleChild, Parent: "orders", AttachEpoch: "e2"},
			{Name: "unrelated", Partitions: 1, Role: topic.RoleChild, Parent: "elsewhere", AttachEpoch: "e3"},
		},
	}
	r, dataDir, _ := newMoveTestRunner(t, store, src, wantHWM)
	r.Reconcile(context.Background())
	r.wg.Wait()

	marker, ok, err := messaging.ReadMoveMarker(storage.TopicPartitionDir(dataDir, "orders", 0))
	if err != nil || !ok {
		t.Fatalf("installed partition has no move marker (ok %v, err %v)", ok, err)
	}
	if marker.Source != "narad-src" || marker.HighWatermark != wantHWM || marker.ForcePromoted {
		t.Fatalf("marker = %+v, want source narad-src, hwm %d, not force-promoted", marker, wantHWM)
	}
	if len(marker.Children) != 2 || marker.Children["audit"] != "e1" || marker.Children["other"] != "e2" {
		t.Fatalf("marker children = %v, want {audit:e1 other:e2}", marker.Children)
	}
}
