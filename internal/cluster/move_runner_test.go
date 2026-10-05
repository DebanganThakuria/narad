package cluster

// The move runner is the destination-side reconcile loop. These tests
// drive one full move to completion (copy → install → guarded flip) and
// the abort path (a rejected flip rolls the install back and clears the
// target), against a fake store and a source served off a real partition
// directory — the same bytes the RPC serve side would stream.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
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
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
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
	assignErr    error         // returned by GetAssignment when set
}

func (s *fakeMoveStore) AppliedCaughtUp() bool { return true }
func (s *fakeMoveStore) Barrier() error        { return nil }
func (s *fakeMoveStore) GetAssignment(string, int) (metastore.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.assignErr != nil {
		return metastore.Assignment{}, s.assignErr
	}
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

// The old owner deletes its copy once the flip is visible and the new
// owner's listing covers it, and that listing reads sizes from the page
// cache. So before the flip the destination must have synced every file
// of the copy, the staging directory, and the topic directory it renamed
// the copy into, and the marker must say so.
func TestMoveRunnerMakesTheCopyDurableBeforeTheFlip(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 10)
	dataDir := t.TempDir()
	var (
		mu     sync.Mutex
		synced = map[string]bool{}
		atFlip map[string]bool
	)
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSync || op == syncfile.OpSyncData {
			mu.Lock()
			synced[filepath.Clean(path)] = true
			mu.Unlock()
		}
		return nil
	})
	defer restore()
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	store.completeHook = func() {
		mu.Lock()
		atFlip = maps.Clone(synced)
		mu.Unlock()
	}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true}}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()
	if atFlip == nil {
		t.Fatal("setup: the flip was never proposed")
	}

	staging := filepath.Clean(r.stagingDir("orders", 0))
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, e := range entries {
		if !e.Type().IsRegular() || e.Name() == messaging.MoveMarkerFileName {
			continue // the marker is synced under its temporary name
		}
		files++
		if !atFlip[filepath.Join(staging, e.Name())] {
			t.Errorf("staged %s was not synced before the flip", e.Name())
		}
	}
	if files < 10 {
		t.Fatalf("setup: the installed copy holds %d files, want every segment and the position files", files)
	}
	for what, d := range map[string]string{"the staging directory": staging, "the topic directory": filepath.Dir(dir)} {
		if !atFlip[filepath.Clean(d)] {
			t.Errorf("%s was not synced before the flip", what)
		}
	}
	marker, ok, err := messaging.ReadMoveMarker(dir)
	if err != nil || !ok || marker.DurableAtUnixMs == 0 {
		t.Fatalf("the move marker does not record the copy as durable: %+v (found %v, err %v)", marker, ok, err)
	}
}

// switchableSourceStore reports the source member alive or dead as the
// test switches it; dead, its last heartbeat stamp is ten minutes old,
// past any ForcePromoteAfter. reads counts the source lookups.
type switchableSourceStore struct {
	*fakeMoveStore
	dead  atomic.Bool
	reads atomic.Int64
}

func (s *switchableSourceStore) GetMember(id string) (metastore.Member, error) {
	m, err := s.fakeMoveStore.GetMember(id)
	if id != "narad-src" {
		return m, err
	}
	s.reads.Add(1)
	if s.dead.Load() {
		m.Status = metastore.MemberDead
		m.LastHeartbeat = time.Now().Add(-10 * time.Minute).Unix()
	}
	return m, err
}

// awaitReads waits until the worker has read the source member n more
// times, or has flipped the move.
func (s *switchableSourceStore) awaitReads(t *testing.T, n int64) {
	t.Helper()
	target := s.reads.Load() + n
	for deadline := time.Now().Add(5 * time.Second); s.reads.Load() < target && !s.flipped(); {
		if time.Now().After(deadline) {
			t.Fatalf("the worker stopped reading the source member (%d reads, want %d)", s.reads.Load(), target)
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *switchableSourceStore) flipped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.completeArgs) > 0
}

// prepareCountingPeer counts handoff freezes; with prepareErr set they
// all fail, so a live source never cuts over.
type prepareCountingPeer struct {
	movePeerFake
	prepares *atomic.Int64
}

func (p prepareCountingPeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	p.prepares.Add(1)
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

// newDeadSourceScenario starts a worker on a fake clock whose copy is
// complete and whose source refuses the handoff freeze, so the move can
// only finish by a force-promote.
func newDeadSourceScenario(t *testing.T) (*switchableSourceStore, *testClock, *MoveRunner, string, int64, map[int64][]byte, context.CancelFunc) {
	t.Helper()
	return startDeadSourceScenario(t, 0, discardLogger())
}

// startDeadSourceScenario is newDeadSourceScenario with the source
// reporting a high watermark behind records past what it serves, so the
// copy is that many records behind it, and the runner logging to logger.
func startDeadSourceScenario(t *testing.T, behind int64, logger *slog.Logger) (*switchableSourceStore, *testClock, *MoveRunner, string, int64, map[int64][]byte, context.CancelFunc) {
	t.Helper()
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 8)
	store := &switchableSourceStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}}
	var prepares atomic.Int64
	peer := prepareCountingPeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: wantHWM + behind, committed: 5, hasCommitted: true}, prepareErr: context.DeadlineExceeded},
		prepares:     &prepares,
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, logger, MoveConfig{
		RetryBackoff: 5 * time.Millisecond, ForcePromoteAfter: 2 * time.Minute,
	})
	clock := newTestClock()
	r.now = clock.Now
	ctx, cancel := context.WithCancel(context.Background())
	r.Reconcile(ctx)
	for deadline := time.Now().Add(5 * time.Second); prepares.Load() == 0; {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the worker never finished its copy")
		}
		time.Sleep(time.Millisecond)
	}
	return store, clock, r, dataDir, wantHWM, payloads, func() { cancel(); r.wg.Wait() }
}

// Force-promote needs the destination itself to have watched the source
// stay dead for ForcePromoteAfter, on its own monotonic clock. The
// leader's heartbeat stamp alone compares this node's wall clock with a
// stamp another node wrote, possibly across a leaderless period: a
// worker that had only just seen the source dead read a ten-minute-old
// stamp and promoted its copy at once, giving up whatever the source
// still held.
func TestForcePromoteWaitsForTheDestinationToSeeTheSourceDead(t *testing.T) {
	store, clock, _, dataDir, wantHWM, payloads, stop := newDeadSourceScenario(t)
	defer stop()
	store.dead.Store(true)
	store.awaitReads(t, 3)
	if store.flipped() {
		t.Fatal("force-promoted the moment the destination saw the source dead (the leader's stamp was ten minutes old)")
	}
	clock.Advance(119 * time.Second)
	store.awaitReads(t, 3)
	if store.flipped() {
		t.Fatal("force-promoted before the destination watched the source dead for ForcePromoteAfter")
	}
	clock.Advance(2 * time.Second)
	for deadline := time.Now().Add(5 * time.Second); !store.flipped(); {
		if time.Now().After(deadline) {
			t.Fatal("no force-promote after the destination watched the source dead for ForcePromoteAfter")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	requireInstalledCopy(t, dataDir, wantHWM, payloads)
}

// A source seen alive again restarts the destination's own clock: a
// source that died, came back and died again has been dead only since
// the second death.
func TestForcePromoteClockRestartsWhenTheSourceComesBack(t *testing.T) {
	store, clock, _, _, _, _, stop := newDeadSourceScenario(t)
	defer stop()
	store.dead.Store(true)
	store.awaitReads(t, 3)
	clock.Advance(90 * time.Second)
	store.awaitReads(t, 3)
	store.dead.Store(false)
	store.awaitReads(t, 3)
	store.dead.Store(true)
	store.awaitReads(t, 3)
	clock.Advance(90 * time.Second)
	store.awaitReads(t, 3)
	if store.flipped() {
		t.Fatal("force-promoted 90s after the source died again: the clock counted the first death too")
	}
	clock.Advance(31 * time.Second)
	for deadline := time.Now().Add(5 * time.Second); !store.flipped(); {
		if time.Now().After(deadline) {
			t.Fatal("no force-promote once the source stayed dead for ForcePromoteAfter after it came back")
		}
		time.Sleep(time.Millisecond)
	}
}

// unverifiablePeer serves a source whose listings report a high
// watermark past the records it serves, so every staged copy recovers
// short of it and fails verification. It counts handoff freezes and the
// chunk fetches that start a segment at its first byte.
type unverifiablePeer struct {
	movePeerFake
	prepares   *atomic.Int64
	freshReads *atomic.Int64
}

func (p unverifiablePeer) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, ttl time.Duration, token string) (messaging.PartitionTransferInfo, error) {
	p.prepares.Add(1)
	return p.movePeerFake.PrepareHandoff(ctx, addr, topicName, partition, ttl, token)
}

func (p unverifiablePeer) FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, base, at, length int64) ([]byte, error) {
	if at == 0 {
		p.freshReads.Add(1)
	}
	return p.movePeerFake.FetchSegmentChunk(ctx, addr, topicName, partition, base, at, length)
}

// nonEmptySegments counts a partition directory's segments that hold
// bytes.
func nonEmptySegments(dir string) int64 {
	var n int64
	for _, s := range mustSegs(dir) {
		if s.SizeBytes > 0 {
			n++
		}
	}
	return n
}

const unverifiableCopyLog = "move: staged copy cannot be verified; not freezing the source again"

// A staged copy that fails verification is copied once more from
// scratch, and when that copy fails too, the worker stops: it froze the
// source's partition for produce and consume every RetryBackoff,
// forever, for a copy that could never pass. It logs once at error and
// freezes the source no more.
func TestMoveStopsFreezingTheSourceWhenTheCopyCannotBeVerified(t *testing.T) {
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 6)
	segments := nonEmptySegments(src)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	var prepares, freshReads atomic.Int64
	peer := unverifiablePeer{
		movePeerFake: movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm + 3}, freeze: &fakeFreeze{}},
		prepares:     &prepares,
		freshReads:   &freshReads,
	}
	logs := &recordedLog{}
	r := NewMoveRunner(store, "narad-dst", t.TempDir(), peer, nil, nil, slog.New(logs), MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.wg.Wait() }()
	r.Reconcile(ctx)

	for deadline := time.Now().Add(5 * time.Second); logs.count(slog.LevelError, unverifiableCopyLog) == 0 && prepares.Load() < 10; {
		if time.Now().After(deadline) {
			t.Fatalf("the worker neither gave up nor kept freezing within 5s (%d freezes)", prepares.Load())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // forty more RetryBackoffs
	if got := prepares.Load(); got != 2 {
		t.Fatalf("the source was frozen %d times; want 2 (the first copy and one fresh re-copy), then never again", got)
	}
	if got := freshReads.Load(); got != 2*segments {
		t.Fatalf("fetched %d segments from their first byte, want %d: the %d segments once, then once more from scratch", got, 2*segments, segments)
	}
	if got := logs.count(slog.LevelError, unverifiableCopyLog); got != 1 {
		t.Fatalf("logged %q %d times at error, want once", unverifiableCopyLog, got)
	}
	store.mu.Lock()
	flipped := store.completeArgs != nil
	store.mu.Unlock()
	if flipped {
		t.Fatal("flipped a copy that failed verification")
	}
}

// A sealed segment with one damaged frame is a partition the source
// serves (recovery does not prove sealed segments at open; the damaged
// record reads as recorded loss). Its move must finish, with the
// damaged bytes copied as they are, and never hold the source frozen
// while a verification of sealed segments refetches them forever.
func TestMoveCompletesForAPartitionWithToleratedSealedDamage(t *testing.T) {
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 6)
	segs := mustSegs(src)
	if len(segs) < 4 || !segs[2].Sealed || segs[2].SizeBytes == 0 {
		t.Fatalf("setup: want a sealed non-empty third segment, got %+v", segs)
	}
	damaged := filepath.Join(src, fmt.Sprintf("%020d.log", segs[2].BaseOffset))
	raw, err := os.ReadFile(damaged)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(damaged, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if n := nextOffsetAt(t, src); n != hwm {
		t.Fatalf("setup: the damaged source recovers next offset %d, want %d", n, hwm)
	}

	freeze := &fakeFreeze{}
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm}, freeze: freeze}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, nil, nil, discardLogger(), MoveConfig{
		RetryBackoff: 10 * time.Millisecond, FreezeTTL: 400 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()
	if ctx.Err() != nil {
		freeze.mu.Lock()
		minted, rearms := freeze.minted, freeze.rearms
		freeze.mu.Unlock()
		t.Fatalf("the move did not finish within 5s (%d freezes, %d re-arms)", minted, rearms)
	}
	store.mu.Lock()
	flipped := store.completeArgs != nil
	store.mu.Unlock()
	if !flipped {
		t.Fatal("the worker ended without flipping the move")
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	for _, seg := range segs {
		name := fmt.Sprintf("%020d.log", seg.BaseOffset)
		want, err := os.ReadFile(filepath.Join(src, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("installed copy lacks segment %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("installed segment %s differs from the source's (%d bytes, want %d)", name, len(got), len(want))
		}
	}
}
