package controller

// The heartbeat pass judges members on the leader's own clock, after a
// Barrier, and refuses a verdict that would leave most voters dead.

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// testClock is a settable clock for the controller.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Now()} }

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// logBuffer collects a controller's log lines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// lines returns the log lines containing every one of parts.
func (b *logBuffer) lines(parts ...string) []string {
	var out []string
	for line := range strings.SplitSeq(b.String(), "\n") {
		match := line != ""
		for _, p := range parts {
			if !strings.Contains(line, p) {
				match = false
				break
			}
		}
		if match {
			out = append(out, line)
		}
	}
	return out
}

func newLogBuffer() (*slog.Logger, *logBuffer) {
	b := &logBuffer{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelInfo})), b
}

// heardFrom is an alive member last heard from ago before clk's now.
func heardFrom(id string, clk *testClock, ago time.Duration) metastore.Member {
	return metastore.Member{ID: id, Addr: id + ":7942", Status: metastore.MemberAlive, LastHeartbeat: clk.now().Add(-ago).Unix()}
}

// A new leader inherits stamps no leader wrote during the leaderless
// stretch before its election. Members it has not heard from yet get one
// full DeadTimeout on its own clock before any is marked dead.
func TestNewLeaderMarksNoMemberDeadWithinItsElectionGrace(t *testing.T) {
	clk := newTestClock()
	store := newFakeControllerStore()
	store.members = []metastore.Member{
		heardFrom("a", clk, 0), heardFrom("b", clk, 60*time.Second), heardFrom("c", clk, 60*time.Second),
		heardFrom("d", clk, 0), heardFrom("e", clk, 0),
	}
	store.leaderID = "a"
	c := &Controller{store: store, now: clk.now, cfg: Config{DeadTimeout: 30 * time.Second}.withDefaults()}
	ctx := withTerm(context.Background(), newLeaderTerm(clk.now()))

	for range 3 {
		c.checkHeartbeats(ctx)
		if len(store.markedDead) != 0 {
			t.Fatalf("marked %v dead within the first dead timeout of the term; their stamps predate the election", store.markedDead)
		}
		clk.advance(9 * time.Second)
	}
	clk.advance(4 * time.Second)       // 31 s into the term
	for _, i := range []int{0, 3, 4} { // a, d and e kept heartbeating
		store.members[i].LastHeartbeat = clk.now().Unix()
	}
	c.checkHeartbeats(ctx)
	if !slices.Equal(store.markedDead, []string{"b", "c"}) {
		t.Fatalf("marked dead after the grace = %v, want [b c] (silent for the whole term)", store.markedDead)
	}
}

// A freshly elected leader's FSM may still be applying heartbeats earlier
// leaders committed. The pass Barriers first and judges what it reads
// after the Barrier.
func TestHeartbeatPassWaitsForABarrierBeforeJudging(t *testing.T) {
	store := newFakeControllerStore()
	stale := time.Now().Add(-time.Hour).Unix()
	store.members = []metastore.Member{
		{ID: "a", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "b", Status: metastore.MemberAlive, LastHeartbeat: stale},
		{ID: "c", Status: metastore.MemberAlive, LastHeartbeat: stale},
		{ID: "d", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "e", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
	}
	store.leaderID = "a"
	store.onBarrier = func(f *fakeControllerStore) { // the replay lands
		for i := range f.members {
			f.members[i].LastHeartbeat = time.Now().Unix()
		}
	}
	c := &Controller{store: store, cfg: Config{DeadTimeout: 30 * time.Second}.withDefaults()}

	c.checkHeartbeats(context.Background())

	if store.barriers == 0 {
		t.Fatal("the heartbeat pass judged members without a Barrier")
	}
	if len(store.markedDead) != 0 {
		t.Fatalf("marked %v dead from stamps read before the Barrier", store.markedDead)
	}
}

// Three voters, two of them silent: marking both would leave one alive
// voter, which a leader holding its lease rules out. The breaker refuses
// the whole verdict for voters, says so once at error, and exports it.
func TestDeadMarkingRefusesAVerdictThatLeavesNoVoterMajority(t *testing.T) {
	store := newFakeControllerStore()
	stale := time.Now().Add(-time.Hour).Unix()
	store.members = []metastore.Member{
		{ID: "a", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "b", Status: metastore.MemberAlive, LastHeartbeat: stale},
		{ID: "c", Status: metastore.MemberAlive, LastHeartbeat: stale},
		{ID: "joiner", Status: metastore.MemberAlive, LastHeartbeat: stale}, // not a voter
	}
	store.voters = []string{"a", "b", "c"}
	store.leaderID = "a"
	log, logs := newLogBuffer()
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{DeadTimeout: 30 * time.Second, Logger: log}.withDefaults(), m: newMetrics(reg)}

	for range 3 {
		c.checkHeartbeats(context.Background())
	}

	if !slices.Equal(store.markedDead, []string{"joiner"}) {
		t.Fatalf("marked dead = %v, want only the non-voter [joiner]; the voters' verdict must be refused", store.markedDead)
	}
	if got := testutil.ToFloat64(c.m.deadMarkingRefused); got != 1 {
		t.Fatalf("narad_dead_marking_refused = %v, want 1", got)
	}
	refusals := logs.lines("level=ERROR", "refusing to mark voters dead")
	if len(refusals) != 1 {
		t.Fatalf("refusal logged %d times at error over 3 passes, want once:\n%s", len(refusals), logs)
	}
	if !strings.Contains(refusals[0], `refused="[b c]"`) {
		t.Fatalf("refusal line does not name the refused voters b and c: %s", refusals[0])
	}

	// The voters heartbeat again: the next pass is normal and clears it.
	for i := range store.members {
		store.members[i].LastHeartbeat = time.Now().Unix()
	}
	c.checkHeartbeats(context.Background())
	if got := testutil.ToFloat64(c.m.deadMarkingRefused); got != 0 {
		t.Fatalf("narad_dead_marking_refused = %v after a normal pass, want 0", got)
	}
}

// Five voters with two silent leaves three alive, a quorum: the breaker
// lets the verdict through and both are marked.
func TestDeadMarkingStillMarksAMinority(t *testing.T) {
	store := newFakeControllerStore()
	stale := time.Now().Add(-time.Hour).Unix()
	store.members = []metastore.Member{
		{ID: "a", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "b", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "c", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "d", Status: metastore.MemberAlive, LastHeartbeat: stale},
		{ID: "e", Status: metastore.MemberAlive, LastHeartbeat: stale},
	}
	store.leaderID = "a"
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{DeadTimeout: 30 * time.Second}.withDefaults(), m: newMetrics(reg)}

	c.checkHeartbeats(context.Background())

	if !slices.Equal(store.markedDead, []string{"d", "e"}) {
		t.Fatalf("marked dead = %v, want [d e]", store.markedDead)
	}
	if got := testutil.ToFloat64(c.m.deadMarkingRefused); got != 0 {
		t.Fatalf("narad_dead_marking_refused = %v, want 0", got)
	}
}

// The controller says what it does: leadership gained and lost, a member
// marked dead, a member alive again.
func TestControllerLogsLeadershipAndMemberTransitions(t *testing.T) {
	clk := newTestClock()
	store := &lockedStore{f: newFakeControllerStore()}
	store.f.members = []metastore.Member{
		heardFrom("a", clk, 0), heardFrom("b", clk, 0), heardFrom("c", clk, 0),
		heardFrom("d", clk, 0), heardFrom("e", clk, time.Hour),
	}
	store.f.leaderID = "a"
	log, logs := newLogBuffer()
	c := &Controller{store: store, now: clk.now, cfg: Config{DeadTimeout: 30 * time.Second, ReconcileInterval: time.Hour, Logger: log}.withDefaults()}

	ctx, cancel := context.WithCancel(context.Background())
	stop := c.startLeaderLoop(ctx)
	waitFor(t, func() bool { return len(logs.lines("leadership gained")) == 1 })
	// The loop's own first pass runs on its goroutine. Stop the loop and
	// wait for it to end before driving passes directly, so no pass of
	// the loop can judge e alongside them.
	stop()
	cancel()
	waitFor(t, func() bool { return len(logs.lines("leader loop stopped")) == 1 })

	// Past the grace, a pass marks e dead; e then comes back.
	clk.advance(31 * time.Second)
	store.with(func(f *fakeControllerStore) {
		for i := range 4 { // a to d kept heartbeating
			f.members[i].LastHeartbeat = clk.now().Unix()
		}
	})
	cctx := withTerm(context.Background(), newLeaderTerm(time.Time{}))
	c.checkHeartbeats(cctx)
	if len(logs.lines("level=WARN", "member marked dead", "member=e")) != 1 {
		t.Fatalf("no warn line for e marked dead:\n%s", logs)
	}
	store.with(func(f *fakeControllerStore) {
		f.members[4].Status = metastore.MemberAlive
		f.members[4].LastHeartbeat = clk.now().Unix()
	})
	c.checkHeartbeats(cctx)
	if len(logs.lines("member alive again", "member=e")) != 1 {
		t.Fatalf("no line for e alive again:\n%s", logs)
	}
}

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// lockedStore serialises every call into a fakeControllerStore, so a
// test can run the real leader loop on its own goroutine and still read
// the fake.
type lockedStore struct {
	mu sync.Mutex
	f  *fakeControllerStore
}

func (s *lockedStore) with(fn func(f *fakeControllerStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.f)
}

func (s *lockedStore) IsLeader() bool        { return true }
func (s *lockedStore) LeaderCh() <-chan bool { return nil }

func (s *lockedStore) Barrier() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Barrier()
}

func (s *lockedStore) ListMembers() ([]metastore.Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms, err := s.f.ListMembers()
	return slices.Clone(ms), err
}

func (s *lockedStore) RoutingMembersVersion() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.RoutingMembersVersion()
}

func (s *lockedStore) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, next, err := s.f.ListTopics(ctx, opts)
	return slices.Clone(ts), next, err
}

func (s *lockedStore) ListAssignments(topicName string) ([]metastore.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.ListAssignments(topicName)
}

func (s *lockedStore) LockAssignments() func() { return func() {} }

func (s *lockedStore) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.AssignPartition(ctx, topicName, partition, ownerID)
}

func (s *lockedStore) SetAssignmentTarget(ctx context.Context, topicName string, partition int, targetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.SetAssignmentTarget(ctx, topicName, partition, targetID)
}

func (s *lockedStore) AbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.AbortMove(ctx, topicName, partition, expectedTarget)
}

func (s *lockedStore) MarkMemberDead(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.MarkMemberDead(ctx, id)
}

func (s *lockedStore) Voters() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.f.Voters()
	return slices.Clone(v), err
}

func (s *lockedStore) Nonvoters() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.f.Nonvoters()
	return slices.Clone(v), err
}

func (s *lockedStore) RemoveServer(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.RemoveServer(id)
}

func (s *lockedStore) RemoveMember(ctx context.Context, id string, at int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.RemoveMember(ctx, id, at)
}

func (s *lockedStore) TransferLeadership() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.TransferLeadership()
}

func (s *lockedStore) LeaderID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.LeaderID()
}
