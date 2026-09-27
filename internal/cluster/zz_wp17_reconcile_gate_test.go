package cluster

// The fan-out and move reconcilers skip a tick's pass while the
// metadata they derive their work from is unchanged. These tests pin
// both halves of that: an unchanged tick does not decode the topic or
// assignment tables, and nothing the old every-tick pass picked up is
// missed (a new link or move, a pass that failed or left work pending,
// a worker that exited on its own).

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// zzWP17MaxAllocsPerTopic bounds what an unchanged tick may allocate,
// per topic in the replica: a pass that decodes the topic table (and,
// for the move runner, every assignment) spends several allocations per
// topic, a skipped tick a fixed handful regardless of the topic count.
const zzWP17MaxAllocsPerTopic = 0.5

// An unchanged tick decodes neither the topic table (fan-out) nor every
// topic's assignments (move). One store serves both runners: seeding
// and electing it is most of the test's time.
func TestZZWP17UnchangedTickSkipsTableScans(t *testing.T) {
	const n = 500
	store := zzWP17SeededStore(t, n)
	fanout, move := zzWP17Runners(t, store)
	ctx := context.Background()

	fanout.Reconcile(ctx)
	if allocs := testing.AllocsPerRun(20, func() { fanout.Reconcile(ctx) }); allocs >= zzWP17MaxAllocsPerTopic*n {
		t.Fatalf("unchanged fan-out tick allocates %.0f times with %d topics; it still decodes the topic table", allocs, n)
	}
	move.Reconcile(ctx)
	if allocs := testing.AllocsPerRun(20, func() { move.Reconcile(ctx) }); allocs >= zzWP17MaxAllocsPerTopic*n {
		t.Fatalf("unchanged move tick allocates %.0f times with %d topics; it still decodes topics and assignments", allocs, n)
	}
}

// A link attached after the runner went quiet is running after the very
// next tick.
func TestZZWP17FanoutPicksUpNewLinkWithinOneTick(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	runner := env.newRunner(t)
	defer func() { cancel(); runner.wg.Wait() }()
	for range 3 {
		runner.Reconcile(ctx)
	}
	if n := zzWP17CursorCount(runner); n != 0 {
		t.Fatalf("cursors before attach = %d, want 0", n)
	}

	if err := env.store.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	runner.Reconcile(ctx)
	if n := zzWP17CursorCount(runner); n != 3 {
		t.Fatalf("cursors one tick after attach = %d, want 3", n)
	}

	// And a detach is acted on by the next tick too.
	if err := env.store.DetachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("DetachChild: %v", err)
	}
	runner.Reconcile(ctx)
	// Cancelled cursors drain in the background; the pass must at least
	// have cancelled every one of them.
	runner.mu.Lock()
	handles := make(map[fanoutCursorKey]*fanoutCursorHandle, len(runner.cursors))
	for key, h := range runner.cursors {
		handles[key] = h
	}
	runner.mu.Unlock()
	for key, h := range handles {
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("cursor %+v not stopped one tick after detach", key)
		}
	}
}

// A pass that leaves a cancelled cursor still draining must run again
// on the next tick to reap it, although no metadata changed.
func TestZZWP17FanoutPassWithDrainingCursorRetries(t *testing.T) {
	env := newFanoutTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	runner := env.newRunner(t)
	defer func() { cancel(); runner.wg.Wait() }()

	// A cursor for a link that no longer exists, still draining.
	done := make(chan struct{})
	key := fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: "gone"}
	runner.mu.Lock()
	runner.cursors[key] = &fanoutCursorHandle{cancel: func() {}, done: done}
	runner.mu.Unlock()

	runner.Reconcile(ctx) // cancels it; it is still draining
	if n := zzWP17CursorCount(runner); n != 1 {
		t.Fatalf("cursors after the cancelling pass = %d, want 1 (still draining)", n)
	}
	close(done)
	runner.Reconcile(ctx)
	if n := zzWP17CursorCount(runner); n != 0 {
		t.Fatalf("cursors after the drained cursor's next tick = %d, want 0 (reaped)", n)
	}
}

// A move targeted at this node after the runner went quiet has a worker
// after the very next tick, against the real metastore.
func TestZZWP17MovePicksUpNewMoveWithinOneTick(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 7_200_000}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-src"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	r := NewMoveRunner(store, "node-self", t.TempDir(), movePeerFake{}, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	defer func() { cancel(); r.wg.Wait() }()
	for range 3 {
		r.Reconcile(ctx)
	}
	if n := zzWP17WorkerCount(r); n != 0 {
		t.Fatalf("workers before the move = %d, want 0", n)
	}

	if err := store.SetAssignmentTarget(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	r.Reconcile(ctx)
	if n := zzWP17WorkerCount(r); n != 1 {
		t.Fatalf("workers one tick after the move was planned = %d, want 1", n)
	}
}

// zzWP17FlakyMoveStore fails ListAssignments a set number of times and
// reports a fixed domain version, so only a retry, never a version
// change, can bring the next pass back.
type zzWP17FlakyMoveStore struct {
	*fakeMoveStore
	flakyMu  sync.Mutex
	failures int
	lists    int
}

func (s *zzWP17FlakyMoveStore) LatestDomainVersion() uint64 { return 7 }

func (s *zzWP17FlakyMoveStore) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	s.flakyMu.Lock()
	s.lists++
	s.flakyMu.Unlock()
	return s.fakeMoveStore.ListTopics(ctx, opts)
}

func (s *zzWP17FlakyMoveStore) ListAssignments(name string) ([]metastore.Assignment, error) {
	s.flakyMu.Lock()
	if s.failures > 0 {
		s.failures--
		s.flakyMu.Unlock()
		return nil, errors.New("transient")
	}
	s.flakyMu.Unlock()
	return s.fakeMoveStore.ListAssignments(name)
}

func (s *zzWP17FlakyMoveStore) listCount() int {
	s.flakyMu.Lock()
	defer s.flakyMu.Unlock()
	return s.lists
}

// A pass whose assignment read failed is retried on the next tick, and
// a pass with nothing left to do is not repeated while the version holds.
func TestZZWP17MoveFailedPassRetries(t *testing.T) {
	store := &zzWP17FlakyMoveStore{
		fakeMoveStore: &fakeMoveStore{
			// The source has no address, so the worker only waits.
			assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		},
		failures: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := NewMoveRunner(store, "narad-dst", t.TempDir(), movePeerFake{}, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	defer func() { cancel(); r.wg.Wait() }()

	r.Reconcile(ctx)
	if n := zzWP17WorkerCount(r); n != 0 {
		t.Fatalf("workers after the failed pass = %d, want 0", n)
	}
	r.Reconcile(ctx)
	if n := zzWP17WorkerCount(r); n != 1 {
		t.Fatalf("workers after the retried pass = %d, want 1", n)
	}
	lists := store.listCount()
	for range 5 {
		r.Reconcile(ctx)
	}
	if got := store.listCount(); got != lists {
		t.Fatalf("topic listings over 5 unchanged ticks = %d, want 0", got-lists)
	}
}

// A move worker that exits on its own while its move is still wanted is
// respawned by the next tick, although no metadata changed.
func TestZZWP17MoveRespawnsSelfExitedWorker(t *testing.T) {
	store := &zzWP17FlakyMoveStore{fakeMoveStore: &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
	}}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, nil, nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.wg.Wait() }()

	// A staging directory the worker cannot clear makes it exit at once.
	staging := r.stagingDir("orders", 0)
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "stuck"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(staging, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(staging, 0o755) })

	r.Reconcile(ctx)
	first := zzWP17Worker(t, r)
	<-first.done
	if err := os.Chmod(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	r.Reconcile(ctx)
	if second := zzWP17Worker(t, r); second == first {
		t.Fatal("the exited worker was not respawned on the next tick")
	}
}

func zzWP17CursorCount(r *FanoutRunner) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cursors)
}

func zzWP17WorkerCount(r *MoveRunner) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.workers)
}

func zzWP17Worker(t *testing.T, r *MoveRunner) *moveHandle {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(r.workers))
	}
	for _, h := range r.workers {
		return h
	}
	return nil
}
