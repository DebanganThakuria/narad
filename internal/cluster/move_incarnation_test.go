package cluster

// Moves and the periodic sweep are the two paths that touch topic
// directories without an open, so they carry their own incarnation
// checks: a copy of another incarnation is never installed, a local
// directory of a deleted incarnation is set aside (after the leader
// confirms) rather than mistaken for a stale copy, and a quarantined
// directory is reclaimed only once the leader confirms its incarnation
// is gone.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// keepingReclaimer is a fakeReclaimer that also prepares topic
// directories through a real runtime.Logs, like the engine does.
type keepingReclaimer struct {
	fakeReclaimer
	logs *runtime.Logs
}

func (k *keepingReclaimer) EnsureTopicIncarnation(topicName, id string) error {
	return k.logs.EnsureTopicIncarnation(topicName, id)
}

func newKeepingReclaimer(t *testing.T, dataDir string) *keepingReclaimer {
	t.Helper()
	logs := runtime.NewLogs(dataDir, storage.Options{}, nil, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	return &keepingReclaimer{logs: logs}
}

// A source that reports its copy as belonging to another incarnation of
// the topic (the source never purged the deleted topic's partition, or
// this replica is behind a recreate) is refused: no install, no flip.
func TestMoveRunnerRefusesCopyOfAnotherIncarnation(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 8)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:     []topic.Topic{{Name: "orders", ID: "1111111111111111", Partitions: 1}},
	}
	peer := movePeerFake{
		dirFetcher:  dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		incarnation: "0000000000000000",
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if len(store.completeArgs) != 0 {
		t.Fatalf("flip proposed for a copy of another incarnation: %v", store.completeArgs)
	}
	if _, err := os.Stat(topicPartitionDirT(t, dataDir, "orders", 0)); !os.IsNotExist(err) {
		t.Fatalf("partition of another incarnation was installed (stat err %v)", err)
	}
}

// A matching incarnation installs, and the install stamps the topic
// directory with it (a copy landing on a node that never opened the
// topic must leave a marked directory behind).
func TestMoveRunnerInstallStampsIncarnation(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 8)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:     []topic.Topic{{Name: "orders", ID: "1111111111111111", Partitions: 1}},
	}
	peer := movePeerFake{
		dirFetcher:  dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		incarnation: "1111111111111111",
	}
	dataDir := t.TempDir()
	// A deleted incarnation's directory is already there: the install
	// must set it aside, not drop the copy into it.
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "orders"), "0000000000000000"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})

	r.Reconcile(context.Background())
	r.wg.Wait()

	if len(store.completeArgs) != 3 {
		t.Fatalf("flip not proposed: %v", store.completeArgs)
	}
	id, ok, err := storage.ReadTopicIncarnation(topicDirT(t, dataDir, "orders"))
	if err != nil || !ok || id != "1111111111111111" {
		t.Fatalf("marker after install = (%q, %v, %v), want 1111111111111111", id, ok, err)
	}
	// The old incarnation's directory was set aside (and, this node
	// being its own leader, its quarantine reclaimed in the same pass).
	log, err := storage.NewLog(topicPartitionDirT(t, dataDir, "orders", 0), storage.Options{})
	if err != nil {
		t.Fatalf("recover installed partition: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("installed NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
}

// The sweep finds topics/orders carrying a marker for an incarnation
// other than the live topic's. It sets the directory aside only after
// the leader confirms the live incarnation, and never treats its
// partitions as stale copies to reclaim.
func TestMoveSweepSetsAsideDirOfDeletedIncarnation(t *testing.T) {
	live := topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}
	newStore := func(leaderID string) *fakeMoveStore {
		return &fakeMoveStore{
			assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-other"},
			member:     metastore.Member{ID: "narad-other", Addr: "otheraddr", Status: metastore.MemberAlive},
			topics:     []topic.Topic{live},
			leaderID:   leaderID,
		}
	}
	prepare := func(t *testing.T) string {
		dataDir := t.TempDir()
		mkLocalPartitionDir(t, dataDir)
		if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "orders"), "1111111111111111"); err != nil {
			t.Fatalf("WriteTopicIncarnation: %v", err)
		}
		return dataDir
	}

	t.Run("leader unreachable keeps the dir in place", func(t *testing.T) {
		dataDir := prepare(t)
		rec := newKeepingReclaimer(t, dataDir)
		store := newStore("narad-ldr")
		store.member = metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}
		r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, rec, nil, nil, MoveConfig{})
		r.sweepStaleCopies(context.Background())
		if rec.count() != 0 {
			t.Fatalf("reclaim called %d times for a directory of another incarnation", rec.count())
		}
		if _, err := os.Stat(topicPartitionDirT(t, dataDir, "orders", 0)); err != nil {
			t.Fatalf("directory moved without leader confirmation: %v", err)
		}
	})

	t.Run("leader confirms the live incarnation: dir set aside, no partition reclaim", func(t *testing.T) {
		dataDir := prepare(t)
		rec := newKeepingReclaimer(t, dataDir)
		store := newStore("narad-ldr")
		store.member = metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}
		r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{leaderTopic: &live}, rec, nil, nil, MoveConfig{})
		r.sweepStaleCopies(context.Background())
		if rec.count() != 0 {
			t.Fatalf("reclaim called %d times for a directory of another incarnation", rec.count())
		}
		// The old partition is no longer under the live topic's directory
		// (quarantined; the same pass then reclaimed the quarantine, the
		// leader having confirmed the incarnation is gone).
		if _, err := os.Stat(filepath.Join(topicDirT(t, dataDir, "orders"), "p00000")); !os.IsNotExist(err) {
			t.Fatalf("deleted incarnation's partition still under the live topic dir (stat err %v)", err)
		}
		if _, err := os.Stat(staleTopicDirT(t, dataDir, "orders", "1111111111111111")); !os.IsNotExist(err) {
			t.Fatalf("quarantine not reclaimed after leader confirmation (stat err %v)", err)
		}
		id, ok, _ := storage.ReadTopicIncarnation(topicDirT(t, dataDir, "orders"))
		if !ok || id != live.ID {
			t.Fatalf("marker after set-aside = (%q, %v), want the live incarnation", id, ok)
		}
	})
}

// A quarantined directory is reclaimed by the periodic sweep only after
// the leader confirms its incarnation is gone; it is kept while the
// leader is unreachable or still reports that incarnation live.
func TestMoveSweepReclaimsQuarantineAfterLeaderConfirms(t *testing.T) {
	cases := []struct {
		name     string
		peer     movePeerFake
		wantGone bool
	}{
		{"leader unreachable keeps it", movePeerFake{}, false},
		{"leader still has that incarnation keeps it", movePeerFake{leaderTopic: &topic.Topic{Name: "orders", ID: "1111111111111111"}}, false},
		{"leader has a newer incarnation reclaims it", movePeerFake{leaderTopic: &topic.Topic{Name: "orders", ID: "2222222222222222"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			stale := staleTopicDirT(t, dataDir, "orders", "1111111111111111")
			if err := storage.WriteTopicIncarnation(stale, "1111111111111111"); err != nil {
				t.Fatalf("WriteTopicIncarnation: %v", err)
			}
			store := &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-dst"},
				member:     metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive},
				topics:     []topic.Topic{{Name: "orders", ID: "2222222222222222", Partitions: 1}},
				leaderID:   "narad-ldr",
			}
			r := NewMoveRunner(store, "narad-dst", dataDir, tc.peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{})
			r.sweepStaleCopies(context.Background())
			_, err := os.Stat(stale)
			if tc.wantGone && !os.IsNotExist(err) {
				t.Fatalf("quarantined dir still present (stat err %v), want reclaimed", err)
			}
			if !tc.wantGone && err != nil {
				t.Fatalf("quarantined dir reclaimed without leader confirmation: %v", err)
			}
		})
	}
}

// Incarnation ids of a topic a move was copying and the same-named topic
// created while the move ran.
const (
	movedIncarnation     = "1111111111111111"
	recreatedIncarnation = "2222222222222222"
)

// logsDest is a move destination whose directory handling is production
// code: a real runtime.Logs installs through ReplacePartitionDir and
// prepares the incarnation through EnsureTopicIncarnation, as
// *messaging.Engine does. onReset runs at each consumer-state reset (n
// counts them) and onEnsure once, before the first prepare takes the
// topic's guard.
type logsDest struct {
	logs     *runtime.Logs
	resets   int
	onReset  func(n int)
	onEnsure func()
}

func (d *logsDest) ReclaimMovedPartition(context.Context, string, int) error { return nil }

func (d *logsDest) ResetPartitionConsumerState(string, int) {
	d.resets++
	if d.onReset != nil {
		d.onReset(d.resets)
	}
}

func (d *logsDest) InstallPartitionDir(topicName string, partition int, swap func() error) error {
	return d.logs.ReplacePartitionDir(topicName, partition, swap)
}

func (d *logsDest) EnsureTopicIncarnation(topicName, id string) error {
	if fn := d.onEnsure; fn != nil {
		d.onEnsure = nil
		fn()
	}
	return d.logs.EnsureTopicIncarnation(topicName, id)
}

// recreateAndServe deletes and recreates orders on store and produces 7
// committed records to the successor's partition on this node through
// logs, returning the successor's hwm.
func recreateAndServe(ctx context.Context, store *metastore.Store, logs *runtime.Logs) (int64, error) {
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		return 0, err
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: recreatedIncarnation, Partitions: 1}); err != nil {
		return 0, err
	}
	l, err := logs.Get("orders", 0)
	if err != nil {
		return 0, err
	}
	for range 7 {
		if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte("successor"))); err != nil {
			return 0, err
		}
	}
	if err := l.CommitDurable(0, 6); err != nil {
		return 0, err
	}
	return l.HighWatermark(), nil
}

// A move installs after it prepared the topic directory for the copy's
// incarnation (finishMove's EnsureTopicIncarnation), and that check
// releases the topic's guard. A delete and recreate of the name before
// the install, with this node serving the successor's partition, must
// not lose the successor's directory to the install's swap: the swap
// runs under the topic's guard with the partition's log closed, and only
// while the topic marker still names the move's incarnation. The seam is
// the reset right before the install, which runs the delete, the
// recreate and the successor's first produce.
func TestMoveInstallLeavesASuccessorOpenedBeforeTheInstall(t *testing.T) {
	ctx := context.Background()
	real := newTestStore(t)
	if err := real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: movedIncarnation, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 10)
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, real, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })

	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:      []topic.Topic{{Name: "orders", ID: movedIncarnation, Partitions: 1}},
		completeErr: errors.New("flip rejected: the topic was deleted"),
	}
	moveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var successorHWM int64
	var hookErr error
	dest := &logsDest{logs: logs, onReset: func(n int) {
		if n != 1 {
			return
		}
		// The runner's replica sees the recreated record too; one
		// attempt only.
		defer cancel()
		store.topics = []topic.Topic{{Name: "orders", ID: recreatedIncarnation, Partitions: 1}}
		successorHWM, hookErr = recreateAndServe(ctx, real, logs)
	}}
	store.completeHook = cancel
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm, committed: 5, hasCommitted: true}, incarnation: movedIncarnation}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, dest, nil, nil, MoveConfig{})
	r.Reconcile(moveCtx)
	r.wg.Wait()
	if hookErr != nil || successorHWM != 7 {
		t.Fatalf("setup: the successor's produce: hwm %d, err %v", successorHWM, hookErr)
	}

	if err := logs.CloseAll(); err != nil {
		t.Logf("close: %v", err)
	}
	id, marked, _ := storage.ReadTopicIncarnation(topicDirT(t, dataDir, "orders"))
	l, err := storage.NewLog(topicPartitionDirT(t, dataDir, "orders", 0), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := l.NextOffset(); got != 7 {
		t.Fatalf("the successor's partition directory recovers next offset %d after the move, want 7 (its 7 committed records); topic marker %q (marked %v); flip args %v",
			got, id, marked, store.completeArgs)
	}
}

// finishMove reads the topic record outside the topic's guard and then
// prepares the directory for that record's incarnation. A delete and
// recreate in between, with this node opening the successor's partition
// (which stamps the successor's marker), must not make the prepare treat
// the live successor's directory as a deleted incarnation's leftover:
// the prepare re-reads the local record under the guard and refuses an
// id the record no longer carries, and the move retries.
//
// The only seam: the destination's prepare runs the delete, the
// recreate and the successor's first produce before it takes the guard,
// standing for the worker being descheduled between finishMove's read
// and its prepare. Everything else is production code: a real
// runtime.Logs over a real Raft metastore.
func TestMoveNeverQuarantinesASuccessorRecreatedBeforeItsInstall(t *testing.T) {
	ctx := context.Background()
	real := newTestStore(t)
	if err := real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: movedIncarnation, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 10)
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, real, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })

	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:      []topic.Topic{{Name: "orders", ID: movedIncarnation, Partitions: 1}},
		completeErr: errors.New("flip rejected: the topic was deleted"),
	}
	moveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	store.completeHook = cancel // one attempt

	var successorHWM int64
	var hookErr error
	dest := &logsDest{logs: logs, onEnsure: func() {
		successorHWM, hookErr = recreateAndServe(ctx, real, logs)
		// A refused prepare retries until the worker is cancelled.
		time.AfterFunc(time.Second, cancel)
	}}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm, committed: 5, hasCommitted: true}, incarnation: movedIncarnation}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, dest, nil, nil, MoveConfig{RetryBackoff: 50 * time.Millisecond})
	r.Reconcile(moveCtx)
	r.wg.Wait()
	if hookErr != nil || successorHWM != 7 {
		t.Fatalf("setup: the successor's produce: hwm %d, err %v", successorHWM, hookErr)
	}

	// The successor serves its partition again: its 7 committed records
	// must still be there.
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := l.NextOffset(); got != 7 {
		t.Fatalf("the successor's partition reopens at next offset %d after the move, want 7 (its records were set aside under %s); flip args %v",
			got, staleTopicDirT(t, dataDir, "orders", recreatedIncarnation), store.completeArgs)
	}
	if len(store.completeArgs) != 0 {
		t.Fatalf("the move flipped a copy of the deleted incarnation into the successor's name: %v", store.completeArgs)
	}
}
