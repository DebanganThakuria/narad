package cluster

// The stale-copy sweep may only fire when the local view AND the leader
// agree the partition lives elsewhere. These tests pin the reclaim call
// and every refusal gate; the engine-side guards have their own tests in
// the messaging package.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

type fakeReclaimer struct {
	mu     sync.Mutex
	calls  []string
	guards []messaging.ReclaimGuard
	err    error
}

func (f *fakeReclaimer) ReclaimMovedPartition(_ context.Context, topicName string, partition int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, topicName)
	return f.err
}

func (f *fakeReclaimer) ReclaimMovedPartitionGuarded(ctx context.Context, topicName string, partition int, guard messaging.ReclaimGuard) error {
	f.mu.Lock()
	f.guards = append(f.guards, guard)
	f.mu.Unlock()
	return f.ReclaimMovedPartition(ctx, topicName, partition)
}

func (f *fakeReclaimer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func sweepTestRunner(t *testing.T, store *fakeMoveStore, rec *fakeReclaimer) (*MoveRunner, string) {
	t.Helper()
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, rec, nil, nil, MoveConfig{})
	return r, dataDir
}

func mkLocalPartitionDir(t *testing.T, dataDir string) string {
	t.Helper()
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return dir
}

// A local dir for a partition owned elsewhere, with the (self-)leader
// confirming, is reclaimed on the first sweep pass.
func TestMoveSweepReclaimsMovedAwayCopy(t *testing.T) {
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	rec := &fakeReclaimer{}
	r, dataDir := sweepTestRunner(t, store, rec)
	mkLocalPartitionDir(t, dataDir)

	r.Reconcile(context.Background()) // pass 1 → sweep runs
	r.wg.Wait()

	if rec.count() != 1 {
		t.Fatalf("reclaim calls = %d, want 1", rec.count())
	}
}

// Every refusal gate: locally owned, moving to us, no local dir, and a
// leader that cannot confirm (follower whose leader RPC fails). None may
// reclaim.
func TestMoveSweepRefusalGates(t *testing.T) {
	cases := []struct {
		name    string
		store   *fakeMoveStore
		makeDir bool
	}{
		{
			name: "locally owned",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-dst"},
			},
			makeDir: true,
		},
		{
			name: "no local dir",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
			},
			makeDir: false,
		},
		{
			name: "leader unreachable for confirmation",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
				leaderID:   "narad-ldr", // not self → follower path → movePeerFake.GetAssignment errors
				member:     metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive},
			},
			makeDir: true,
		},
		{
			name: "unassigned partition",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: ""},
			},
			makeDir: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeReclaimer{}
			r, dataDir := sweepTestRunner(t, tc.store, rec)
			if tc.makeDir {
				mkLocalPartitionDir(t, dataDir)
			}
			r.sweepStaleCopies(context.Background())
			if rec.count() != 0 {
				t.Fatalf("%s: reclaim was called (%d times) — must refuse", tc.name, rec.count())
			}
		})
	}
}

// Before reclaiming, the sweep asks the new owner what it holds and
// hands the reclaim a KNOWN guard at the position the owner vouches for
// (its hwm, capped at the move marker's promoted one), which quarantines
// a local copy that is ahead of it. An owner without a marker is guarded
// by its own hwm, or the copy is set aside when the owner holds records
// that did not come from it; it is never reclaimed unguarded. An owner
// that cannot be asked defers the sweep.
func TestMoveSweepPassesPromotedHWMToReclaim(t *testing.T) {
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-new"},
		member:     metastore.Member{ID: "narad-new", Addr: "newaddr", Status: metastore.MemberAlive},
	}
	rec := &fakeReclaimer{}
	dataDir := t.TempDir()
	peer := movePeerFake{
		marker:     &messaging.MoveMarker{Source: "narad-dst", HighWatermark: 42, ForcePromoted: true},
		dirFetcher: dirFetcher{hwm: 50}, // the owner took more produce since
	}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{})
	mkLocalPartitionDir(t, dataDir)

	r.sweepStaleCopies(context.Background())
	if rec.count() != 1 || len(rec.guards) != 1 {
		t.Fatalf("reclaim calls = %d (guarded %d), want 1 guarded call", rec.count(), len(rec.guards))
	}
	if g := rec.guards[0]; !g.Known || g.PromotedHWM != 42 || g.SetAside != "" {
		t.Fatalf("reclaim guard = %+v, want the owner's promoted hwm 42", g)
	}

	// No marker on the owner and nothing local: guarded by the hwm it
	// reports.
	rec = &fakeReclaimer{}
	r = NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{dirFetcher: dirFetcher{hwm: 7}}, rec, nil, nil, MoveConfig{})
	r.sweepStaleCopies(context.Background())
	if len(rec.guards) != 1 || !rec.guards[0].Known || rec.guards[0].PromotedHWM != 7 || rec.guards[0].SetAside != "" {
		t.Fatalf("guards = %+v, want one reclaim guarded at the owner's hwm 7", rec.guards)
	}

	// No marker on the owner, which holds records, and records here: the
	// owner's records did not come from this copy, so it is set aside.
	ownerDir := t.TempDir()
	ownerHWM, _ := buildSourcePartition(t, ownerDir, 5)
	localData := t.TempDir()
	buildSourcePartition(t, storage.TopicPartitionDir(localData, "orders", 0), 5)
	rec = &fakeReclaimer{}
	r = NewMoveRunner(store, "narad-dst", localData, movePeerFake{dirFetcher: dirFetcher{dir: ownerDir, hwm: ownerHWM}}, rec, nil, nil, MoveConfig{})
	r.sweepStaleCopies(context.Background())
	if len(rec.guards) != 1 || !rec.guards[0].Known || rec.guards[0].SetAside == "" {
		t.Fatalf("guards = %+v, want one reclaim that sets the copy aside", rec.guards)
	}

	// Owner has no address: the sweep defers rather than deleting blind.
	rec = &fakeReclaimer{}
	store.member.Addr = ""
	r = NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{})
	r.sweepStaleCopies(context.Background())
	if rec.count() != 0 {
		t.Fatalf("reclaim called %d times with the owner unreachable; must defer", rec.count())
	}
}

// plainReclaimer offers only the reclaim that deletes without comparing.
type plainReclaimer struct{ calls int }

func (p *plainReclaimer) ReclaimMovedPartition(context.Context, string, int) error {
	p.calls++
	return nil
}

// A broker that cannot compare the local copy with what the new owner
// holds is never asked to reclaim: the copy stays where it is.
func TestMoveSweepNeverCallsAnUnguardedReclaim(t *testing.T) {
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-new"},
		member:     metastore.Member{ID: "narad-new", Addr: "newaddr", Status: metastore.MemberAlive},
	}
	rec := &plainReclaimer{}
	dataDir := t.TempDir()
	peer := movePeerFake{marker: &messaging.MoveMarker{Source: "narad-dst", HighWatermark: 42}, dirFetcher: dirFetcher{hwm: 42}}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{})
	dir := mkLocalPartitionDir(t, dataDir)
	r.sweepStaleCopies(context.Background())
	if rec.calls != 0 {
		t.Fatalf("the unguarded reclaim was called %d times", rec.calls)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the local copy is gone: %v", err)
	}
}

// A new owner on a release that did not sync its copy before the flip
// leaves a move marker with no durable stamp, and its listing can report
// segments that are still only in its page cache. The old owner's sweep
// waits until the install is old enough for the kernel to have written
// it back before it trusts that listing; a stamped marker is trusted at
// once.
func TestMoveSweepWaitsOutTheWritebackOfAnUnsyncedInstall(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name        string
		installedAt time.Time
		durable     bool
		wantReclaim bool
	}{
		{name: "unsynced, installed just now", installedAt: now},
		{name: "unsynced, installed past the writeback window", installedAt: now.Add(-moveUnsyncedCopyWriteback - time.Minute), wantReclaim: true},
		{name: "synced before the flip", installedAt: now, durable: true, wantReclaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-new"},
				member:     metastore.Member{ID: "narad-new", Addr: "newaddr", Status: metastore.MemberAlive},
			}
			marker := &messaging.MoveMarker{Source: "narad-dst", HighWatermark: 42, InstalledAtUnixMs: tc.installedAt.UnixMilli()}
			if tc.durable {
				marker.DurableAtUnixMs = tc.installedAt.UnixMilli()
			}
			rec := &fakeReclaimer{}
			dataDir := t.TempDir()
			r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{marker: marker, dirFetcher: dirFetcher{hwm: 42}}, rec, nil, nil, MoveConfig{})
			mkLocalPartitionDir(t, dataDir)
			r.sweepStaleCopies(context.Background())
			if got := rec.count() == 1; got != tc.wantReclaim {
				t.Fatalf("reclaimed = %v (%d calls), want %v", got, rec.count(), tc.wantReclaim)
			}
		})
	}
}

// recordView serves runtime.Logs the topic records of a fakeMoveStore,
// the only metastore call the log map makes.
type recordView struct {
	metastore.Metastore
	store *fakeMoveStore
}

func (v recordView) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return v.store.GetTopic(ctx, name)
}

// orphanReclaimer reclaims deleted topics' directories through a real
// runtime.Logs, as the engine does. beforePurge, when set, runs once
// before the first purge takes the topic's guard.
type orphanReclaimer struct {
	keepingReclaimer
	beforePurge func()
	purges      atomic.Int32
}

func (o *orphanReclaimer) ReclaimOrphanTopicDir(topicName, id string) (bool, error) {
	if fn := o.beforePurge; fn != nil {
		o.beforePurge = nil
		fn()
	}
	o.purges.Add(1)
	return o.logs.ReclaimOrphanTopicDir(topicName, id)
}

func (o *orphanReclaimer) QuarantinedCopies() (runtime.QuarantineSummary, error) {
	return o.logs.QuarantinedCopies()
}

// orphanTestRunner wires a runner whose reclaimer purges through a real
// runtime.Logs over store's records. The store's topic list holds only
// "live" unless the test says otherwise.
func orphanTestRunner(t *testing.T, store *fakeMoveStore, peer movePeerFake) (*MoveRunner, string, *orphanReclaimer) {
	t.Helper()
	if store.topics == nil {
		store.topics = []topic.Topic{{Name: "live", ID: "9999999999999999", Partitions: 1}}
	}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, recordView{store: store}, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	rec := &orphanReclaimer{keepingReclaimer: keepingReclaimer{logs: logs}}
	return NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{}), dataDir, rec
}

// mkTopicDirWithData makes topics/<name>/p00000 holding a segment file,
// stamped with incarnation id unless it is empty.
func mkTopicDirWithData(t *testing.T, dataDir, name, id string) string {
	t.Helper()
	part := storage.TopicPartitionDir(dataDir, name, 0)
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(part, "00000000000000000000.log"), []byte("records"), 0o644); err != nil {
		t.Fatal(err)
	}
	if id != "" {
		if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, name), id); err != nil {
			t.Fatal(err)
		}
	}
	return topicDirT(t, dataDir, name)
}

func dirExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// A node that missed a deleted topic's purge (it was down, marked dead
// or lagging when the delete committed) keeps the topic's directory. The
// periodic sweep removes it once the leader confirms the directory's
// incarnation is gone: the name is absent, or live as another
// incarnation.
func TestStaleCopySweepReclaimsADeletedTopicsDirectoryOnceTheLeaderConfirms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeMoveStore
		peer  movePeerFake
	}{
		{"the leader has no such topic", &fakeMoveStore{}, movePeerFake{}},
		{
			"the leader has a newer incarnation",
			&fakeMoveStore{leaderID: "narad-ldr", member: metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}},
			movePeerFake{leaderTopic: &topic.Topic{Name: "gone", ID: "2222222222222222"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, dataDir, _ := orphanTestRunner(t, tc.store, tc.peer)
			dir := mkTopicDirWithData(t, dataDir, "gone", "1111111111111111")
			live := mkTopicDirWithData(t, dataDir, "live", "9999999999999999")
			r.sweepStaleCopies(context.Background())
			if dirExists(dir) {
				t.Fatalf("the deleted topic's directory %s survived the sweep", dir)
			}
			if entries, _ := os.ReadDir(storage.TopicsDir(dataDir)); len(entries) != 1 {
				t.Fatalf("topics/ holds %d entries after the sweep, want only the live topic's", len(entries))
			}
			if !dirExists(live) {
				t.Fatal("the live topic's directory was removed")
			}
		})
	}
}

// Without the leader's word the directory stays: a leader that cannot
// be asked, or one that still lists the directory's incarnation (this
// replica is behind a create), never authorizes the purge.
func TestStaleCopySweepKeepsAnOrphanDirectoryTheLeaderCannotConfirm(t *testing.T) {
	for _, tc := range []struct {
		name string
		peer movePeerFake
	}{
		{"the leader is unreachable", movePeerFake{}},
		{"the leader still lists the incarnation", movePeerFake{leaderTopic: &topic.Topic{Name: "gone", ID: "1111111111111111"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeMoveStore{leaderID: "narad-ldr", member: metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}}
			r, dataDir, rec := orphanTestRunner(t, store, tc.peer)
			dir := mkTopicDirWithData(t, dataDir, "gone", "1111111111111111")
			r.sweepStaleCopies(context.Background())
			if !dirExists(dir) {
				t.Fatal("the directory was removed without the leader's confirmation")
			}
			if n := rec.purges.Load(); n != 0 {
				t.Fatalf("the sweep asked for %d purges without the leader's confirmation", n)
			}
		})
	}
}

// The leader's confirmation and the purge are not atomic: the name can
// be recreated in between, with this node opening the successor's
// partition (which sets the old directory aside and stamps the
// successor's marker). The purge re-checks under the topic's guard and
// leaves the successor's directory alone.
func TestStaleCopySweepLeavesADirectoryARecreateStamped(t *testing.T) {
	store := &fakeMoveStore{}
	r, dataDir, rec := orphanTestRunner(t, store, movePeerFake{})
	dir := mkTopicDirWithData(t, dataDir, "gone", "1111111111111111")
	var successorHWM int64
	var hookErr error
	rec.beforePurge = func() {
		store.topics = append(store.topics, topic.Topic{Name: "gone", ID: "2222222222222222", Partitions: 1})
		l, err := rec.logs.Get("gone", 0)
		if err != nil {
			hookErr = err
			return
		}
		for range 3 {
			if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte("successor"))); err != nil {
				hookErr = err
				return
			}
		}
		if hookErr = l.CommitDurable(0, 2); hookErr == nil {
			successorHWM = l.HighWatermark()
		}
	}
	r.sweepStaleCopies(context.Background())
	if hookErr != nil || successorHWM != 3 {
		t.Fatalf("setup: the successor's produce: hwm %d, err %v", successorHWM, hookErr)
	}
	if rec.purges.Load() == 0 {
		t.Fatal("setup: the sweep never asked for the purge")
	}
	if marker, _, _ := storage.ReadTopicIncarnation(dir); marker != "2222222222222222" {
		t.Fatalf("topics/gone carries marker %q after the sweep, want the successor's", marker)
	}
	l, err := rec.logs.Get("gone", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := l.NextOffset(); got != 3 {
		t.Fatalf("the successor reopens at next offset %d after the sweep, want 3", got)
	}
}

// A directory with no incarnation marker was written before markers
// existed, or by a record without an ID; the periodic sweep cannot tell
// it from a directory a concurrent open is making, so it only counts it.
// The startup sweep, which runs under the create gate, removes it.
func TestStaleCopySweepNeverRemovesAnUnmarkedOrphanDirectory(t *testing.T) {
	r, dataDir, rec := orphanTestRunner(t, &fakeMoveStore{}, movePeerFake{})
	dir := mkTopicDirWithData(t, dataDir, "gone", "")
	r.sweepStaleCopies(context.Background())
	if !dirExists(dir) {
		t.Fatal("the periodic sweep removed an unmarked directory")
	}
	if n := rec.purges.Load(); n != 0 {
		t.Fatalf("the sweep asked for %d purges of an unmarked directory", n)
	}
}

// narad_orphan_topic_dirs reports the plain directories of topics this
// replica no longer knows that the pass left in place: an unmarked one
// (only the startup sweep removes it) counts, a reclaimed one does not,
// and neither do the live topic's directory or a quarantined copy.
func TestOrphanTopicDirsGaugeCountsWhatTheSweepKept(t *testing.T) {
	r, dataDir, _ := orphanTestRunner(t, &fakeMoveStore{}, movePeerFake{})
	reg := prometheus.NewRegistry()
	r.RegisterMetrics(reg)
	mkTopicDirWithData(t, dataDir, "gone", "1111111111111111")
	mkTopicDirWithData(t, dataDir, "legacy", "")
	mkTopicDirWithData(t, dataDir, "live", "9999999999999999")
	if err := storage.WriteTopicIncarnation(staleTopicDirT(t, dataDir, "live", "8888888888888888"), "8888888888888888"); err != nil {
		t.Fatal(err)
	}
	r.sweepStaleCopies(context.Background())
	if dirExists(topicDirT(t, dataDir, "gone")) {
		t.Fatal("setup: the confirmed orphan directory survived the sweep")
	}
	want := `
# HELP narad_orphan_topic_dirs Topic directories on this node whose topic no longer exists and that the last reclaim pass left in place: unmarked directories (removed only by the startup sweep) and directories the leader has not yet confirmed gone.
# TYPE narad_orphan_topic_dirs gauge
narad_orphan_topic_dirs 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "narad_orphan_topic_dirs"); err != nil {
		t.Fatalf("after the sweep (want 1, the unmarked directory it kept): %v", err)
	}
}

// Every sweep retakes the quarantine inventory, so a copy set aside on
// this node shows in narad_quarantined_copies within one sweep, and a
// scrape never walks the disk itself.
func TestStaleCopySweepRefreshesTheQuarantineInventory(t *testing.T) {
	r, dataDir, rec := orphanTestRunner(t, &fakeMoveStore{}, movePeerFake{})
	mkTopicDirWithData(t, dataDir, "live", "9999999999999999")
	set := storage.TopicPartitionDir(dataDir, "live", 0) + messaging.QuarantineSuffix
	if err := os.Rename(storage.TopicPartitionDir(dataDir, "live", 0), set); err != nil {
		t.Fatal(err)
	}
	r.sweepStaleCopies(context.Background())
	sum, ok := rec.logs.LastQuarantinedCopies()
	if !ok || sum.Count != 1 || len(sum.Copies) != 1 || sum.Copies[0].Dir != set {
		t.Fatalf("inventory after the sweep = %+v (taken %v), want the one set-aside copy %s", sum, ok, set)
	}
}
