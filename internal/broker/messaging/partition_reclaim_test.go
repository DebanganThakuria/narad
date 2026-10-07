package messaging

// Reclaim deletes partition data, so its guards get their own tests: it
// must refuse for anything locally owned (or becoming so) and only delete
// a copy whose partition affirmatively lives elsewhere.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestReclaimMovedPartitionGuards(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	// p0 owned by us; p1 moved away; p2 owned elsewhere but moving BACK to us.
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-other"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-other"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.SetAssignmentTarget(ctx, "orders", 2, "node-self"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	dataDir := engine.logs.DataDir()
	mkPartition := func(p int) string {
		dir := topicPartitionDirT(t, dataDir, "orders", p)
		log, err := storage.NewLog(dir, storage.Options{})
		if err != nil {
			t.Fatalf("NewLog(p%d): %v", p, err)
		}
		if _, err := log.Append(storage.EncodeKeyedRecord("k", 0, []byte("x"))); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	dir0, dir1, dir2 := mkPartition(0), mkPartition(1), mkPartition(2)

	// Locally owned: refused, data untouched.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 0); err == nil {
		t.Fatal("reclaim of a locally-owned partition must refuse")
	}
	if _, err := os.Stat(dir0); err != nil {
		t.Fatalf("owned partition dir touched: %v", err)
	}

	// Moving back to us: refused, data untouched.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 2); err == nil {
		t.Fatal("reclaim of a partition targeted at this node must refuse")
	}
	if _, err := os.Stat(dir2); err != nil {
		t.Fatalf("inbound-move partition dir touched: %v", err)
	}

	// Moved away: reclaimed, dir gone.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 1); err != nil {
		t.Fatalf("reclaim of a moved-away partition: %v", err)
	}
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Fatalf("moved-away partition dir still present (err=%v)", err)
	}
}

// A source that lost contact (not crashed) kept committing while the
// destination force-promoted its copy at the source's last-seen HWM. When
// the source returns, its copy is AHEAD of the promoted position and holds
// records that exist nowhere else: the reclaim must quarantine it, never
// delete it. A copy at or behind the promoted HWM is reclaimed as before,
// and a reclaim that does not know the promoted position deletes as it
// always did.
func TestReclaimQuarantinesCopyAheadOfPromotedHWM(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	for p := range 3 {
		if err := store.AssignPartition(ctx, "orders", p, "node-other"); err != nil {
			t.Fatalf("AssignPartition: %v", err)
		}
	}
	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	dataDir := engine.logs.DataDir()
	mkPartition := func(p, n int) string {
		dir := topicPartitionDirT(t, dataDir, "orders", p)
		log, err := storage.NewLog(dir, storage.Options{})
		if err != nil {
			t.Fatalf("NewLog(p%d): %v", p, err)
		}
		for i := range n {
			if _, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), []byte("x"))); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := log.AdvanceHighWatermark(int64(n)); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	dir0, dir1, dir2 := mkPartition(0, 10), mkPartition(1, 10), mkPartition(2, 10)

	// p0: promoted elsewhere at 7, local copy has 10: AHEAD, quarantine.
	err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 0, ReclaimGuard{PromotedHWM: 7, Known: true})
	if !errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("reclaim of a copy ahead of the promoted hwm = %v, want ErrPartitionQuarantined", err)
	}
	if _, err := os.Stat(dir0); !os.IsNotExist(err) {
		t.Fatalf("quarantined partition dir still at its original path (err=%v)", err)
	}
	qdir := dir0 + QuarantineSuffix
	log, err := storage.NewLog(qdir, storage.Options{})
	if err != nil {
		t.Fatalf("quarantined copy does not recover: %v", err)
	}
	if log.NextOffset() != 10 {
		t.Fatalf("quarantined copy next offset = %d, want 10 (records intact)", log.NextOffset())
	}
	_ = log.Close()

	// p1: promoted at 10 (or later): the copy holds nothing unique, reclaim.
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 1, ReclaimGuard{PromotedHWM: 10, Known: true}); err != nil {
		t.Fatalf("reclaim of a copy at the promoted hwm: %v", err)
	}
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Fatalf("copy at the promoted hwm not reclaimed (err=%v)", err)
	}
	// p2: promoted position unknown (older owner): delete as before.
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 2, ReclaimGuard{}); err != nil {
		t.Fatalf("unguarded reclaim: %v", err)
	}
	if _, err := os.Stat(dir2); !os.IsNotExist(err) {
		t.Fatalf("unguarded copy not reclaimed (err=%v)", err)
	}
}

// lockedBuffer is a log sink safe to read while the engine writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// When the new owner cannot vouch for the local copy (it lost its
// volume, rolled its install back, or lost a segment it was given), the
// copy may hold the only instance of its records: the reclaim sets it
// aside to <dir>.quarantine, whatever the promoted position and even when
// its log cannot be recovered, and never deletes it. It still refuses,
// touching nothing, for a partition that is locally owned or a topic
// directory of another incarnation.
func TestReclaimSetsAsideACopyTheOwnerCannotVouchFor(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "events", ID: "aaaaaaaaaaaaaaaa", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	for p, owner := range []string{"node-other", "node-other", "node-self"} {
		if err := store.AssignPartition(ctx, "orders", p, owner); err != nil {
			t.Fatalf("AssignPartition: %v", err)
		}
	}
	if err := store.AssignPartition(ctx, "events", 0, "node-other"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	var logged lockedBuffer
	engine := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(&logged, nil)), "node-self")
	dataDir := logs.DataDir()
	mkPartition := func(topicName string, p, n int) string {
		dir := topicPartitionDirT(t, dataDir, topicName, p)
		log, err := storage.NewLog(dir, storage.Options{})
		if err != nil {
			t.Fatalf("NewLog(%s/p%d): %v", topicName, p, err)
		}
		for i := range n {
			if _, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), []byte("x"))); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := log.AdvanceHighWatermark(int64(n)); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	const reason = "the owner lists no records while the local copy holds unexpired records"
	setAside := ReclaimGuard{PromotedHWM: 10, Known: true, SetAside: reason}

	// p0: at the promoted position, which a plain guard would delete.
	dir0 := mkPartition("orders", 0, 10)
	err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 0, setAside)
	if !errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("set-aside reclaim = %v, want ErrPartitionQuarantined", err)
	}
	if _, err := os.Stat(dir0); !os.IsNotExist(err) {
		t.Fatalf("set-aside copy still at its path (err=%v)", err)
	}
	if l, err := storage.NewLog(dir0+QuarantineSuffix, storage.Options{}); err != nil {
		t.Fatalf("set-aside copy does not recover: %v", err)
	} else {
		if l.NextOffset() != 10 {
			t.Fatalf("set-aside copy next offset %d, want 10", l.NextOffset())
		}
		_ = l.Close()
	}
	if out := logged.String(); !strings.Contains(out, "the new owner cannot vouch for the local partition copy") || !strings.Contains(out, reason) {
		t.Fatalf("no error line naming the set-aside and its reason: %s", out)
	}

	// p2: locally owned: refused, untouched.
	dir2 := mkPartition("orders", 2, 3)
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 2, setAside); err == nil || errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("set-aside reclaim of a locally owned partition = %v, want a refusal", err)
	}
	if _, err := os.Stat(dir2); err != nil {
		t.Fatalf("locally owned partition touched: %v", err)
	}
	if _, err := os.Stat(dir2 + QuarantineSuffix); !os.IsNotExist(err) {
		t.Fatalf("locally owned partition set aside (err=%v)", err)
	}

	// events/0: the topic directory belongs to another incarnation.
	dirE := mkPartition("events", 0, 3)
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "events"), "bbbbbbbbbbbbbbbb"); err != nil {
		t.Fatal(err)
	}
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "events", 0, setAside); err == nil || errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("set-aside reclaim under another incarnation's directory = %v, want a refusal", err)
	}
	if _, err := os.Stat(dirE); err != nil {
		t.Fatalf("other incarnation's partition touched: %v", err)
	}
	if _, err := os.Stat(dirE + QuarantineSuffix); !os.IsNotExist(err) {
		t.Fatalf("other incarnation's partition set aside (err=%v)", err)
	}

	// p1: a copy whose log cannot be recovered is set aside as it is.
	dir1 := mkPartition("orders", 1, 4)
	segs, err := storage.ListPartitionSegments(dir1)
	if err != nil || len(segs) == 0 {
		t.Fatalf("list p1 segments: %v (%d)", err, len(segs))
	}
	unreadable := filepath.Join(dir1, fmt.Sprintf("%020d.log", segs[0].BaseOffset))
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o644) })
	if _, err := storage.NewLog(dir1, storage.Options{}); err == nil {
		t.Skip("this platform can read a mode-000 file (running as root?); cannot build an unrecoverable copy")
	}
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 1, setAside); !errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("set-aside reclaim of an unrecoverable copy = %v, want ErrPartitionQuarantined", err)
	}
	if _, err := os.Stat(dir1 + QuarantineSuffix); err != nil {
		t.Fatalf("unrecoverable copy not set aside: %v", err)
	}
	_ = os.Chmod(filepath.Join(dir1+QuarantineSuffix, filepath.Base(unreadable)), 0o644)
}

// successorOpen deletes and recreates orders on store with partition 0
// assigned to successorOwner (none when empty), opens the successor's
// partition on logs and commits 7 records to it, reporting the hwm.
func successorOpen(ctx context.Context, store *metastore.Store, logs *runtime.Logs, successorOwner string) (int64, error) {
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		return 0, err
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: successorIncarnation, Partitions: 1}); err != nil {
		return 0, err
	}
	if successorOwner != "" {
		if err := store.AssignPartition(ctx, "orders", 0, successorOwner); err != nil {
			return 0, err
		}
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

// seedMovedAwayCopy gives this node a 5-record copy of orders/0 of the
// retired incarnation, whose partition has since moved to node-other.
func seedMovedAwayCopy(t *testing.T, store *metastore.Store, logs *runtime.Logs) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: retiredIncarnation, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	old, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := old.Append(storage.EncodeKeyedRecord("k", 1, []byte("old"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.CommitDurable(0, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatal(err)
	}
}

// requireSuccessorKeepsItsRecords fails unless orders/0 on dataDir still
// recovers the successor's 7 committed records.
func requireSuccessorKeepsItsRecords(t *testing.T, logs *runtime.Logs, dataDir string, reclaimErr error) {
	t.Helper()
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
		t.Fatalf("the successor's partition directory recovers next offset %d after the reclaim (%v), want 7 (its 7 committed records); topic marker %q (marked %v)",
			got, reclaimErr, id, marked)
	}
}

// A reclaim closes the partition's log and resets its consumer state,
// then acts on the partition's PATH. A delete and recreate of the name
// in between, with this node serving the successor's partition (its open
// quarantines the old topic directory and makes a new partition
// directory), must not lose the successor's directory: the reclaim acts
// under the topic's guard with the log closed, and only while the topic
// marker is absent or names the incarnation it read.
//
// The only seam is the drop notifier (production wires the committer's
// Forget there), which runs the recreate and the successor's first
// produce at the reclaim's reset.
func TestReclaimLeavesASuccessorOpenedDuringTheReclaim(t *testing.T) {
	for _, guard := range []struct {
		name  string
		guard ReclaimGuard
	}{
		{"unguarded", ReclaimGuard{}},
		{"guarded", ReclaimGuard{Known: true, PromotedHWM: 5}},
	} {
		t.Run(guard.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			dataDir := t.TempDir()
			logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
			t.Cleanup(func() { _ = logs.CloseAll() })
			seedMovedAwayCopy(t, store, logs)

			offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
				return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
			}, nil)
			var successorHWM int64
			var hookErr error
			fired := false
			offsets.SetDropNotifier(func(string, int) {
				if fired {
					return
				}
				fired = true
				successorHWM, hookErr = successorOpen(ctx, store, logs, "")
			})
			e := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
			t.Cleanup(func() { e.dispatch.close() })

			rerr := e.ReclaimMovedPartitionGuarded(ctx, "orders", 0, guard.guard)
			if hookErr != nil || successorHWM != 7 {
				t.Fatalf("setup: the successor's produce: hwm %d, err %v", successorHWM, hookErr)
			}
			requireSuccessorKeepsItsRecords(t, logs, dataDir, rerr)
		})
	}
}

// recordHookStore runs hook once, inside the first GetTopic, before the
// read.
type recordHookStore struct {
	*metastore.Store
	mu   sync.Mutex
	hook func()
}

func (s *recordHookStore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	s.mu.Lock()
	hook := s.hook
	s.hook = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return s.Store.GetTopic(ctx, name)
}

// A reclaim compares the topic marker, under the topic's guard, with the
// incarnation it read, so it must read that incarnation before it checks
// the assignment. Read the other way round, a delete and recreate
// applied between the two reads, with the successor's partition placed
// on this node and opened here, made the record the successor's while
// the approved assignment was the deleted incarnation's: the marker
// check passed on the successor's own directory, and the reclaim removed
// it (or set it aside, when the guard found it ahead).
//
// The only seam: the engine's metastore runs the delete, the recreate,
// the successor's open and 7 commits inside the reclaim's first topic
// read, standing for the reclaim being descheduled at that read.
func TestReclaimReadsTheIncarnationBeforeTheAssignment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		guard ReclaimGuard
	}{
		{"unguarded", ReclaimGuard{}},
		{"guarded behind", ReclaimGuard{Known: true, PromotedHWM: 10}},
		{"guarded ahead", ReclaimGuard{Known: true, PromotedHWM: 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			real := newTestStore(t)
			dataDir := t.TempDir()
			logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, real, nil)
			t.Cleanup(func() { _ = logs.CloseAll() })
			seedMovedAwayCopy(t, real, logs)

			var successorHWM int64
			var hookErr error
			store := &recordHookStore{Store: real}
			store.hook = func() { successorHWM, hookErr = successorOpen(ctx, real, logs, "node-self") }
			offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
				return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
			}, nil)
			e := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
			t.Cleanup(func() { e.dispatch.close() })

			rerr := e.ReclaimMovedPartitionGuarded(ctx, "orders", 0, tc.guard)
			if hookErr != nil || successorHWM != 7 {
				t.Fatalf("setup: the successor's produce: hwm %d, err %v", successorHWM, hookErr)
			}
			if rerr == nil {
				t.Errorf("the reclaim of a partition now placed on this node succeeded, want a refusal")
			}
			requireSuccessorKeepsItsRecords(t, logs, dataDir, rerr)
		})
	}
}
