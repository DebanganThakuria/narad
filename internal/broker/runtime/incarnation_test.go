package runtime

// A topic name outlives the topic. These tests pin the incarnation
// bookkeeping that stops a recreated topic from inheriting the deleted
// one's data: the marker, adoption of unmarked directories, quarantine
// of a stale directory on open, the id-aware purge, and the guard that
// keeps a purge and a concurrent open from interleaving.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// newIncarnationStore is a single-node Raft-backed metastore: the
// versioned fast-path re-check only exists against a real store.
func newIncarnationStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{NodeID: "n0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = s.DeleteTopic(context.Background(), "__probe__")
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func appendOld(t *testing.T, l *storage.Log, n int, payload string) {
	t.Helper()
	for range n {
		if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte(payload))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.CommitDurable(0, int64(n-1)); err != nil {
		t.Fatalf("CommitDurable: %v", err)
	}
}

func readMarker(t *testing.T, dataDir, name string) string {
	t.Helper()
	id, ok, err := storage.ReadTopicIncarnation(topicDirT(t, dataDir, name))
	if err != nil {
		t.Fatalf("ReadTopicIncarnation: %v", err)
	}
	if !ok {
		return ""
	}
	return id
}

func staleDirs(t *testing.T, dataDir, name string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dataDir, "topics"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), name+storage.StaleTopicDirSuffix) {
			out = append(out, e.Name())
		}
	}
	return out
}

// The audit reproduction: delete "orders", recreate "orders", and the
// purge for the old incarnation never runs on this node (it was
// skipped because the name existed again, or the node was down). The
// recreated topic must open EMPTY; the old incarnation's directory is
// quarantined, not served, and the retired hook fires so in-memory
// consumer state does not carry over either.
func TestRecreatedTopicDoesNotResurrectOldData(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	var retired []string
	logs.SetTopicRetiredHook(func(name string) { retired = append(retired, name) })

	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "1111111111111111", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	appendOld(t, l, 3, "old-incarnation")
	if err := logs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	if got := readMarker(t, dataDir, "orders"); got != "1111111111111111" {
		t.Fatalf("marker after first open = %q, want the first incarnation", got)
	}

	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(again): %v", err)
	}

	l2, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(recreated): %v", err)
	}
	if hwm := l2.HighWatermark(); hwm != 0 {
		t.Fatalf("recreated topic opened with hwm=%d; it resurrected the deleted incarnation's data", hwm)
	}
	if got := readMarker(t, dataDir, "orders"); got != "2222222222222222" {
		t.Fatalf("marker after recreate = %q, want the second incarnation", got)
	}
	stale := staleDirs(t, dataDir, "orders")
	if len(stale) != 1 || stale[0] != "orders"+storage.StaleTopicDirSuffix+"1111111111111111" {
		t.Fatalf("quarantined dirs = %v, want [orders.stale-1111111111111111]", stale)
	}
	// The quarantined copy still holds the old records, untouched.
	old, err := storage.NewLog(filepath.Join(dataDir, "topics", stale[0], "p00000"), storage.Options{})
	if err != nil {
		t.Fatalf("open quarantined copy: %v", err)
	}
	defer old.Close()
	if old.NextOffset() != 3 {
		t.Fatalf("quarantined copy next offset = %d, want 3", old.NextOffset())
	}
	if len(retired) != 1 || retired[0] != "orders" {
		t.Fatalf("retired hook calls = %v, want [orders]", retired)
	}
}

// A delete plus recreate applied while the partition logs are OPEN
// (the purge has not arrived yet) must retire the open logs: the fast
// path re-checks the topic version and the next Get quarantines.
func TestOpenLogIsRetiredWhenIncarnationChangesUnderIt(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()

	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "aaaaaaaaaaaaaaaa", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	l0, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	appendOld(t, l0, 2, "old")
	if _, err := logs.Get("orders", 1); err != nil {
		t.Fatalf("Get(1): %v", err)
	}

	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "bbbbbbbbbbbbbbbb", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic(again): %v", err)
	}

	l1, err := logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get(recreated, 1): %v", err)
	}
	if l1.HighWatermark() != 0 {
		t.Fatalf("recreated partition 1 hwm = %d, want 0", l1.HighWatermark())
	}
	// Partition 0's old log was closed by the retire, so a writer that
	// still holds it cannot commit into the quarantined directory.
	if _, err := l0.Append([]byte("late")); !errors.Is(err, storage.ErrLogClosed) {
		t.Fatalf("Append on the retired log error = %v, want ErrLogClosed", err)
	}
	l0b, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(recreated, 0): %v", err)
	}
	if l0b.HighWatermark() != 0 || l0b == l0 {
		t.Fatalf("recreated partition 0 served the old log (hwm=%d)", l0b.HighWatermark())
	}
	if got := readMarker(t, dataDir, "orders"); got != "bbbbbbbbbbbbbbbb" {
		t.Fatalf("marker = %q, want bbbbbbbbbbbbbbbb", got)
	}
}

// A directory written before markers existed is adopted by the first
// open: stamped with the current incarnation, data kept.
func TestUnmarkedTopicDirIsAdoptedOnOpen(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "cccccccccccccccc", Partitions: 1}
	dataDir := t.TempDir()
	pre, err := storage.NewLog(storage.TopicPartitionDir(dataDir, "orders", 0), storage.Options{FlushInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	appendOld(t, pre, 2, "pre-upgrade")
	_ = pre.Close()
	if got := readMarker(t, dataDir, "orders"); got != "" {
		t.Fatalf("unexpected marker before open: %q", got)
	}

	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	defer logs.CloseAll()
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if l.HighWatermark() != 2 {
		t.Fatalf("adopted log hwm = %d, want 2 (pre-upgrade data kept)", l.HighWatermark())
	}
	if got := readMarker(t, dataDir, "orders"); got != "cccccccccccccccc" {
		t.Fatalf("marker after adoption = %q, want cccccccccccccccc", got)
	}
	if stale := staleDirs(t, dataDir, "orders"); len(stale) != 0 {
		t.Fatalf("adoption quarantined something: %v", stale)
	}
}

// A record without an incarnation (created before IDs) keeps the
// name-based behaviour: no marker is written, nothing is quarantined.
func TestRecordWithoutIncarnationLeavesDirUnmarked(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["legacy"] = topic.Topic{Name: "legacy", Partitions: 1}
	logs := newRuntimeTestLogs(t, ms)
	defer logs.CloseAll()
	if _, err := logs.Get("legacy", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readMarker(t, logs.DataDir(), "legacy"); got != "" {
		t.Fatalf("marker = %q, want none for a record without an ID", got)
	}
}

// A purge that names the deleted incarnation removes that incarnation's
// directory even though the name exists locally again, and leaves the
// recreated topic's directory alone. Both orders (purge before or after
// the recreate's first open) end in the same state.
func TestPurgeByIncarnationWithNameRecreated(t *testing.T) {
	for _, order := range []string{"purge-first", "open-first"} {
		t.Run(order, func(t *testing.T) {
			ms := newRuntimeFakeMetastore()
			dataDir := t.TempDir()
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
			defer logs.CloseAll()

			ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 1}
			l, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			appendOld(t, l, 3, "old")
			// The fake metastore has no versions, so an open entry is
			// trusted by the fast path (the versioned re-check has its
			// own test above); close the log as an idle eviction would.
			if err := logs.CloseTopic("orders"); err != nil {
				t.Fatalf("CloseTopic: %v", err)
			}
			// Recreate applied locally; the purge for 0000000000000001 is
			// still in flight.
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 1}

			if order == "open-first" {
				if l2, err := logs.Get("orders", 0); err != nil || l2.HighWatermark() != 0 {
					t.Fatalf("Get(recreated) = hwm %v, err %v; want empty", l2, err)
				}
				if stale := staleDirs(t, dataDir, "orders"); len(stale) != 1 {
					t.Fatalf("stale dirs = %v, want the quarantined old incarnation", stale)
				}
			}
			purged, err := logs.PurgeTopic("orders", "0000000000000001")
			if err != nil {
				t.Fatalf("PurgeTopic: %v", err)
			}
			if order == "purge-first" && !purged {
				t.Fatal("purge-first: old incarnation's directory not purged")
			}
			if order == "open-first" && purged {
				t.Fatal("open-first: purge removed the recreated topic's directory")
			}
			if stale := staleDirs(t, dataDir, "orders"); len(stale) != 0 {
				t.Fatalf("stale dirs after purge = %v, want none (purge reclaims its quarantine)", stale)
			}
			l3, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatalf("Get after purge: %v", err)
			}
			if l3.HighWatermark() != 0 {
				t.Fatalf("recreated topic hwm = %d after purge, want 0", l3.HighWatermark())
			}
			if got := readMarker(t, dataDir, "orders"); got != "0000000000000002" {
				t.Fatalf("marker = %q, want 0000000000000002", got)
			}
		})
	}
}

// A purge from a sender without incarnation IDs purges by name, as it
// always did, and an unmarked directory is purged by an ID-bearing
// purge (it predates markers and belongs to the deleted incarnation or
// an older one).
func TestPurgeLegacyAndUnmarked(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	defer logs.CloseAll()

	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000009", Partitions: 1}
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	purged, err := logs.PurgeTopic("orders", "")
	if err != nil || !purged {
		t.Fatalf("legacy purge = (%v, %v), want (true, nil)", purged, err)
	}
	if _, err := os.Stat(topicDirT(t, dataDir, "orders")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("topic dir after legacy purge: stat err = %v, want not-exist", err)
	}

	// Unmarked directory, purge names an incarnation.
	if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "legacy", 0), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	purged, err = logs.PurgeTopic("legacy", "0000000000000008")
	if err != nil || !purged {
		t.Fatalf("purge of unmarked dir = (%v, %v), want (true, nil)", purged, err)
	}
	if _, err := os.Stat(topicDirT(t, dataDir, "legacy")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unmarked dir after purge: stat err = %v, want not-exist", err)
	}
}

// The delete race from the audit: a purge removes the directory outside
// the log map lock, so an open between CloseTopic and RemoveAll could
// land a log in a directory that is unlinked underneath it (its writes
// go to unlinked inodes). Under the topic guard the two cannot
// interleave: every log Get hands out is either still open with its
// directory present, or was closed by the purge.
func TestPurgeAndGetDoNotInterleave(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	defer logs.CloseAll()
	partitionDir := storage.TopicPartitionDir(dataDir, "orders", 0)

	for i := range 200 {
		if _, err := logs.Get("orders", 0); err != nil {
			t.Fatalf("Get: %v", err)
		}
		var wg sync.WaitGroup
		var got *storage.Log
		var getErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = logs.PurgeTopic("orders", "")
		}()
		go func() {
			defer wg.Done()
			got, getErr = logs.Get("orders", 0)
		}()
		wg.Wait()
		if getErr != nil {
			t.Fatalf("Get: %v", getErr)
		}
		_, err := got.Append([]byte("x"))
		switch {
		case errors.Is(err, storage.ErrLogClosed):
			// Opened before the purge and closed by it: fine.
		case err == nil:
			if _, statErr := os.Stat(partitionDir); statErr != nil {
				t.Fatalf("iteration %d: Get returned an open log whose directory is gone (%v)", i, statErr)
			}
		default:
			t.Fatalf("Append: %v", err)
		}
		_, _ = logs.PurgeTopic("orders", "")
	}
}

// Get on a topic the local metastore no longer knows still refuses,
// with or without an open entry.
func TestGetRefusesDeletedTopicWithOpenEntry(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	logs := NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "dddddddddddddddd", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := logs.Get("orders", 0); !errors.Is(err, errs.ErrTopicNotFound) {
		t.Fatalf("Get after delete error = %v, want ErrTopicNotFound", err)
	}
}

// EnsureTopicIncarnation (the move-install hook) behaves like an open
// without opening a log: it quarantines a stale directory and stamps
// the current incarnation.
func TestEnsureTopicIncarnationQuarantinesStaleDir(t *testing.T) {
	dataDir := t.TempDir()
	ms := newRuntimeFakeMetastore()
	// EnsureTopicIncarnation acts only for the incarnation the local
	// record still carries.
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "ffffffffffffffff", Partitions: 1}
	logs := NewLogs(dataDir, storage.Options{}, ms, nil)
	defer logs.CloseAll()
	if err := storage.WriteTopicIncarnation(topicDirT(t, dataDir, "orders"), "eeeeeeeeeeeeeeee"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	if err := logs.EnsureTopicIncarnation("orders", "ffffffffffffffff"); err != nil {
		t.Fatalf("EnsureTopicIncarnation: %v", err)
	}
	if got := readMarker(t, dataDir, "orders"); got != "ffffffffffffffff" {
		t.Fatalf("marker = %q, want ffffffffffffffff", got)
	}
	if stale := staleDirs(t, dataDir, "orders"); len(stale) != 1 {
		t.Fatalf("stale dirs = %v, want one", stale)
	}
	ok, err := logs.TopicIncarnationMatches("orders", "ffffffffffffffff")
	if err != nil || !ok {
		t.Fatalf("TopicIncarnationMatches(current) = (%v, %v), want (true, nil)", ok, err)
	}
	ok, err = logs.TopicIncarnationMatches("orders", "eeeeeeeeeeeeeeee")
	if err != nil || ok {
		t.Fatalf("TopicIncarnationMatches(old) = (%v, %v), want (false, nil)", ok, err)
	}
}

// EnsureTopicIncarnation acts only for the incarnation the local record
// still carries. Its callers (a move's install, the stale-incarnation
// sweep) read the id before it takes the topic's guard, so the id can
// name an incarnation deleted since, with the name recreated and this
// node serving the successor under the path: preparing the directory
// for the deleted id would set the live successor's directory aside as a
// leftover and stamp the deleted id on a fresh one.
func TestEnsureTopicIncarnationRefusesAnIDTheRecordNoLongerHas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record *topic.Topic
	}{
		{"the record names the successor", &topic.Topic{Name: "orders", ID: "bbbbbbbbbbbbbbbb", Partitions: 1}},
		{"the record is gone", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			ms := newRuntimeFakeMetastore()
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: "bbbbbbbbbbbbbbbb", Partitions: 1}
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
			defer logs.CloseAll()
			l, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			appendOld(t, l, 7, "successor")
			if tc.record == nil {
				delete(ms.topics, "orders")
			}

			err = logs.EnsureTopicIncarnation("orders", "aaaaaaaaaaaaaaaa")
			if !errors.Is(err, ErrStaleTopicIncarnation) {
				t.Errorf("EnsureTopicIncarnation(deleted id) = %v, want ErrStaleTopicIncarnation", err)
			}
			if got := readMarker(t, dataDir, "orders"); got != "bbbbbbbbbbbbbbbb" {
				t.Errorf("marker = %q after the prepare, want the successor's bbbbbbbbbbbbbbbb", got)
			}
			if stale := staleDirs(t, dataDir, "orders"); len(stale) != 0 {
				t.Errorf("the successor's directory was set aside: %v", stale)
			}
			if tc.record != nil {
				l, err := logs.Get("orders", 0)
				if err != nil {
					t.Fatal(err)
				}
				if got := l.NextOffset(); got != 7 {
					t.Errorf("the successor reopens at next offset %d, want 7", got)
				}
			}
		})
	}
}

// Incarnation ids the purge tests below use: the purged topic and the
// same-named topic created after it.
const (
	purgedIncarnation    = "1111111111111111"
	successorIncarnation = "2222222222222222"
)

// wireConsumerState wires an InFlight to committer as
// cmd/narad/serve_wiring.go does, with caps resolved through the
// metastore record (a shard is made only for a topic the replica
// holds). onCommit, when set, runs in place of committer.Commit.
func wireConsumerState(dataDir string, store *metastore.Store, committer *ConsumerOffsetCommitter, onCommit func(name string, p int, off int64)) *consumer.InFlight {
	if onCommit == nil {
		onCommit = committer.Commit
	}
	offsets := consumer.NewInFlight(func(ctx context.Context, name string) (consumer.Caps, error) {
		if _, err := store.GetTopic(ctx, name); err != nil {
			return consumer.Caps{}, err
		}
		return consumer.Caps{MaxInFlight: 1 << 10, MaxAckedAhead: 1 << 10}, nil
	}, onCommit)
	offsets.SetCommittedRecovery(func(name string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, name, p))
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(name string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, name, p))
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	committer.SetAheadSource(offsets.AheadSnapshot)
	offsets.SetDropNotifier(committer.Forget)
	return offsets
}

// persistedFrontier is the frontier a recovery of dir starts from: the
// larger of the two consumer files' frontiers, -1 for none.
func persistedFrontier(dir string) int64 {
	frontier := int64(-1)
	if off, ok, err := storage.ReadConsumerOffset(dir); err == nil && ok {
		frontier = off
	}
	if rec, ok, err := storage.ReadConsumerAhead(dir); err == nil && ok {
		frontier = max(frontier, rec.Committed)
	}
	return frontier
}

// seedAckedIncarnation runs an earlier process of the purged
// incarnation: 30 records, 0..19 acked and persisted, a clean stop.
func seedAckedIncarnation(t *testing.T, store *metastore.Store, dataDir string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: purgedIncarnation, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	committer := manualOffsetCommitter(dataDir)
	offsets := wireConsumerState(dataDir, store, committer, nil)
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l, 30, "purged-incarnation")
	for range 20 {
		res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
		if err != nil || !res.Reserved {
			t.Fatalf("reserve: %+v %v", res, err)
		}
		if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
			t.Fatal(err)
		}
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = logs.CloseAll()
	if got := persistedFrontier(storage.TopicPartitionDir(dataDir, "orders", 0)); got != 19 {
		t.Fatalf("setup: persisted frontier %d, want 19", got)
	}
}

// recreateTopic deletes the purged incarnation's record and creates the
// successor under the same name.
func recreateTopic(t *testing.T, store *metastore.Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: successorIncarnation, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
}

// removeAllWithTick removes a topic directory holding one partition
// directory (partBase) in os.RemoveAll's syscall order: the partition
// directory's entries and the topic marker are unlinked, then tick runs
// (the window), then the two directories are rmdir'ed. os.RemoveAll does
// not list again before the rmdir, so a file created in the window fails
// it with ENOTEMPTY, which it returns.
func removeAllWithTick(t *testing.T, partBase string, tick func()) func(string) error {
	return func(topicDir string) error {
		partDir := filepath.Join(topicDir, partBase)
		entries, err := os.ReadDir(partDir)
		if err != nil {
			t.Errorf("list %s: %v", partDir, err)
		}
		for _, e := range entries {
			if err := os.Remove(filepath.Join(partDir, e.Name())); err != nil {
				t.Errorf("unlink %s: %v", e.Name(), err)
			}
		}
		if err := os.Remove(filepath.Join(topicDir, storage.IncarnationMarkerFileName)); err != nil {
			t.Errorf("unlink marker: %v", err)
		}
		tick()
		if err := syscall.Rmdir(partDir); err != nil {
			return &os.PathError{Op: "unlinkat", Path: partDir, Err: err}
		}
		if err := syscall.Rmdir(topicDir); err != nil {
			return &os.PathError{Op: "unlinkat", Path: topicDir, Err: err}
		}
		return nil
	}
}

// requireSuccessorDeliversFromZero opens the successor's partition,
// appends 30 records and consumes them: every one must be delivered,
// from offset 0, as it is on a node that never held the purged topic.
func requireSuccessorDeliversFromZero(t *testing.T, logs *Logs, offsets *consumer.InFlight, committer *ConsumerOffsetCommitter, partDir string) {
	t.Helper()
	ctx := context.Background()
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm := l.HighWatermark(); hwm != 0 {
		t.Fatalf("the successor's log opens at hwm %d, want 0", hwm)
	}
	appendOld(t, l, 30, "successor")
	var got []int64
	for {
		r, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
		if err != nil {
			t.Fatal(err)
		}
		if !r.Reserved {
			break
		}
		if err := offsets.CommitHandle("orders", 0, r.Offset, r.Nonce); err != nil {
			t.Fatal(err)
		}
		got = append(got, r.Offset)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 30 || got[0] != 0 {
		first := int64(-1)
		if len(got) > 0 {
			first = got[0]
		}
		t.Fatalf("the successor delivered %d of its 30 records, from offset %d; its directory holds frontier %d",
			len(got), first, persistedFrontier(partDir))
	}
}

// A purge sets the topic directory aside before it removes it, so
// nothing written by path during the removal lands where a same-named
// successor opens. The purge's first retire drops the purged
// incarnation's shards, but a consume still holding the purged
// incarnation's log can make the partition's shard again from the
// consumer files at the path, and a commit that reaches the committer
// after the retire (a late ack, or the committer's own requeue of a
// snapshot it was told to forget) makes a tick prime that shard by path.
// A tick inside the removal, after the unlinks and before the rmdir,
// recreated the consumer files in the directory being removed: the
// rmdir failed, and the successor adopted the unmarked leftover with the
// purged frontier and skipped its own first records.
//
// The seams stand for schedules only: the late ack's onCommit is parked,
// or the committer's first tick is parked after its snapshot, and the
// removal runs in os.RemoveAll's syscall order with a tick in its
// window. Everything else is production code.
func TestPurgeLeavesNoLeftoverASuccessorCouldAdopt(t *testing.T) {
	for _, tc := range []struct {
		name string
		// lagging: the purge reaches this node before its replica
		// applied the delete, so caps still resolve for the old record.
		lagging  bool
		requeued bool
	}{
		{name: "a late ack after the successor was created"},
		{name: "a late ack while the replica lags the delete", lagging: true},
		{name: "a commit the committer requeued", requeued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newIncarnationStore(t)
			dataDir := t.TempDir()
			partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
			seedAckedIncarnation(t, store, dataDir)

			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
			defer logs.CloseAll()
			committer := manualOffsetCommitter(dataDir)
			defer func() { _ = committer.Close() }()
			var parkCommit atomic.Bool
			parked := make(chan struct{})
			release := make(chan struct{})
			offsets := wireConsumerState(dataDir, store, committer, func(name string, p int, off int64) {
				if parkCommit.CompareAndSwap(true, false) {
					close(parked)
					<-release
				}
				committer.Commit(name, p, off)
			})
			var parkSnap atomic.Bool
			snapped := make(chan struct{})
			resume := make(chan struct{})
			committer.SetAheadSource(func(name string, p int) (int64, []int64, uint64, bool) {
				committed, offs, version, ok := offsets.AheadSnapshot(name, p)
				if parkSnap.CompareAndSwap(true, false) {
					close(snapped)
					<-resume
				}
				return committed, offs, version, ok
			})
			var retires atomic.Int32
			logs.SetTopicRetiredHook(func(name string) {
				retires.Add(1)
				offsets.DropTopic(name)
			})

			// Consumer A resolved the purged incarnation's log; consumer
			// C holds offset 20.
			oldLog, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			c, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
			if err != nil || !c.Reserved || c.Offset != 20 {
				t.Fatalf("setup: C's reserve %+v %v", c, err)
			}

			var beforeRemoval func()
			if tc.requeued {
				// C acks before the delete; the committer's first tick
				// snapshots the purged shard and is descheduled. Resumed
				// after the first retire's Forget, it requeues the commit.
				if err := offsets.CommitHandle("orders", 0, c.Offset, c.Nonce); err != nil {
					t.Fatal(err)
				}
				recreateTopic(t, store)
				parkSnap.Store(true)
				tick1 := make(chan error, 1)
				go func() { tick1 <- committer.flush() }()
				select {
				case <-snapped:
				case <-time.After(10 * time.Second):
					t.Fatal("setup: the first tick never took a snapshot")
				}
				beforeRemoval = func() {
					close(resume)
					if err := <-tick1; err != nil {
						t.Logf("first tick: %v", err)
					}
				}
			} else {
				if !tc.lagging {
					recreateTopic(t, store)
				}
				// C's ack moves the frontier to 20; its onCommit parks
				// and lands only after the first retire.
				parkCommit.Store(true)
				acked := make(chan error, 1)
				go func() { acked <- offsets.CommitHandle("orders", 0, c.Offset, c.Nonce) }()
				select {
				case <-parked:
				case <-time.After(10 * time.Second):
					t.Fatal("setup: C's onCommit never ran")
				}
				beforeRemoval = func() {
					close(release)
					if err := <-acked; err != nil {
						t.Errorf("setup: C's ack: %v", err)
					}
				}
			}

			removal := removeAllWithTick(t, filepath.Base(partDir), func() {
				if err := committer.flush(); err != nil {
					t.Logf("tick inside the removal: %v", err)
				}
			})
			logs.removeAll = func(dir string) error {
				if n := retires.Load(); n != 1 {
					t.Errorf("setup: the removal started after %d retires, want 1", n)
				}
				beforeRemoval()
				// A reserves on the log it resolved before the purge,
				// making the partition's shard again.
				if r, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark()); err == nil && r.Reserved {
					if _, _, _, rerr := oldLog.ReadKeyedShared(r.Offset); rerr != nil {
						_ = offsets.ReleaseHandle("orders", 0, r.Offset, r.Nonce)
					}
				}
				return removal(dir)
			}

			if _, err := logs.PurgeTopic("orders", purgedIncarnation); err != nil {
				t.Errorf("purge: %v", err)
			}
			entries, err := os.ReadDir(storage.TopicsDir(dataDir))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				t.Errorf("left under topics/ after the purge: %s (frontier %d at the partition path)", e.Name(), persistedFrontier(partDir))
			}
			if tc.lagging {
				recreateTopic(t, store)
			}
			requireSuccessorDeliversFromZero(t, logs, offsets, committer, partDir)
		})
	}
}

// A purge whose removal cannot finish leaves only the copy it set aside,
// never topics/<name>, named so the purge's retry and the orphan sweeps
// can reclaim it: topics/<name>.stale-<id> for a purge that names the
// incarnation, the marker's id for a legacy purge of a marked directory
// (a quarantine of that incarnation), and a random purge- suffix for a
// legacy purge of an unmarked one (a plain directory of no topic). A
// same-named topic created afterwards opens empty.
func TestPurgeThatCannotFinishLeavesOnlyASetAsideCopy(t *testing.T) {
	for _, tc := range []struct {
		name, recordID, purgeID, wantPrefix string
		quarantined                         bool
	}{
		{"a purge naming the incarnation", "0000000000000009", "0000000000000009", "orders.stale-0000000000000009", true},
		{"a legacy purge of a marked directory", "0000000000000009", "", "orders.stale-0000000000000009", true},
		{"a legacy purge of an unmarked directory", "", "", "orders.stale-purge-", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := newRuntimeFakeMetastore()
			dataDir := t.TempDir()
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
			defer logs.CloseAll()
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: tc.recordID, Partitions: 1}
			l, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			appendOld(t, l, 3, "purged")

			var removing string
			logs.removeAll = func(dir string) error {
				removing = dir
				return errors.New("removal interrupted")
			}
			if purged, err := logs.PurgeTopic("orders", tc.purgeID); !purged || err == nil {
				t.Fatalf("purge = (%v, %v), want (true, the removal's error)", purged, err)
			}
			if _, err := os.Stat(topicDirT(t, dataDir, "orders")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("topics/orders after the purge: %v, want it gone", err)
			}
			base := filepath.Base(removing)
			if !strings.HasPrefix(base, tc.wantPrefix) {
				t.Fatalf("the purge removed %s, want a set-aside copy named %s*", base, tc.wantPrefix)
			}
			c, err := classifyTopicDir(storage.TopicsDir(dataDir), base)
			if err != nil {
				t.Fatal(err)
			}
			if c.Quarantined != tc.quarantined {
				t.Fatalf("the leftover classifies as %+v, want quarantined %v", c, tc.quarantined)
			}

			logs.removeAll = os.RemoveAll
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: "000000000000000a", Partitions: 1}
			l2, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			if hwm := l2.HighWatermark(); hwm != 0 {
				t.Fatalf("the recreated topic opens at hwm %d, want 0", hwm)
			}
			if tc.purgeID != "" {
				if _, err := logs.PurgeTopic("orders", tc.purgeID); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(removing); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("the retried purge left %s (%v)", base, err)
				}
			}
		})
	}
}
