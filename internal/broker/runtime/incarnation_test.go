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
	"testing"
	"time"

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
	id, ok, err := storage.ReadTopicIncarnation(storage.TopicDir(dataDir, name))
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
	if _, err := os.Stat(storage.TopicDir(dataDir, "orders")); !errors.Is(err, os.ErrNotExist) {
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
	if _, err := os.Stat(storage.TopicDir(dataDir, "legacy")); !errors.Is(err, os.ErrNotExist) {
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
	logs := NewLogs(dataDir, storage.Options{}, newRuntimeFakeMetastore(), nil)
	defer logs.CloseAll()
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "orders"), "eeeeeeeeeeeeeeee"); err != nil {
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
