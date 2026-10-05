package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// memSink is a raft.SnapshotSink that keeps the image in memory.
type memSink struct {
	bytes.Buffer
	closed, cancelled bool
}

func (s *memSink) ID() string    { return "mem" }
func (s *memSink) Close() error  { s.closed = true; return nil }
func (s *memSink) Cancel() error { s.cancelled = true; return nil }

// discardSink is a raft.SnapshotSink that counts the image and keeps
// none of it.
type discardSink struct {
	n                 int64
	closed, cancelled bool
}

func (s *discardSink) Write(p []byte) (int, error) { s.n += int64(len(p)); return len(p), nil }
func (s *discardSink) ID() string                  { return "discard" }
func (s *discardSink) Close() error                { s.closed = true; return nil }
func (s *discardSink) Cancel() error               { s.cancelled = true; return nil }

// failingSink refuses every write.
type failingSink struct{ discardSink }

func (s *failingSink) Write([]byte) (int, error) {
	return 0, errors.New("sink: no space left on device")
}

// snapshotImage takes a snapshot of f the way Raft does (Snapshot,
// Persist, Release) and returns the image.
func snapshotImage(t *testing.T, f *fsmState) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Release()
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed || sink.cancelled {
		t.Fatalf("Persist left the sink closed=%v cancelled=%v, want closed only", sink.closed, sink.cancelled)
	}
	return sink.Bytes()
}

// growDatabase applies schema versions of about 256 KiB each to one
// topic until fsm.db holds about mib MiB. Sync is off while it runs, so
// the setup takes a moment rather than one disk flush per entry.
func growDatabase(t *testing.T, f *fsmState, mib int) {
	t.Helper()
	f.db.NoSync = true
	defer func() { f.db.NoSync = false }()
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "big", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"description":"` + strings.Repeat("y", 256<<10) + `"}`)
	for v := 1; v <= mib*4; v++ {
		if err := applyEntry(t, f, uint64(1+v), opPutSchema, schemaPayload{Topic: "big", Version: v, Schema: body}); err != nil {
			t.Fatalf("schema v%d: %v", v, err)
		}
	}
}

// bytesAllocated is how much fn allocated on the heap, whatever it
// freed again: a copy of the database held only for a moment counts.
func bytesAllocated(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// streamBound is how much a snapshot or a restore may allocate,
// whatever the size of the database: both stream it.
const streamBound = 4 << 20

func snapshotCopies(t *testing.T, f *fsmState) []string {
	t.Helper()
	copies, err := filepath.Glob(f.dbPath + ".snapshot-*")
	if err != nil {
		t.Fatal(err)
	}
	return copies
}

func TestSnapshotDoesNotHoldTheDatabaseInMemory(t *testing.T) {
	f := newTestFSM(t)
	growDatabase(t, f, 24)

	sink := &discardSink{}
	var persistErr error
	allocated := bytesAllocated(func() {
		snap, err := f.Snapshot()
		if err != nil {
			persistErr = err
			return
		}
		persistErr = snap.Persist(sink)
		snap.Release()
	})
	if persistErr != nil {
		t.Fatal(persistErr)
	}
	if sink.n < 20<<20 {
		t.Fatalf("the image is %d bytes, want the whole database (about 24 MiB)", sink.n)
	}
	t.Logf("snapshot of %d MiB allocated %d KiB", sink.n>>20, allocated>>10)
	if allocated > streamBound {
		t.Fatalf("Snapshot and Persist of a %d MiB database allocated %d MiB; want under %d MiB: the image is streamed, not held in memory",
			sink.n>>20, allocated>>20, streamBound>>20)
	}
}

func TestRestoreStreamsTheImageToDisk(t *testing.T) {
	src := newTestFSM(t)
	growDatabase(t, src, 24)
	image := filepath.Join(t.TempDir(), "image.db")
	if err := src.db.View(func(tx *bolt.Tx) error { return tx.CopyFile(image, 0o600) }); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(image)
	if err != nil {
		t.Fatal(err)
	}

	dst := newTestFSM(t)
	in, err := os.Open(image)
	if err != nil {
		t.Fatal(err)
	}
	var restoreErr error
	allocated := bytesAllocated(func() { restoreErr = dst.Restore(in) })
	if restoreErr != nil {
		t.Fatal(restoreErr)
	}
	t.Logf("restore of %d MiB allocated %d KiB", st.Size()>>20, allocated>>10)
	if allocated > streamBound {
		t.Fatalf("Restore of a %d MiB image allocated %d MiB; want under %d MiB: the image is streamed to disk, not read into memory",
			st.Size()>>20, allocated>>20, streamBound>>20)
	}
	if _, found := fsmSchema(t, dst, "big", 96); !found {
		t.Fatal("the restored database is missing the image's records")
	}
	if _, err := os.Stat(dst.dbPath + ".restore"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the restore left %s.restore behind (stat: %v)", dst.dbPath, err)
	}
}

// Raft persists a snapshot on its own goroutine while the FSM goes on
// applying. A read transaction left open until Persist would make any
// commit that grows bbolt's memory map wait for it.
func TestSnapshotHoldsNoReadTransactionAfterItReturns(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Far larger than the file, so its commit has to grow the map.
	grow := encodeEntry(t, opPutSchema, schemaPayload{Topic: "orders", Version: 1, Schema: []byte(`"` + strings.Repeat("z", 4<<20) + `"`)})
	done := make(chan error, 1)
	go func() { done <- applyRaw(f, 2, grow) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("apply after the snapshot: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an apply that grows the database waited for a snapshot that Raft had not persisted yet")
	}

	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	g := newTestFSM(t)
	if err := g.Restore(io.NopCloser(&sink.Buffer)); err != nil {
		t.Fatal(err)
	}
	if !hasTopic(t, g, "orders") {
		t.Fatal("the image lacks what was applied before the snapshot")
	}
	if _, found := fsmSchema(t, g, "orders", 1); found {
		t.Fatal("the image holds an entry applied after the snapshot")
	}
}

func TestSnapshotTempFileIsRemoved(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if copies := snapshotCopies(t, f); len(copies) != 1 {
		t.Fatalf("Snapshot left %d copies of the database beside it (%v), want 1", len(copies), copies)
	}
	if err := snap.Persist(&discardSink{}); err != nil {
		t.Fatal(err)
	}
	snap.Release()
	if copies := snapshotCopies(t, f); len(copies) != 0 {
		t.Fatalf("Release left %v behind", copies)
	}

	// A crash between Snapshot and Release, or inside a Restore, leaves
	// files behind; the next open removes them, and only them.
	path := filepath.Join(t.TempDir(), "fsm.db")
	leftovers := []string{path + ".snapshot-1234567", path + ".restore"}
	for _, p := range append([]string{path + ".stale"}, leftovers...) {
		if err := os.WriteFile(p, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	g, err := newFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	defer g.db.Close()
	for _, p := range leftovers {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("open left %s behind (stat: %v)", filepath.Base(p), err)
		}
	}
	if _, err := os.Stat(path + ".stale"); err != nil {
		t.Errorf("open removed fsm.db.stale, which the operator deletes: %v", err)
	}
}

// A snapshot that cannot be written is an error Raft logs and retries
// at its next interval; it is counted, and leaves nothing behind.
func TestSnapshotFailureIsCountedAndLeavesNothingBehind(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &failingSink{}
	if err := snap.Persist(sink); err == nil {
		t.Fatal("Persist into a sink that refuses writes succeeded")
	}
	if !sink.cancelled {
		t.Fatal("a failed Persist did not cancel the sink")
	}
	snap.Release()
	if copies := snapshotCopies(t, f); len(copies) != 0 {
		t.Fatalf("a failed snapshot left %v behind", copies)
	}
	if got := f.snapshotFailures.Load(); got != 1 {
		t.Fatalf("snapshot failures = %d after a failed Persist, want 1", got)
	}

	// No room for the copy: the directory refuses new files.
	dir := filepath.Dir(f.dbPath)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if snap, err := f.Snapshot(); err == nil {
		snap.Release()
		t.Fatal("Snapshot succeeded with nowhere to copy the database")
	} else if !strings.Contains(err.Error(), f.dbPath) {
		t.Fatalf("Snapshot error %q does not name the database", err)
	}
	if got := f.snapshotFailures.Load(); got != 2 {
		t.Fatalf("snapshot failures = %d after a failed copy, want 2", got)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if copies := snapshotCopies(t, f); len(copies) != 0 {
		t.Fatalf("a failed copy left %v behind", copies)
	}
}

func TestFSMSnapshotRestoreRoundTrip(t *testing.T) {
	source, err := newFSM(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("newFSM(source): %v", err)
	}
	defer source.db.Close()

	data, err := json.Marshal(topic.Topic{Name: "orders", Partitions: 2})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := source.applyCreateTopic(data); err != nil {
		t.Fatalf("applyCreateTopic: %v", err)
	}

	snapData := snapshotImage(t, source)

	target, err := newFSM(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("newFSM(target): %v", err)
	}
	defer func() { target.db.Close() }()

	versionBefore := target.metadataVersion()
	if err := target.Restore(io.NopCloser(bytes.NewReader(snapData))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := target.metadataVersion(); got <= versionBefore {
		t.Fatalf("metadataVersion after restore = %d, want > %d", got, versionBefore)
	}

	// The restored db must be open and contain the snapshotted topic.
	err = target.view(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketTopics).Get([]byte("orders")) == nil {
			return errors.New("topic missing after restore")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view after restore: %v", err)
	}
}

// A follower that missed entries the leader has already compacted
// away catches up through InstallSnapshot: the leader streams its
// snapshot file and the follower's FSM streams it into fsm.db.
func TestFollowerCatchesUpThroughAStreamedSnapshot(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	ids := []string{"ss-1", "ss-2", "ss-3"}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	raftLogs := make([]*lockedBuffer, len(ids))
	cfgs := make([]Config, len(ids))
	for i := range ids {
		var peers []Peer
		for j := range ids {
			if i != j {
				peers = append(peers, Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		raftLogs[i] = &lockedBuffer{}
		cfgs[i] = Config{
			NodeID: ids[i], DataDir: filepath.Join(base, ids[i]), BindAddr: addrs[i], AdvertiseAddr: addrs[i], Peers: peers,
			Logger:            raftLogs[i],
			SnapshotThreshold: 4, SnapshotInterval: 50 * time.Millisecond, TrailingLogs: 2,
		}
	}
	stores := make([]*Store, len(ids))
	t.Cleanup(func() {
		for _, s := range stores {
			if s != nil {
				_ = s.Close()
			}
		}
	})
	for i := range ids {
		s, err := New(cfgs[i])
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores[i] = s
	}
	leader := -1
	waitUntil(t, 15*time.Second, "a leader", func() bool {
		for i, s := range stores {
			if s.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	})
	if err := stores[leader].CreateTopic(ctx, topic.Topic{Name: "before", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	victim := (leader + 1) % len(ids)
	waitUntil(t, 10*time.Second, "the victim to apply the first write", func() bool {
		return stores[victim].AppliedIndex() >= stores[leader].AppliedIndex()
	})
	victimLog := stores[victim].r.LastIndex()
	if err := stores[victim].Close(); err != nil {
		t.Fatal(err)
	}
	stores[victim] = nil

	for i := range 20 {
		if err := stores[leader].CreateTopic(ctx, topic.Topic{Name: fmt.Sprintf("after-%d", i), Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 15*time.Second, "the leader to compact its log past what the victim holds", func() bool {
		first, err := stores[leader].logs.FirstIndex()
		return err == nil && first > victimLog+1
	})

	s, err := New(cfgs[victim])
	if err != nil {
		t.Fatalf("reopen %s: %v", ids[victim], err)
	}
	stores[victim] = s
	waitUntil(t, 20*time.Second, "the victim to hold every topic", func() bool {
		_, err := s.GetTopic(ctx, "after-19")
		return err == nil
	})
	for _, name := range []string{"before", "after-0", "after-10"} {
		if _, err := s.GetTopic(ctx, name); err != nil {
			t.Fatalf("victim lacks %s after catching up: %v", name, err)
		}
	}
	// Raft logs the install once the FSM has restored the image, so a
	// moment after the topics show.
	waitUntil(t, 10*time.Second, "the victim to log that it installed the leader's snapshot", func() bool {
		raftLogs[victim].mu.Lock()
		defer raftLogs[victim].mu.Unlock()
		return strings.Contains(raftLogs[victim].buf.String(), "Installed remote snapshot")
	})

	for i, st := range stores {
		_ = st.Close()
		stores[i] = nil
	}
	for i := range ids {
		left, _ := filepath.Glob(filepath.Join(cfgs[i].DataDir, "fsm.db.*"))
		for _, p := range left {
			if !strings.HasSuffix(p, ".stale") {
				t.Errorf("%s: %s left behind", ids[i], filepath.Base(p))
			}
		}
	}
}
