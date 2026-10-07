package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func coldTestLogs(t *testing.T, ms *runtimeFakeMetastore) *Logs {
	t.Helper()
	g, _ := coldTestLogsWithMetrics(t, ms, nil)
	return g
}

// ageSegments backdates every segment file of a partition so that, on
// disk and once reopened, its last write looks older than by.
func ageSegments(t *testing.T, g *Logs, topicName string, idx int, by time.Duration) {
	t.Helper()
	dir := topicPartitionDirT(t, g.dataDir, topicName, idx)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	old := time.Now().Add(-by)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
				t.Fatalf("Chtimes: %v", err)
			}
		}
	}
}

func segmentFiles(t *testing.T, g *Logs, topicName string, idx int) []string {
	t.Helper()
	dir := topicPartitionDirT(t, g.dataDir, topicName, idx)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestColdRetentionReapsClosedPartition is the gap the walk exists for: a
// partition whose log is closed (evicted, or never opened since a
// restart) with an expired active segment is rolled and reaped, and left
// closed afterwards.
func TestColdRetentionReapsClosedPartition(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	g := coldTestLogs(t, ms)
	appendAndCommit(t, g, "orders", 0, "a")
	appendAndCommit(t, g, "orders", 0, "b")
	if err := g.ClosePartition("orders", 0); err != nil {
		t.Fatalf("ClosePartition: %v", err)
	}
	before := segmentFiles(t, g, "orders", 0)
	if len(before) != 1 || !strings.HasPrefix(before[0], "00000000000000000000") {
		t.Fatalf("precondition: want one base-0 segment, have %v", before)
	}
	ageSegments(t, g, "orders", 0, 2*time.Hour)

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ColdRetentionOnce: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1", swept)
	}
	after := segmentFiles(t, g, "orders", 0)
	if len(after) != 1 || after[0] != "00000000000000000002.log" {
		t.Fatalf("after the sweep want a single empty active at base 2, have %v", after)
	}
	if _, open := g.Peek("orders", 0); open {
		t.Fatal("the walk left the partition open")
	}
	if n := g.OpenCount(); n != 0 {
		t.Fatalf("OpenCount = %d, want 0", n)
	}
	// The next open sees the durable state intact: offsets continue
	// from 2, nothing restarts at 0.
	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := l.NextOffset(); got != 2 {
		t.Fatalf("NextOffset after cold reap = %d, want 2", got)
	}
}

// TestColdRetentionLeavesEverythingElseAlone pins the skips: an open log
// (the shared reaper's), a keep-forever topic, a partition that is not
// due yet, and a directory the metastore no longer knows.
func TestColdRetentionLeavesEverythingElseAlone(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	hour := int64(time.Hour / time.Millisecond)
	ms.topics["open"] = topic.Topic{Name: "open", Partitions: 1, RetentionMs: hour}
	ms.topics["forever"] = topic.Topic{Name: "forever", Partitions: 1, RetentionMs: 0}
	ms.topics["fresh"] = topic.Topic{Name: "fresh", Partitions: 1, RetentionMs: hour}
	ms.topics["gone"] = topic.Topic{Name: "gone", Partitions: 1, RetentionMs: hour}
	g := coldTestLogs(t, ms)
	for _, name := range []string{"open", "forever", "fresh", "gone"} {
		appendAndCommit(t, g, name, 0, "x")
	}
	for _, name := range []string{"forever", "fresh", "gone"} {
		if err := g.ClosePartition(name, 0); err != nil {
			t.Fatalf("ClosePartition(%s): %v", name, err)
		}
	}
	ageSegments(t, g, "open", 0, 2*time.Hour)
	ageSegments(t, g, "forever", 0, 2*time.Hour)
	ageSegments(t, g, "gone", 0, 2*time.Hour)
	delete(ms.topics, "gone")

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ColdRetentionOnce: %v", err)
	}
	if swept != 0 {
		t.Fatalf("swept = %d, want 0", swept)
	}
	if _, open := g.Peek("open", 0); !open {
		t.Fatal("the walk closed a log that was open")
	}
	for _, name := range []string{"open", "forever", "fresh", "gone"} {
		files := segmentFiles(t, g, name, 0)
		if len(files) != 1 || !strings.HasPrefix(files[0], "00000000000000000000") {
			t.Fatalf("%s: segments changed: %v", name, files)
		}
	}
	if n := g.OpenCount(); n != 1 {
		t.Fatalf("OpenCount = %d, want 1 (only the open log)", n)
	}
}

// coldTestLogsWithMetrics is coldTestLogs with a metrics registry, for
// the counter assertions.
func coldTestLogsWithMetrics(t *testing.T, ms *runtimeFakeMetastore, m *metrics.Metrics) (*Logs, *metrics.Metrics) {
	t.Helper()
	g := NewLogs(t.TempDir(), storage.Options{
		FlushInterval: 5 * time.Millisecond,
		Retention:     storage.RetentionConfig{CheckInterval: time.Minute},
	}, ms, m)
	t.Cleanup(func() { _ = g.CloseAll() })
	return g, m
}

// dueClosedPartition writes a record, closes the log and backdates the
// segment so the partition is due on the next walk.
func dueClosedPartition(t *testing.T, g *Logs, topicName string) {
	t.Helper()
	appendAndCommit(t, g, topicName, 0, "x")
	if err := g.ClosePartition(topicName, 0); err != nil {
		t.Fatalf("ClosePartition: %v", err)
	}
	ageSegments(t, g, topicName, 0, 2*time.Hour)
}

// TestColdRetentionWithoutTopicsDirIsNoOp pins the fresh-node case: a
// data dir that has never held a topic has no topics root, and the walk
// reports nothing rather than an error every interval.
func TestColdRetentionWithoutTopicsDirIsNoOp(t *testing.T) {
	g := coldTestLogs(t, newRuntimeFakeMetastore())
	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil || swept != 0 {
		t.Fatalf("ColdRetentionOnce on an empty data dir = %d, %v; want 0, nil", swept, err)
	}
}

// TestColdRetentionReportsUnreadableTopicsRoot pins the one ReadDir
// error the walk surfaces: a topics root that cannot be listed (here, a
// file in its place) is returned to the caller, who logs it, rather than
// swallowed as "nothing to do".
func TestColdRetentionReportsUnreadableTopicsRoot(t *testing.T) {
	g := coldTestLogs(t, newRuntimeFakeMetastore())
	if err := os.WriteFile(filepath.Join(g.dataDir, "topics"), []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := g.ColdRetentionOnce(context.Background(), time.Now()); err == nil {
		t.Fatal("ColdRetentionOnce returned nil for a topics root that cannot be listed")
	}
}

// TestColdRetentionStopsOnCancelledContext pins the shutdown path: a
// walk whose context is done returns the context's error before opening
// anything, so a stopping node does not reap partitions on its way out.
func TestColdRetentionStopsOnCancelledContext(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	g := coldTestLogs(t, ms)
	dueClosedPartition(t, g, "orders")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	swept, err := g.ColdRetentionOnce(ctx, time.Now())
	if swept != 0 || err == nil || ctx.Err() == nil || err.Error() != ctx.Err().Error() {
		t.Fatalf("ColdRetentionOnce(cancelled) = %d, %v; want 0, %v", swept, err, ctx.Err())
	}
	if files := segmentFiles(t, g, "orders", 0); len(files) != 1 || !strings.HasPrefix(files[0], "00000000000000000000") {
		t.Fatalf("a cancelled walk touched the partition: %v", files)
	}
}

// TestColdRetentionSkipsDirectoriesThatAreNotTopics pins the entries the
// walk must step around in the topics root and inside a topic: a stray
// file, a quarantined incarnation (the orphan sweep's, not ours), a
// directory whose incarnation marker cannot be read, and a partition
// directory whose name does not parse.
func TestColdRetentionSkipsDirectoriesThatAreNotTopics(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	hour := int64(time.Hour / time.Millisecond)
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: hour}
	ms.topics["broken"] = topic.Topic{Name: "broken", Partitions: 1, RetentionMs: hour}
	g := coldTestLogs(t, ms)
	dueClosedPartition(t, g, "orders")
	root := filepath.Join(g.dataDir, "topics")

	// A file in the topics root.
	if err := os.WriteFile(filepath.Join(root, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// A quarantined copy of a due partition: name and marker agree.
	stale := filepath.Join(root, "orders"+storage.StaleTopicDirSuffix+"old")
	if err := os.MkdirAll(filepath.Join(stale, "p00000"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := storage.WriteTopicIncarnation(stale, "old"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	// A topic whose marker is unreadable (a directory in its place).
	if err := os.MkdirAll(filepath.Join(root, "broken", storage.IncarnationMarkerFileName), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// Non-partition entries inside a topic directory.
	if err := os.MkdirAll(filepath.Join(root, "orders", "pX"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "orders", "p00001"), []byte("a file, not a partition"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ColdRetentionOnce: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1 (only orders/p00000 is a due partition)", swept)
	}
	if n := g.OpenCount(); n != 0 {
		t.Fatalf("OpenCount = %d, want 0", n)
	}
}

// TestColdRetentionSkipsTopicWhenLookupFails pins the lookup error path:
// a metastore that cannot answer (as opposed to one that says the topic
// is gone) is not grounds to open and reap anything; the topic is left
// for the next walk.
func TestColdRetentionSkipsTopicWhenLookupFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	g := coldTestLogs(t, ms)
	dueClosedPartition(t, g, "orders")
	ms.getTopicErr = errors.New("metastore unavailable")

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil || swept != 0 {
		t.Fatalf("ColdRetentionOnce with a failing lookup = %d, %v; want 0, nil", swept, err)
	}
	if files := segmentFiles(t, g, "orders", 0); len(files) != 1 || !strings.HasPrefix(files[0], "00000000000000000000") {
		t.Fatalf("a topic with a failed lookup was reaped: %v", files)
	}
}

// TestColdRetentionUsesStorageMaxAgeWithoutMetastore pins the embedded
// case: with no metastore the walk takes the age bound from the storage
// options, the same as the lazy-open path does.
func TestColdRetentionUsesStorageMaxAgeWithoutMetastore(t *testing.T) {
	g := NewLogs(t.TempDir(), storage.Options{
		FlushInterval: 5 * time.Millisecond,
		Retention:     storage.RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Minute},
	}, nil, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	dueClosedPartition(t, g, "orders")

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil || swept != 1 {
		t.Fatalf("ColdRetentionOnce without a metastore = %d, %v; want 1, nil", swept, err)
	}
	if files := segmentFiles(t, g, "orders", 0); len(files) != 1 || files[0] != "00000000000000000001.log" {
		t.Fatalf("after the sweep want a single empty active at base 1, have %v", files)
	}
}

// TestColdRetentionReportsUnreadablePartitionDir pins the stat error
// path: a partition directory that cannot be listed stops that topic's
// walk with the error, rather than being counted as "nothing due".
func TestColdRetentionReportsUnreadablePartitionDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	g := coldTestLogs(t, ms)
	dueClosedPartition(t, g, "orders")
	dir := topicPartitionDirT(t, g.dataDir, "orders", 0)
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err == nil || swept != 0 {
		t.Fatalf("ColdRetentionOnce with an unreadable partition = %d, %v; want 0 and an error", swept, err)
	}
}

// TestColdPartitionDue pins the on-disk test for "the reaper would
// remove something": nothing on disk, nothing older than the cutoff, and
// an empty lone active segment are all not due; an old sealed segment or
// an old non-empty active one is.
func TestColdPartitionDue(t *testing.T) {
	cutoff := time.Unix(1_000_000, 0)
	old, fresh := cutoff.Unix()-1, cutoff.Unix()+1
	for name, tc := range map[string]struct {
		st   coldStat
		want bool
	}{
		"no segments":          {coldStat{}, false},
		"no mtime":             {coldStat{segments: 2, sizeBytes: 10}, false},
		"fresh":                {coldStat{segments: 2, sizeBytes: 10, oldestSegmentAt: fresh}, false},
		"at cutoff":            {coldStat{segments: 2, sizeBytes: 10, oldestSegmentAt: cutoff.Unix()}, false},
		"old empty active":     {coldStat{segments: 1, sizeBytes: 0, oldestSegmentAt: old}, false},
		"old non-empty active": {coldStat{segments: 1, sizeBytes: 10, oldestSegmentAt: old}, true},
		"old sealed":           {coldStat{segments: 2, sizeBytes: 0, oldestSegmentAt: old}, true},
	} {
		if got := coldPartitionDue(tc.st, cutoff); got != tc.want {
			t.Errorf("%s: coldPartitionDue(%+v) = %v, want %v", name, tc.st, got, tc.want)
		}
	}
}

// TestRunColdRetentionTicksAndStops pins the loop: a non-positive
// interval disables it and returns at once, a positive one walks on
// every tick (reaping what is due and counting it), and cancelling the
// context ends the loop.
func TestRunColdRetentionTicksAndStops(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: int64(time.Hour / time.Millisecond)}
	g, m := coldTestLogsWithMetrics(t, ms, metrics.New(prometheus.NewRegistry()))
	dueClosedPartition(t, g, "orders")

	done := make(chan struct{})
	go func() {
		g.RunColdRetention(context.Background(), 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunColdRetention with a zero interval did not return")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan struct{})
	go func() {
		g.RunColdRetention(ctx, 20*time.Millisecond)
		close(done)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for testutil.ToFloat64(m.ColdRetentionSweptTotal) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the loop never swept the due partition")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunColdRetention did not return after its context was cancelled")
	}
	if files := segmentFiles(t, g, "orders", 0); len(files) != 1 || files[0] != "00000000000000000001.log" {
		t.Fatalf("after the loop's sweep want a single empty active at base 1, have %v", files)
	}
	if _, open := g.Peek("orders", 0); open {
		t.Fatal("the loop left the partition open")
	}
}

// panicOnceRecorder is a storage.MetricsRecorder whose first
// ObserveRetentionRun, the deferred call that ends every retention
// sweep, panics: one partition's sweep panicking.
type panicOnceRecorder struct {
	armed atomic.Bool
	runs  atomic.Int64
}

func (*panicOnceRecorder) ObserveFlush(time.Duration, int64)                 {}
func (*panicOnceRecorder) ObserveFsync(time.Duration)                        {}
func (*panicOnceRecorder) ObserveHighWatermarkPersist(time.Duration, string) {}
func (*panicOnceRecorder) IncRetentionDeletion(string, int64, int64)         {}

func (r *panicOnceRecorder) ObserveRetentionRun(time.Duration) {
	r.runs.Add(1)
	if r.armed.CompareAndSwap(true, false) {
		panic("injected sweep panic")
	}
}

// A panic in one partition's sweep is contained: the walk logs and
// counts it, closes the log it opened, leaves that partition alone for a
// while, and still sweeps the next partition. The walk runs on its own
// goroutine with nothing above it to recover, so an escaped panic took
// the whole node down, and a deterministic one did it on every restart.
func TestColdWalkSurvivesAPanicInOnePartition(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, RetentionMs: int64(time.Hour / time.Millisecond)}
	rec := &panicOnceRecorder{}
	g := NewLogs(t.TempDir(), storage.Options{
		FlushInterval: 5 * time.Millisecond,
		Retention:     storage.RetentionConfig{CheckInterval: time.Minute},
		Metrics:       rec,
	}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	for p := range 2 {
		appendAndCommit(t, g, "orders", p, "x")
		if err := g.ClosePartition("orders", p); err != nil {
			t.Fatalf("ClosePartition(%d): %v", p, err)
		}
		ageSegments(t, g, "orders", p, 2*time.Hour)
	}
	rec.armed.Store(true)

	var escaped any
	var swept int
	var err error
	func() {
		defer func() { escaped = recover() }()
		swept, err = g.ColdRetentionOnce(context.Background(), time.Now())
	}()
	if escaped != nil {
		t.Fatalf("a sweep panic escaped the cold walk, which would crash the node: %v", escaped)
	}
	if err == nil {
		t.Fatal("the walk reported no error for a partition whose sweep panicked")
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1: the partition after the panicking one must still be swept", swept)
	}
	if got := testutil.ToFloat64(g.ColdRetentionPanics()); got != 1 {
		t.Fatalf("narad_cold_retention_panics_total = %v, want 1", got)
	}
	if n := g.OpenCount(); n != 0 {
		t.Fatalf("OpenCount = %d after a contained panic: the walk-owned log was left open", n)
	}
	if !g.coldDeferred(keyOf("orders", 0), time.Now()) {
		t.Fatal("the partition whose sweep panicked is not left alone for a while")
	}
	runs := rec.runs.Load()
	if _, err := g.ColdRetentionOnce(context.Background(), time.Now()); err != nil {
		t.Fatalf("second walk: %v", err)
	}
	if got := rec.runs.Load(); got != runs {
		t.Fatalf("the next walk swept the panicking partition again at once (%d sweeps, want %d)", got, runs)
	}
}

// A due copy of a partition another node owns (a move's source awaiting
// its reclaim, say) is not this node's to open or reap: the walk sweeps
// only the partition the local assignment gives this node.
func TestColdWalkSkipsPartitionsOwnedElsewhere(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, RetentionMs: int64(time.Hour / time.Millisecond)}
	ms.setAssignment("orders", 0, "node-self")
	ms.setAssignment("orders", 1, "node-other")
	g := coldTestLogs(t, ms)
	g.SetOwnership(func(topicName string, idx int) bool {
		a, err := ms.GetAssignment(topicName, idx)
		return err == nil && a.OwnerID == "node-self"
	})
	for p := range 2 {
		appendAndCommit(t, g, "orders", p, "x")
		if err := g.ClosePartition("orders", p); err != nil {
			t.Fatalf("ClosePartition(%d): %v", p, err)
		}
		ageSegments(t, g, "orders", p, 2*time.Hour)
	}
	foreign := segmentFiles(t, g, "orders", 1)

	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ColdRetentionOnce: %v", err)
	}
	if swept != 1 {
		t.Fatalf("swept = %d, want 1 (the owned partition only)", swept)
	}
	if got := segmentFiles(t, g, "orders", 1); !slices.Equal(got, foreign) {
		t.Fatalf("the walk changed a partition node-other owns: %v -> %v", foreign, got)
	}
}

// guardWaiters reports how many callers hold or wait on the topic's
// guard.
func guardWaiters(g *Logs, topicName string) int {
	g.guardMu.Lock()
	defer g.guardMu.Unlock()
	if tg := g.guards[topicName]; tg != nil {
		return tg.refs
	}
	return 0
}

// The walk stats a partition before it takes the topic's guard. A
// reclaim that removes the directory in between, under that guard, must
// not see the walk's open recreate it as an empty partition at offset
// zero next to wherever the data went.
func TestColdWalkNeverRecreatesAReclaimedPartition(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "inc-1", Partitions: 2, RetentionMs: int64(time.Hour / time.Millisecond)}
	g := coldTestLogs(t, ms)
	appendAndCommit(t, g, "orders", 1, "x")
	if err := g.ClosePartition("orders", 1); err != nil {
		t.Fatalf("ClosePartition: %v", err)
	}
	ageSegments(t, g, "orders", 1, 2*time.Hour)
	dir := topicPartitionDirT(t, g.DataDir(), "orders", 1)

	inGuard := make(chan struct{})
	reclaimed := make(chan error, 1)
	go func() {
		// A reclaim's shape: remove the directory while holding the
		// partition's produce mutex and the topic's guard, once the walk
		// waits on that guard.
		reclaimed <- g.ReplacePartitionDir("orders", 1, func() error {
			close(inGuard)
			deadline := time.Now().Add(5 * time.Second)
			for guardWaiters(g, "orders") < 2 {
				if time.Now().After(deadline) {
					return errors.New("the walk never waited on the topic guard")
				}
				time.Sleep(time.Millisecond)
			}
			return os.RemoveAll(dir)
		})
	}()
	<-inGuard
	swept, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if rerr := <-reclaimed; rerr != nil {
		t.Fatalf("reclaim: %v", rerr)
	}
	if err != nil || swept != 0 {
		t.Fatalf("walk swept=%d err=%v, want 0, nil", swept, err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the walk recreated %s after the reclaim removed it (stat err %v)", dir, err)
	}
	if n := g.OpenCount(); n != 0 {
		t.Fatalf("OpenCount = %d, want 0", n)
	}
}
