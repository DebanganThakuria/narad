package cluster

// The partition mover copies a source owner's segments into a staging
// dir and must reproduce the partition exactly: same offsets, same HWM,
// same committed consumer offset, byte-identical records. The fake
// fetcher serves real segment bytes off disk (the same reads the RPC
// serve side performs), so this exercises the mover's copy + tail-
// termination + verify against genuine storage.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// dirFetcher serves a partition directory as a segmentFetcher, exactly
// as the owning node's RPC handlers do.
type dirFetcher struct {
	dir          string
	hwm          int64
	committed    int64
	hasCommitted bool
}

func (d dirFetcher) ListPartitionSegments(_ context.Context, _, _ string, _ int) (messaging.PartitionTransferInfo, error) {
	segs, err := storage.ListPartitionSegments(d.dir)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	return messaging.PartitionTransferInfo{
		Segments:        segs,
		HighWatermark:   d.hwm,
		CommittedOffset: d.committed,
		HasCommitted:    d.hasCommitted,
	}, nil
}

func (d dirFetcher) FetchSegmentChunk(_ context.Context, _, _ string, _ int, base, at, length int64) ([]byte, error) {
	return storage.ReadSegmentRange(d.dir, base, at, length)
}

func buildSourcePartition(t *testing.T, dir string, n int) (int64, map[int64][]byte) {
	t.Helper()
	log, err := storage.NewLog(dir, storage.Options{FlushInterval: time.Millisecond, SegmentBytes: 1})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	payloads := map[int64][]byte{}
	for i := range n {
		p := []byte{byte('a' + i%26), byte('0' + i%10), byte('!' + i%15)}
		off, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), p))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := log.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		payloads[off] = p
	}
	next := log.NextOffset()
	if err := log.AdvanceHighWatermark(next); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return next, payloads
}

func TestPartitionMoverCopiesIdentically(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 25)

	fetcher := dirFetcher{dir: src, hwm: wantHWM, committed: 9, hasCommitted: true}
	mover := NewPartitionMover(fetcher, 7, nil) // tiny chunks exercise the streaming loop

	staging := filepath.Join(t.TempDir(), "staging")
	res, err := mover.Copy(context.Background(), "source-addr", "orders", 0, staging)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if res.HighWatermark != wantHWM {
		t.Fatalf("copy HWM = %d, want %d", res.HighWatermark, wantHWM)
	}
	if !res.HasCommitted || res.CommittedOffset != 9 {
		t.Fatalf("committed = %d (has %v), want 9", res.CommittedOffset, res.HasCommitted)
	}
	if res.BytesCopied <= 0 {
		t.Fatalf("copied 0 bytes")
	}

	// The staged copy must recover into an identical log.
	log, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("recover staging: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("staged NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	if log.HighWatermark() != wantHWM {
		t.Fatalf("staged HWM = %d, want %d", log.HighWatermark(), wantHWM)
	}
	for off, want := range payloads {
		_, _, got, err := log.ReadKeyed(off)
		if err != nil {
			t.Fatalf("staged ReadKeyed(%d): %v", off, err)
		}
		if string(got) != string(want) {
			t.Fatalf("staged offset %d = %q, want %q", off, got, want)
		}
	}
	// The committed consumer offset moved with the partition.
	committed, ok, err := storage.ReadConsumerOffset(staging)
	if err != nil || !ok || committed != 9 {
		t.Fatalf("staged consumer offset = %d (ok %v, err %v), want 9", committed, ok, err)
	}
}

// A source that lists its file sizes (a release before committed
// listings) can list a hidden tail: records a commit wrote and never
// made visible. The copy is promoted at the source's high watermark, so
// those records are cut from it: left in place, the new owner's first
// commit would expose them next to the ingress WAL's own re-commit of
// the same records.
func TestFinalizeCutsAStagedTailPastTheHighWatermark(t *testing.T) {
	t.Run("hidden frames in the active segment", func(t *testing.T) {
		src := t.TempDir()
		l := sourceWithHiddenFrames(t, src, 10, 3)
		want := sourceRecords(t, l)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		mover := NewPartitionMover(&statSizeSource{dir: src, hwm: 10}, 16, nil)
		staging := filepath.Join(t.TempDir(), "staging")
		res, err := mover.Copy(context.Background(), "a", "orders", 0, staging)
		if err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if res.HighWatermark != 10 {
			t.Fatalf("copy HWM = %d, want 10", res.HighWatermark)
		}
		requireStagedRecords(t, staging, 10, want)
	})
	t.Run("hidden segments", func(t *testing.T) {
		src := t.TempDir()
		recordCount, _ := buildSourcePartition(t, src, 10) // one record per segment
		hiddenHWM := recordCount - 3
		mover := NewPartitionMover(dirFetcher{dir: src, hwm: hiddenHWM}, 16, nil)
		staging := filepath.Join(t.TempDir(), "staging")
		res, err := mover.Copy(context.Background(), "a", "orders", 0, staging)
		if err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if res.HighWatermark != hiddenHWM {
			t.Fatalf("copy HWM = %d, want %d", res.HighWatermark, hiddenHWM)
		}
		log, err := storage.NewLog(staging, storage.Options{})
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		defer log.Close()
		if log.HighWatermark() != hiddenHWM || log.NextOffset() != hiddenHWM {
			t.Fatalf("staged HWM %d, next offset %d; want both %d (the hidden records cut)", log.HighWatermark(), log.NextOffset(), hiddenHWM)
		}
	})
}

// CatchUp against a static source returns once caught up without a
// freeze — the freeze-free phase that lets a GB partition copy while
// produce keeps flowing. Finalize then completes the (already-caught-up)
// copy identically.
func TestMoveSessionCatchUpThenFinalize(t *testing.T) {
	src := t.TempDir()
	wantHWM, payloads := buildSourcePartition(t, src, 30)
	fetcher := dirFetcher{dir: src, hwm: wantHWM, committed: 12, hasCommitted: true}
	mover := NewPartitionMover(fetcher, 16, nil)

	staging := filepath.Join(t.TempDir(), "staging")
	sess := mover.Begin("addr", "orders", 0, staging)

	// Freeze-free catch-up. A static source converges immediately: the first
	// post-bulk pass copies nothing, so it's caught up.
	converged, err := sess.CatchUp(context.Background(), 4096, 20, 3)
	if err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if !converged {
		t.Fatal("static source must converge, not fall back to stop-and-copy")
	}
	// Finalize (as if the source were now frozen) completes + verifies.
	res, err := sess.Finalize(context.Background())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if res.HighWatermark != wantHWM || res.CommittedOffset != 12 {
		t.Fatalf("res = hwm %d committed %d, want %d / 12", res.HighWatermark, res.CommittedOffset, wantHWM)
	}
	log, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("staged NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
	for off, want := range payloads {
		if _, _, got, err := log.ReadKeyed(off); err != nil || string(got) != string(want) {
			t.Fatalf("offset %d = %q (err %v), want %q", off, got, err, want)
		}
	}
}

// growingFetcher simulates a partition under sustained write: every list
// reports a larger tail, so the copy can never "catch up" to a moving
// target. It serves arbitrary bytes for the delta.
type growingFetcher struct {
	mu     sync.Mutex
	size   int64
	growth int64
}

func (g *growingFetcher) ListPartitionSegments(context.Context, string, string, int) (messaging.PartitionTransferInfo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.size += g.growth // writers keep pace with the copy
	return messaging.PartitionTransferInfo{
		Segments: []storage.SegmentInfo{{BaseOffset: 0, SizeBytes: g.size, Sealed: false}},
	}, nil
}

func (g *growingFetcher) FetchSegmentChunk(_ context.Context, _, _ string, _ int, _, _, length int64) ([]byte, error) {
	return make([]byte, length), nil
}

// CatchUp must NOT loop forever when the writers keep pace with the copy: it
// stops after a bounded number of passes and reports converged=false so the
// caller falls back to a stop-and-copy under the freeze. This is the exact
// hot-big-partition case: without the bound the move would never cut over.
func TestCatchUpBoundedWhenWritersKeepPace(t *testing.T) {
	fetcher := &growingFetcher{growth: 8192} // 8 KiB of new writes per pass
	mover := NewPartitionMover(fetcher, 4096, nil)
	sess := mover.Begin("addr", "orders", 0, filepath.Join(t.TempDir(), "staging"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// lagBytes (100) is far below the per-pass growth (8 KiB), so the lag
	// bound is never met; maxRounds/stallRounds must stop it anyway.
	converged, err := sess.CatchUp(ctx, 100, 20, 3)
	if err != nil {
		t.Fatalf("CatchUp errored (should return a bounded fallback, not fail): %v", err)
	}
	if converged {
		t.Fatal("CatchUp reported converged for a partition that never catches up")
	}
	if ctx.Err() != nil {
		t.Fatal("CatchUp ran until the timeout — it looped instead of falling back")
	}
}

// The fan-out cursor files in the source's partition directory must move
// with the partition: a new owner that finds none tail-anchors and skips
// the child's whole backlog (for a delay child, its entire pending
// window). Confirmed lost before the fix by the audit's reproduction.
func TestPartitionMoverCarriesFanoutCursors(t *testing.T) {
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 25)
	if err := storage.WriteFanoutCursorIfPartitionDirExists(src, "audit-child",
		storage.FanoutCursor{Epoch: "abc", NextOffset: 10}); err != nil {
		t.Fatalf("write cursor: %v", err)
	}
	fetcher := sidecarFetcher{dirFetcher{dir: src, hwm: hwm, committed: 9, hasCommitted: true}}
	mover := NewPartitionMover(fetcher, 1<<20, nil)
	staging := filepath.Join(t.TempDir(), "staged")

	if _, err := mover.Copy(context.Background(), "src-addr", "orders", 0, staging); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	cur, found, err := storage.ReadFanoutCursor(staging, "audit-child")
	if err != nil || !found {
		t.Fatalf("staged copy has no fanout-audit-child.offset (found %v, err %v): the new owner would tail-anchor at %d instead of resuming at 10", found, err, hwm)
	}
	if cur.Epoch != "abc" || cur.NextOffset != 10 {
		t.Fatalf("staged cursor = %+v, want epoch abc next 10 (the source's values)", cur)
	}

	// A force-promote (dead source) carries the last cursor files it saw.
	sess := mover.Begin("src-addr", "orders", 0, filepath.Join(t.TempDir(), "promoted"))
	if _, err := sess.CatchUp(context.Background(), 4096, 3, 2); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if _, err := sess.ForcePromote(); err != nil {
		t.Fatalf("ForcePromote: %v", err)
	}
	if cur, found, _ := storage.ReadFanoutCursor(sess.stagingDir, "audit-child"); !found || cur.NextOffset != 10 {
		t.Fatalf("force-promoted copy cursor = %+v (found %v), want next 10", cur, found)
	}
}

// sidecarFetcher serves a partition directory including its fan-out
// cursor files, as the engine's transfer info does.
type sidecarFetcher struct{ dirFetcher }

func (f sidecarFetcher) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	if err != nil {
		return info, err
	}
	info.Sidecars, err = storage.ListFanoutCursorFiles(f.dir)
	return info, err
}

// hwmDriftFetcher reports a HWM that keeps moving for the first few
// listings (a commit finishing its fsync under the freeze) and then
// settles. Finalize must not stop on a single quiet pass that straddles
// the moving HWM.
type hwmDriftFetcher struct {
	dirFetcher
	mu    sync.Mutex
	lists int
	drift int // listings during which the HWM still moves
}

func (f *hwmDriftFetcher) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	f.mu.Lock()
	f.lists++
	n := f.lists
	f.mu.Unlock()
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	if err != nil {
		return info, err
	}
	if n <= f.drift {
		info.HighWatermark = f.hwm - int64(f.drift-n+1)
	}
	return info, nil
}

func TestMoveSessionFinalizeRequiresTwoIdenticalHWMPasses(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 12)
	fetcher := &hwmDriftFetcher{dirFetcher: dirFetcher{dir: src, hwm: wantHWM}, drift: 3}
	mover := NewPartitionMover(fetcher, 1<<20, nil)
	sess := mover.Begin("addr", "orders", 0, filepath.Join(t.TempDir(), "staging"))

	res, err := sess.Finalize(context.Background())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if res.HighWatermark != wantHWM {
		t.Fatalf("finalized hwm = %d, want the settled %d (Finalize stopped while the HWM was still moving)", res.HighWatermark, wantHWM)
	}
	fetcher.mu.Lock()
	lists := fetcher.lists
	fetcher.mu.Unlock()
	// drift listings with a moving HWM, then two identical quiet ones.
	if lists < fetcher.drift+2 {
		t.Fatalf("Finalize listed %d times, want at least %d (two identical passes after the HWM settled)", lists, fetcher.drift+2)
	}
}

// TestPartitionMoverCopiesAgedOutPartition moves a partition whose
// retained log is empty: retention removed every record and the log keeps
// one empty segment named for the base offset, with the high watermark
// well above zero. The staged copy must recover to that offset, or the
// finalize verify fails and the move retries forever (seen on a devstack
// rebalance toward a freshly joined node: every move of an old, fully
// expired benchmark topic looped on "staged copy next offset 0 < source
// hwm").
func TestPartitionMoverCopiesAgedOutPartition(t *testing.T) {
	const base = int64(5168020)
	src := t.TempDir()
	if err := storage.WriteSegmentFile(src, base, nil); err != nil {
		t.Fatalf("WriteSegmentFile: %v", err)
	}
	if err := storage.WritePersistedHighWatermark(src, base); err != nil {
		t.Fatalf("WritePersistedHighWatermark: %v", err)
	}
	srcLog, err := storage.NewLog(src, storage.Options{})
	if err != nil {
		t.Fatalf("NewLog(source): %v", err)
	}
	if got := srcLog.NextOffset(); got != base {
		t.Fatalf("precondition: source recovers next offset %d, want %d", got, base)
	}
	_ = srcLog.Close()

	staging := filepath.Join(t.TempDir(), "staging")
	mover := NewPartitionMover(dirFetcher{dir: src, hwm: base}, 1<<16, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := mover.Copy(context.Background(), "source", "orders", 0, staging); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	staged, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("NewLog(staged): %v", err)
	}
	defer staged.Close()
	if got := staged.NextOffset(); got != base {
		t.Fatalf("staged copy recovers next offset %d, want %d", got, base)
	}
}

// statSizeSource is a source on a release before committed listings:
// it lists every segment's file size, and, with overServe, a chunk read
// returns everything to the end of the file whatever length was asked
// for. hwm is the high watermark it reports; with log set, the log's
// own. onFetch, when set, runs instead of the first chunk read and
// returns its bytes.
type statSizeSource struct {
	dir       string
	hwm       int64
	log       *storage.Log
	overServe bool
	onFetch   func(base, at int64) []byte
	fetched   bool
}

func (d *statSizeSource) ListPartitionSegments(context.Context, string, string, int) (messaging.PartitionTransferInfo, error) {
	segs, err := storage.ListPartitionSegments(d.dir)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	hwm := d.hwm
	if d.log != nil {
		hwm = d.log.HighWatermark()
	}
	return messaging.PartitionTransferInfo{Segments: segs, HighWatermark: hwm}, nil
}

func (d *statSizeSource) FetchSegmentChunk(_ context.Context, _, _ string, _ int, base, at, length int64) ([]byte, error) {
	if d.onFetch != nil && !d.fetched {
		d.fetched = true
		return d.onFetch(base, at), nil
	}
	if d.overServe {
		length = storage.MaxSegmentReadBytes
	}
	return storage.ReadSegmentRange(d.dir, base, at, length)
}

func rewriteRecord(payload string, stamp int64) []byte {
	return storage.EncodeKeyedRecord("k", stamp, []byte(payload))
}

func rewriteBatch(prefix string, n int, stamp int64) [][]byte {
	var recs [][]byte
	for i := range n {
		recs = append(recs, rewriteRecord(fmt.Sprintf("%s-%d", prefix, i), stamp))
	}
	return recs
}

// sourceWithHiddenFrames writes n committed records to a log in dir in
// one frame, then m records in another frame that is fsynced and never
// made visible.
func sourceWithHiddenFrames(t *testing.T, dir string, n, m int) *storage.Log {
	t.Helper()
	l, err := storage.NewLog(dir, storage.Options{SegmentBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	first, last, err := l.AppendBatch(rewriteBatch("P", n, 1000))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(first, last); err != nil {
		t.Fatal(err)
	}
	if m > 0 {
		if _, _, err := l.AppendBatch(rewriteBatch("HIDDEN", m, 1500)); err != nil {
			t.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

// reopenedSource writes ten committed records to a log in dir, closes
// it, and opens it again, as an owner restart or an idle-evicted log's
// reopen leaves it: its first commit releases the hwm file Close wrote,
// which failCommit can make fail.
func reopenedSource(t *testing.T, dir string) *storage.Log {
	t.Helper()
	l := sourceWithHiddenFrames(t, dir, 10, 0)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := storage.NewLog(dir, storage.Options{SegmentBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// failCommit appends recs to l and commits them through the production
// failure path: the frame is written and fsynced, the first
// high-watermark advance of the log's life fails (EIO on the hwm file
// release), and the log discards the tail for the ingress WAL to commit
// again. during runs in the window between the fsync and the failed
// advance, where a live copy can list and read the frame.
func failCommit(t *testing.T, l *storage.Log, dir string, recs [][]byte, during func()) {
	t.Helper()
	next := l.NextOffset()
	ran := false
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpOpen || path != filepath.Join(dir, "hwm") {
			return nil
		}
		if !ran {
			ran = true
			during()
		}
		return syscall.EIO
	})
	first, last, err := l.AppendBatch(recs)
	if err != nil {
		restore()
		t.Fatal(err)
	}
	cerr := l.CommitDurable(first, last)
	restore()
	if cerr == nil || !ran {
		t.Fatalf("setup: the commit should have failed at the hwm release (err %v, window reached %v)", cerr, ran)
	}
	if l.NextOffset() != next || l.HighWatermark() != next {
		t.Fatalf("setup: after the discard next %d, hwm %d; want both %d", l.NextOffset(), l.HighWatermark(), next)
	}
}

func commitBatch(t *testing.T, l *storage.Log, recs [][]byte) {
	t.Helper()
	first, last, err := l.AppendBatch(recs)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(first, last); err != nil {
		t.Fatal(err)
	}
}

// sourceRecords reads every visible record of l as "payload@stamp".
func sourceRecords(t *testing.T, l *storage.Log) map[int64]string {
	t.Helper()
	want := map[int64]string{}
	for off := range l.HighWatermark() {
		_, at, p, err := l.ReadKeyed(off)
		if err != nil {
			t.Fatalf("source read %d: %v", off, err)
		}
		want[off] = fmt.Sprintf("%s@%d", p, at)
	}
	return want
}

// requireStagedRecords opens the staged copy and requires it to end at
// hwm and hold exactly the records want has below it.
func requireStagedRecords(t *testing.T, staging string, hwm int64, want map[int64]string) {
	t.Helper()
	staged, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	defer staged.Close()
	bad := 0
	for off := range hwm {
		_, at, p, err := staged.ReadKeyed(off)
		got := fmt.Sprintf("%s@%d", p, at)
		if err != nil {
			got = "ERR " + err.Error()
		}
		if got != want[off] {
			bad++
			t.Logf("offset %d: staged %q, source %q", off, got, want[off])
		}
	}
	if bad > 0 {
		t.Fatalf("WRONG RECORDS: the verified copy differs from the source at %d of %d committed offsets", bad, hwm)
	}
	if staged.NextOffset() != hwm {
		t.Fatalf("the staged copy recovers next offset %d, want %d", staged.NextOffset(), hwm)
	}
}

// A source on an older release discards a frame the copy already took
// (a failed commit) and a listing then reports the segment shorter than
// the copy's cursor. The copy cuts its staged segment back to the listed
// size; otherwise it kept the discarded records and appended the tail of
// the records committed at those offsets after them.
func TestMoveTruncatesAStagedSegmentTheSourceShrank(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	l := reopenedSource(t, src)
	source := &statSizeSource{dir: src, log: l}
	staging := filepath.Join(t.TempDir(), "staging")
	sess := NewPartitionMover(source, 1<<20, nil).Begin("src", "orders", 0, staging)

	failCommit(t, l, src, rewriteBatch("A", 3, 2000), func() {
		if _, err := sess.CatchUp(ctx, 1<<30, 3, 1); err != nil {
			t.Errorf("catch-up in the commit window: %v", err)
		}
	})
	// A listing after the discard reports the shorter segment.
	if _, err := sess.CatchUp(ctx, 1<<30, 3, 1); err != nil {
		t.Fatal(err)
	}
	commitBatch(t, l, append(rewriteBatch("A", 3, 3000), rewriteBatch("B", 3, 3000)...))
	commitBatch(t, l, rewriteBatch("C", 3, 4000))
	want := sourceRecords(t, l)

	res, err := sess.Finalize(ctx)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if res.HighWatermark != l.HighWatermark() {
		t.Fatalf("finalized at %d, the source is at %d", res.HighWatermark, l.HighWatermark())
	}
	requireStagedRecords(t, staging, res.HighWatermark, want)
}

// A source on an older release can answer a chunk read with more than
// was asked for: bytes written since the listing, which a failed commit
// may discard and other records replace. The copy stages only what it
// asked for; otherwise its cursor sat past the listed size and the next
// pass appended the tail of the replacing records after the discarded
// ones.
func TestMoveNeverStagesMoreThanItAskedFor(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	l := reopenedSource(t, src)
	source := &statSizeSource{dir: src, log: l, overServe: true}
	source.onFetch = func(base, at int64) []byte {
		var got []byte
		failCommit(t, l, src, rewriteBatch("A", 3, 2000), func() {
			b, err := storage.ReadSegmentRange(src, base, at, storage.MaxSegmentReadBytes)
			if err != nil {
				t.Error(err)
			}
			got = b
		})
		return got
	}
	staging := filepath.Join(t.TempDir(), "staging")
	sess := NewPartitionMover(source, 1<<20, nil).Begin("src", "orders", 0, staging)
	// One pass: a second listing before the regrow would show the
	// discard as a shorter segment, which the copy handles on its own.
	if _, _, err := sess.pass(ctx); err != nil {
		t.Fatal(err)
	}
	commitBatch(t, l, append(rewriteBatch("A", 3, 3000), rewriteBatch("B", 3, 3000)...))
	commitBatch(t, l, rewriteBatch("C", 3, 4000))
	want := sourceRecords(t, l)

	res, err := sess.Finalize(ctx)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	requireStagedRecords(t, staging, res.HighWatermark, want)
}

// A move copies only committed records, through the production source
// path. A commit on the source writes and fsyncs its frame, the copy
// lists and reads the partition in the window before the high watermark
// advances, and the advance fails: the source discards the frame and
// commits other records at the same offsets, here of the same length.
// A copy that took the discarded frame would end at the right length
// and pass every check with records the source never committed.
func TestMoveNeverPromotesARewrittenUncommittedTail(t *testing.T) {
	ctx := context.Background()
	store := seedMoveCluster(t)
	srcEngine, srcLogs, srcData := newTestEngine(t, store, "narad-src")
	produceRecords(t, store, srcEngine, 10)
	// Closed and opened again (an owner restart, an idle-evicted log), so
	// the next commit releases the hwm file Close wrote.
	if err := srcLogs.CloseAll(); err != nil {
		t.Fatal(err)
	}
	peer := enginePeer{store: store, engines: map[string]*messaging.Engine{"src-addr": srcEngine}}
	staging := filepath.Join(t.TempDir(), "staging")
	sess := NewPartitionMover(peer, 1<<20, discardLogger()).Begin("src-addr", "orders", 0, staging)

	rec, err := store.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	batch := func(payload string) []ingress.ProduceRecord {
		return []ingress.ProduceRecord{
			{Topic: "orders", TopicID: rec.ID, Key: "k10", TargetPartition: 0, Payload: []byte(payload)},
			{Topic: "orders", TopicID: rec.ID, Key: "k11", TargetPartition: 0, Payload: []byte(payload)},
		}
	}
	hwmFile := filepath.Join(topicPartitionDirT(t, srcData, "orders", 0), "hwm")
	ran := false
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpOpen || path != hwmFile {
			return nil
		}
		if !ran {
			ran = true
			if _, err := sess.CatchUp(ctx, 1<<30, 3, 1); err != nil {
				t.Errorf("catch-up in the commit window: %v", err)
			}
		}
		return syscall.EIO
	})
	_, cerr := srcEngine.CommitAcceptedProduceBatch(ctx, batch("UNCOMMITTED-TAIL"))
	restore()
	if cerr == nil || !ran {
		t.Fatalf("setup: the commit should have failed at the hwm release (err %v, window reached %v)", cerr, ran)
	}
	if _, err := srcEngine.CommitAcceptedProduceBatch(ctx, batch("COMMITTED-RECORD")); err != nil {
		t.Fatalf("the commit at the same offsets: %v", err)
	}
	log, err := srcLogs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if log.HighWatermark() != 12 {
		t.Fatalf("setup: source hwm %d, want 12", log.HighWatermark())
	}
	want := sourceRecords(t, log)

	res, err := sess.Finalize(ctx)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if res.HighWatermark != 12 {
		t.Fatalf("finalized at %d, want 12", res.HighWatermark)
	}
	requireStagedRecords(t, staging, 12, want)
}

// skewedSource lists a source whose clock is off by skew from this
// node's, on which every segment was last modified age ago.
type skewedSource struct {
	dirFetcher
	skew, age time.Duration
}

func (f skewedSource) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	sourceNow := time.Now().Add(f.skew)
	info.ListedAtUnixNano = sourceNow.UnixNano()
	for i := range info.Segments {
		info.Segments[i].ModTimeUnixNano = sourceNow.Add(-f.age).UnixNano()
	}
	return info, err
}

// ageSegments sets every segment file in dir to modification time at.
func ageSegments(t *testing.T, dir string, at time.Time) {
	t.Helper()
	segs, err := storage.ListPartitionSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if err := storage.SetSegmentModTime(dir, seg.BaseOffset, at); err != nil {
			t.Fatal(err)
		}
	}
}

// stagedSegmentAge is how old the staged copy's first segment file is on
// this node's clock.
func stagedSegmentAge(t *testing.T, staging string) time.Duration {
	t.Helper()
	segs, err := storage.ListPartitionSegments(staging)
	if err != nil || len(segs) == 0 {
		t.Fatalf("staged segments %v (err %v)", segs, err)
	}
	return time.Since(time.Unix(0, segs[0].ModTimeUnixNano))
}

// A move keeps each segment's age. Retention and the cold walk judge a
// segment by its file's modification time, and a copy written fresh
// restarted the retention clock of every moved record: with 1h
// retention, 48h-old records the source reaps survived another hour on
// the new owner. The age is carried, not the source's wall-clock time,
// so a skewed clock on either node does not shift it.
func TestMoveKeepsEachSegmentsAge(t *testing.T) {
	t.Run("same clock", func(t *testing.T) {
		src := t.TempDir()
		hwm, _ := buildSourcePartition(t, src, 8)
		ageSegments(t, src, time.Now().Add(-48*time.Hour))
		staging := filepath.Join(t.TempDir(), "staging")
		if _, err := NewPartitionMover(dirFetcher{dir: src, hwm: hwm}, 7, nil).Copy(context.Background(), "src", "orders", 0, staging); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		open := func(dir string) *storage.Log {
			l, err := storage.NewLog(dir, storage.Options{
				FlushInterval: time.Millisecond,
				Retention:     storage.RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Hour},
			})
			if err != nil {
				t.Fatal(err)
			}
			return l
		}
		s, d := open(src), open(staging)
		defer s.Close()
		defer d.Close()
		s.SweepRetentionNow()
		d.SweepRetentionNow()
		if s.OldestOffset() == 0 || d.OldestOffset() != s.OldestOffset() {
			t.Fatalf("after a retention sweep the source keeps offsets from %d and the copy from %d (hwm %d): the move restarted the copy's retention clock",
				s.OldestOffset(), d.OldestOffset(), hwm)
		}
	})
	for _, skew := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		t.Run("source clock off by "+skew.String(), func(t *testing.T) {
			src := t.TempDir()
			hwm, _ := buildSourcePartition(t, src, 3)
			const age = 48 * time.Hour
			staging := filepath.Join(t.TempDir(), "staging")
			mover := NewPartitionMover(skewedSource{dirFetcher: dirFetcher{dir: src, hwm: hwm}, skew: skew, age: age}, 1<<20, nil)
			if _, err := mover.Copy(context.Background(), "src", "orders", 0, staging); err != nil {
				t.Fatalf("Copy: %v", err)
			}
			if got := stagedSegmentAge(t, staging); got < age-time.Minute || got > age+time.Minute {
				t.Fatalf("staged segment is %v old on this node's clock, want about %v", got.Round(time.Minute), age)
			}
		})
	}
}

// futureSource reports every segment modified far in the future; with
// listed set, it also reports a listing time on a sane clock.
type futureSource struct {
	dirFetcher
	listed bool
}

func (f futureSource) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	info, err := f.dirFetcher.ListPartitionSegments(ctx, addr, topicName, partition)
	if f.listed {
		info.ListedAtUnixNano = time.Now().UnixNano()
	}
	for i := range info.Segments {
		info.Segments[i].ModTimeUnixNano = time.Now().Add(72 * time.Hour).UnixNano()
	}
	return info, err
}

// A modification time in the future is a broken clock, not an age:
// honouring it would keep the segment past its retention, so the copy
// keeps its own time.
func TestMoveIgnoresASourceModTimeInTheFuture(t *testing.T) {
	for _, listed := range []bool{false, true} {
		t.Run(fmt.Sprintf("listing_time=%v", listed), func(t *testing.T) {
			src := t.TempDir()
			hwm, _ := buildSourcePartition(t, src, 3)
			staging := filepath.Join(t.TempDir(), "staging")
			mover := NewPartitionMover(futureSource{dirFetcher: dirFetcher{dir: src, hwm: hwm}, listed: listed}, 1<<20, nil)
			if _, err := mover.Copy(context.Background(), "src", "orders", 0, staging); err != nil {
				t.Fatalf("Copy: %v", err)
			}
			if age := stagedSegmentAge(t, staging); age < -time.Minute {
				t.Fatalf("staged segment stamped %v in the future", (-age).Round(time.Minute))
			}
		})
	}
}
