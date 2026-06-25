package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

func navTestEntry(seg, base int64, recordCount int32) indexEntry {
	return indexEntry{
		segmentBaseOffset: seg,
		baseOffset:        base,
		recordCount:       recordCount,
		framePos:          base * 64, // unique per frame within a segment
		frameLen:          40,
	}
}

// bestAnchor returns the closest cached frame at or below the offset, scoped to
// the segment.
func TestNavCacheBestAnchorClosest(t *testing.T) {
	c := newNavCache(16)
	for _, base := range []int64{0, 5, 10} {
		c.put(navTestEntry(0, base, 5)) // frames cover [0,5) [5,10) [10,15)
	}

	cases := []struct {
		offset   int64
		wantBase int64
		wantOK   bool
	}{
		{3, 0, true},    // inside [0,5)
		{7, 5, true},    // inside [5,10)
		{12, 10, true},  // inside [10,15)
		{100, 10, true}, // past all -> closest from below
		{-1, 0, false},  // below the lowest baseOffset
	}
	for _, tc := range cases {
		got, ok := c.bestAnchor(0, tc.offset)
		if ok != tc.wantOK {
			t.Fatalf("bestAnchor(0,%d) ok=%v, want %v", tc.offset, ok, tc.wantOK)
		}
		if ok && got.baseOffset != tc.wantBase {
			t.Fatalf("bestAnchor(0,%d) baseOffset=%d, want %d", tc.offset, got.baseOffset, tc.wantBase)
		}
	}
}

func TestNavCacheSegmentScoping(t *testing.T) {
	c := newNavCache(16)
	c.put(navTestEntry(0, 5, 5))
	c.put(navTestEntry(100, 5, 5))

	got, ok := c.bestAnchor(0, 7)
	if !ok || got.segmentBaseOffset != 0 {
		t.Fatalf("bestAnchor(seg0) = %+v ok=%v, want segment 0", got, ok)
	}
	got, ok = c.bestAnchor(100, 7)
	if !ok || got.segmentBaseOffset != 100 {
		t.Fatalf("bestAnchor(seg100) = %+v ok=%v, want segment 100", got, ok)
	}
	// A segment with no cached frames misses.
	if _, ok := c.bestAnchor(200, 7); ok {
		t.Fatal("bestAnchor(seg200) ok=true, want miss")
	}
}

func TestNavCacheInvalidateSegment(t *testing.T) {
	c := newNavCache(16)
	c.put(navTestEntry(0, 5, 5))
	c.put(navTestEntry(1, 5, 5))

	c.invalidateSegment(0)
	if _, ok := c.bestAnchor(0, 7); ok {
		t.Fatal("segment 0 still cached after invalidate")
	}
	if _, ok := c.bestAnchor(1, 7); !ok {
		t.Fatal("segment 1 dropped by invalidate(0)")
	}
}

func TestNavCacheEvictionBound(t *testing.T) {
	const limit = 8
	c := newNavCache(limit)
	for base := range limit + 20 {
		c.put(navTestEntry(0, int64(base), 1))
	}
	c.mu.Lock()
	n := c.ll.Len()
	items := len(c.items)
	c.mu.Unlock()
	if n != limit || items != limit {
		t.Fatalf("size after overflow: list=%d items=%d, want %d", n, items, limit)
	}
}

func TestNavCachePutRefreshesExisting(t *testing.T) {
	c := newNavCache(16)
	c.put(navTestEntry(0, 5, 1))  // recordCount 1 -> covers [5,6)
	c.put(navTestEntry(0, 5, 10)) // same frame (framePos), updated recordCount -> [5,15)
	got, ok := c.bestAnchor(0, 12)
	if !ok || got.recordCount != 10 {
		t.Fatalf("bestAnchor after refresh = %+v ok=%v, want recordCount 10", got, ok)
	}
	c.mu.Lock()
	n := c.ll.Len()
	c.mu.Unlock()
	if n != 1 {
		t.Fatalf("duplicate frame inserted twice: list len=%d, want 1", n)
	}
}

func TestNavCacheConcurrent(t *testing.T) {
	c := newNavCache(64)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 1000 {
				base := int64((g*1000 + i) % 200)
				c.put(navTestEntry(0, base, 3))
				c.bestAnchor(0, base+1)
			}
		}(g)
	}
	wg.Wait()
}

// Sequential reads must NOT re-walk frames from the sparse anchor: a re-read of
// the same offset costs zero header reads (direct cache hit), and stepping to
// the next frame costs ~one — versus the cold first read which walks the whole
// anchor gap. Proves the navCache fixes the consume pread storm.
func TestNavCacheEliminatesSequentialRewalk(t *testing.T) {
	var headerReads atomic.Int64
	frameHeaderReadHook = func() { headerReads.Add(1) }
	t.Cleanup(func() { frameHeaderReadHook = nil })

	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 8 << 20 // one segment
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	const total = 2000
	for i := range total {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "record-%04d", i))
	}

	read := func(off int64) []byte {
		t.Helper()
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		return got
	}

	// Cold read of a deep offset walks the whole gap from its sparse anchor.
	const deep = 1500
	headerReads.Store(0)
	read(deep)
	cold := headerReads.Load()
	if cold < 2 {
		t.Fatalf("cold read header reads = %d, want a real walk (>=2)", cold)
	}

	// Re-read of the same offset is a direct cache hit: zero header reads.
	headerReads.Store(0)
	if got := read(deep); !bytes.Equal(got, []byte("record-1500")) {
		t.Fatalf("re-read payload = %q", got)
	}
	if warm := headerReads.Load(); warm != 0 {
		t.Fatalf("re-read header reads = %d, want 0 (direct hit)", warm)
	}

	// Stepping forward frame-by-frame costs ~one header read per frame (here one
	// record per frame), never the full gap again.
	headerReads.Store(0)
	for off := int64(deep + 1); off <= deep+20; off++ {
		want := fmt.Appendf(nil, "record-%04d", off)
		if got := read(off); !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", off, got, want)
		}
	}
	stepped := headerReads.Load()
	if stepped > 20 {
		t.Fatalf("20 sequential steps used %d header reads, want <=20 (~1/frame)", stepped)
	}
}

// Reads populate the navCache; retention deletion invalidates the cached
// positions for the removed segment (memory hygiene) and post-deletion reads
// correctly return ErrOffsetNotFound — never a stale framePos into a gone file.
func TestNavCacheInvalidatedOnRetention(t *testing.T) {
	clock := newAtomicTime(time.Now())
	opts := retentionOpts(t, clock, RetentionConfig{
		MaxAge:        10 * time.Minute,
		CheckInterval: 1 * time.Hour,
	})
	dir := testLogPath(t)
	produceN(t, dir, opts, 5) // 5 sealed segments (one record each) + active

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	// Reading the sealed offsets populates the navCache.
	for off := range int64(5) {
		if _, err := l.Read(off); err != nil {
			t.Fatalf("Read(%d) before sweep: %v", off, err)
		}
	}
	l.navCache.mu.Lock()
	populated := l.navCache.ll.Len()
	l.navCache.mu.Unlock()
	if populated == 0 {
		t.Fatal("navCache not populated by reads")
	}

	clock.Set(time.Now().Add(1 * time.Hour)) // every sealed segment is old
	l.reaper.sweep()

	l.navCache.mu.Lock()
	remaining := l.navCache.ll.Len()
	l.navCache.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("navCache not invalidated on retention: %d entries remain", remaining)
	}
	for off := range int64(5) {
		if _, err := l.Read(off); !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("Read(%d) after sweep want ErrOffsetNotFound got %v", off, err)
		}
	}
}
