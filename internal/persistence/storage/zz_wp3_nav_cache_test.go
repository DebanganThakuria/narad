package storage

import "testing"

// bestAnchor stops at the first cached frame that contains the offset
// instead of walking the whole list. The element parked at the back is
// not a navCacheEntry: a walk that reaches it panics.
func TestWP3NavCacheStopsAtCoveringFrame(t *testing.T) {
	c := newNavCache(maxNavCacheEntries)
	for i := range 4 {
		c.put(indexEntry{segmentBaseOffset: 0, baseOffset: int64(i * 10), recordCount: 10, framePos: int64(i * 500), frameLen: 500})
	}
	c.ll.PushBack("not a navCacheEntry")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("bestAnchor walked past the covering frame: %v", r)
		}
	}()
	// Front is the frame at 30; 25 lives in the frame at 20, second in line.
	e, ok := c.bestAnchor(0, 25)
	if !ok || e.baseOffset != 20 {
		t.Fatalf("bestAnchor(25) = (%+v, %v), want the frame at 20", e, ok)
	}
	if front := c.ll.Front().Value.(*navCacheEntry).entry; front.baseOffset != 20 {
		t.Fatalf("front after lookup = frame at %d, want 20 (most recently used)", front.baseOffset)
	}
}
