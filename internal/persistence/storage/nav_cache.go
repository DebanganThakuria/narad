package storage

import (
	"container/list"
	"sync"
)

// maxNavCacheEntries bounds the per-Log navigation cache. Entries are tiny
// position metadata (an indexEntry, ~48 B), so a few hundred is negligible
// memory while comfortably covering the frames a partition's consumers are
// actively reading near the committed frontier.
const maxNavCacheEntries = 256

// navCache memoizes recently-resolved frame positions so a sequential consume
// read resolves from the previous frame instead of re-walking from the sparse
// index anchor. Locating an offset between sparse anchors otherwise costs one
// header read (pread) per frame stepped over (scanSegmentFromIndexAnchorLocked);
// for sequential reads that is O(distance-from-anchor) every read. This cache
// collapses it to ~one header read per frame boundary (0 within a frame).
//
// It holds only immutable position metadata: frames are append-only and segment
// base offsets are never reused, so a cached indexEntry.framePos stays valid for
// the life of its segment. Entries are dropped when the segment is deleted
// (invalidateSegment, called by retention under the Log's write lock).
//
// Concurrency mirrors frameCache: navCache.mu always nests inside the Log's
// rwmu (read or write) and its methods never touch the Log, so the lock order
// is strictly rwmu -> navCache.mu and cannot deadlock. It complements frameCache
// (which caches decoded payloads); this one caches where to find a frame.
type navCache struct {
	mu         sync.Mutex
	ll         *list.List // front = most recently used
	items      map[frameKey]*list.Element
	maxEntries int
}

type navCacheEntry struct {
	key   frameKey
	entry indexEntry
}

func newNavCache(maxEntries int) *navCache {
	if maxEntries < 1 {
		maxEntries = 1
	}
	return &navCache{
		ll:         list.New(),
		items:      make(map[frameKey]*list.Element),
		maxEntries: maxEntries,
	}
}

// bestAnchor returns the cached frame in segmentBase with the largest baseOffset
// that is <= offset — the closest navigation start point from below. ok is false
// when no cached frame qualifies. The chosen entry is marked most-recently-used.
func (c *navCache) bestAnchor(segmentBase, offset int64) (indexEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Fast path: the most-recently-used entry almost always covers a sequential
	// read (the frame just resolved, and the next few records fall in it). A
	// frame that contains the offset is unambiguously the best anchor, so return
	// it in O(1) without walking the list — this is the common case at high read
	// rates, where the full scan was showing up as a CPU hot spot.
	if front := c.ll.Front(); front != nil {
		e := front.Value.(*navCacheEntry).entry
		if e.segmentBaseOffset == segmentBase &&
			offset >= e.baseOffset && offset < e.baseOffset+int64(e.recordCount) {
			return e, true
		}
	}

	// Slow path: closest cached frame at or below the offset (boundary crossings
	// and non-sequential reads).
	var (
		best    indexEntry
		bestEl  *list.Element
		bestSet bool
	)
	for el := c.ll.Front(); el != nil; el = el.Next() {
		e := el.Value.(*navCacheEntry).entry
		if e.segmentBaseOffset != segmentBase || e.baseOffset > offset {
			continue
		}
		if !bestSet || e.baseOffset > best.baseOffset {
			best, bestEl, bestSet = e, el, true
		}
	}
	if bestSet {
		c.ll.MoveToFront(bestEl)
	}
	return best, bestSet
}

// put inserts (or refreshes) a resolved frame.
func (c *navCache) put(e indexEntry) {
	k := frameKey{segmentBase: e.segmentBaseOffset, framePos: e.framePos}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		el.Value.(*navCacheEntry).entry = e
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&navCacheEntry{key: k, entry: e})
	c.items[k] = el
	for c.ll.Len() > c.maxEntries {
		back := c.ll.Back()
		if back == nil || back == el {
			break // never evict the entry we just inserted
		}
		be := back.Value.(*navCacheEntry)
		c.ll.Remove(back)
		delete(c.items, be.key)
	}
}

// invalidateSegment drops every cached frame from the given segment, called when
// retention deletes that segment file.
func (c *navCache) invalidateSegment(segmentBase int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		if k.segmentBase == segmentBase {
			c.ll.Remove(el)
			delete(c.items, k)
		}
	}
}
