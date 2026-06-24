package storage

import (
	"container/list"
	"sync"
)

const (
	// maxDecodeCacheFrames bounds how many decoded frames a partition retains;
	// maxDecodeCacheBytes caps their total size. Frames hold few records at
	// high fan-out (small) and many only when partitions are few (but then
	// there are few partitions), so per-partition memory stays modest either
	// way. The byte cap is the hard ceiling.
	maxDecodeCacheFrames = 16
	maxDecodeCacheBytes  = 4 << 20 // 4 MiB
)

// frameKey identifies a decoded frame. (segmentBaseOffset, framePos) is
// unique forever: segments are append-only and segment base offsets are
// never reused, so a cached frame stays valid until its segment is deleted.
type frameKey struct {
	segmentBase int64
	framePos    int64
}

type frameCacheEntry struct {
	key   frameKey
	recs  [][]byte
	bytes int
}

// frameCache is a small byte- and count-bounded LRU of decoded frames. It
// replaces a single-slot cache that thrashed under concurrent readers: many
// consumers reading the same hot frame on one partition each lost the slot
// and re-decoded the whole (zstd) frame. The byte cap matches the old
// single-slot worst case (one max-size frame), so memory does not grow, but
// several smaller frames can be retained — eliminating the re-decode churn.
type frameCache struct {
	mu        sync.Mutex
	ll        *list.List // front = most recently used
	items     map[frameKey]*list.Element
	maxFrames int
	maxBytes  int
	curBytes  int
}

func newFrameCache(maxFrames, maxBytes int) *frameCache {
	if maxFrames < 1 {
		maxFrames = 1
	}
	if maxBytes < 1 {
		maxBytes = 1
	}
	return &frameCache{
		ll:        list.New(),
		items:     make(map[frameKey]*list.Element),
		maxFrames: maxFrames,
		maxBytes:  maxBytes,
	}
}

// get returns the cached decoded records for a frame. The returned slice is
// shared and must not be mutated; callers copy the single record they need.
func (c *frameCache) get(k frameKey) ([][]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(e)
	return e.Value.(*frameCacheEntry).recs, true
}

// put inserts a frame's decoded records, evicting LRU entries to stay within
// the frame and byte budgets. recs must already be owned by the cache (the
// caller copies decoder output before calling).
func (c *frameCache) put(k frameKey, recs [][]byte) {
	bytes := 0
	for _, r := range recs {
		bytes += len(r)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		return
	}
	e := c.ll.PushFront(&frameCacheEntry{key: k, recs: recs, bytes: bytes})
	c.items[k] = e
	c.curBytes += bytes
	for c.ll.Len() > c.maxFrames || c.curBytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil || back == e {
			break // never evict the entry we just inserted
		}
		be := back.Value.(*frameCacheEntry)
		c.ll.Remove(back)
		delete(c.items, be.key)
		c.curBytes -= be.bytes
	}
}

// invalidateSegment drops every cached frame from the given segment, called
// when retention deletes that segment file.
func (c *frameCache) invalidateSegment(segmentBase int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.items {
		if k.segmentBase == segmentBase {
			c.curBytes -= e.Value.(*frameCacheEntry).bytes
			c.ll.Remove(e)
			delete(c.items, k)
		}
	}
}
