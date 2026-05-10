package metastore

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// lruCache is a byte-bounded LRU keyed by string. Reads use sync.Map for
// a lock-free fast path; the LRU list is bumped under mu on a best-effort
// basis (TryLock) so a contended bump degrades to "leave it where it is"
// rather than blocking the reader.
//
// A byTopic index lets DeleteTopic remove only the entries owned by one
// topic in O(versions+1), instead of nuking the whole cache.
type lruCache struct {
	entries sync.Map // string -> *cacheEntry

	mu       sync.Mutex
	ll       *list.List // front = most recently used
	byTopic  map[string]map[string]struct{}
	total    atomic.Int64
	maxBytes int64
}

type cacheEntry struct {
	key   string
	value any
	size  int64
	topic string // "" for non-topic-scoped entries (e.g. listTopicsKey)
	elem  *list.Element
}

func newLRUCache(maxMB int) *lruCache {
	if maxMB <= 0 {
		maxMB = defaultCacheMaxMB
	}
	return &lruCache{
		ll:       list.New(),
		byTopic:  make(map[string]map[string]struct{}),
		maxBytes: int64(maxMB) * 1024 * 1024,
	}
}

func (c *lruCache) get(key string) (any, bool) {
	v, ok := c.entries.Load(key)
	if !ok {
		return nil, false
	}
	e := v.(*cacheEntry)
	// Best-effort LRU bump. If mu is contended, skip — readers never
	// block, and we accept slightly stale recency information.
	if c.mu.TryLock() {
		if e.elem != nil {
			c.ll.MoveToFront(e.elem)
		}
		c.mu.Unlock()
	}
	return e.value, true
}

// store inserts or updates key. topic is the owning topic for surgical
// deletion via deleteTopicScope; pass "" for entries that aren't scoped
// to a specific topic (e.g. the topics list).
//
// Updates allocate a fresh *cacheEntry rather than mutating the existing
// one in place. Lock-free readers in get() dereference e.value without
// taking mu; an in-place mutation would race with those reads (torn
// struct or slice header). Replacing the pointer in entries via
// sync.Map.Store is atomic — readers see either the old entry or the
// new one, never a half-updated value. The old entry stays alive for
// any in-flight reader and is GC'd afterwards.
func (c *lruCache) store(key string, val any, topicName string) {
	size := sizeOf(val)
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.entries.Load(key); ok {
		old := existing.(*cacheEntry)
		// Topic is fixed at first insertion; preserve it so the byTopic
		// index stays consistent regardless of what callers pass.
		e := &cacheEntry{key: key, value: val, size: size, topic: old.topic}
		c.ll.Remove(old.elem)
		e.elem = c.ll.PushFront(e)
		c.entries.Store(key, e)
		c.total.Add(size - old.size)
		c.evictLocked()
		return
	}

	e := &cacheEntry{key: key, value: val, size: size, topic: topicName}
	e.elem = c.ll.PushFront(e)
	c.entries.Store(key, e)
	c.total.Add(size)

	if topicName != "" {
		set, ok := c.byTopic[topicName]
		if !ok {
			set = make(map[string]struct{})
			c.byTopic[topicName] = set
		}
		set[key] = struct{}{}
	}

	c.evictLocked()
}

func (c *lruCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeKeyLocked(key)
}

// deleteTopicScope removes only the entries owned by topicName. Callers
// that also want to invalidate non-scoped entries (e.g. the topics list)
// must call delete() for those keys separately.
func (c *lruCache) deleteTopicScope(topicName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := c.byTopic[topicName]
	for k := range keys {
		c.removeKeyLocked(k)
	}
	delete(c.byTopic, topicName)
}

func (c *lruCache) removeKeyLocked(key string) {
	v, ok := c.entries.LoadAndDelete(key)
	if !ok {
		return
	}
	e := v.(*cacheEntry)
	if e.elem != nil {
		c.ll.Remove(e.elem)
	}
	c.total.Add(-e.size)
	if e.topic != "" {
		if set, ok := c.byTopic[e.topic]; ok {
			delete(set, key)
			if len(set) == 0 {
				delete(c.byTopic, e.topic)
			}
		}
	}
}

func (c *lruCache) evictLocked() {
	for c.total.Load() > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			return
		}
		e := back.Value.(*cacheEntry)
		c.removeKeyLocked(e.key)
	}
}

// sizeOf returns an approximate byte cost for a cached value.
func sizeOf(v any) int64 {
	switch x := v.(type) {
	case []byte:
		return int64(len(x))
	case topic.Topic:
		return 256
	case []topic.Topic:
		return int64(len(x)) * 256
	default:
		return 256
	}
}
