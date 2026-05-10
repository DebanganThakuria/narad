package metastore

import (
	"container/list"
	"strings"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestSizeOfTypedValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  any
		want int64
	}{
		{"bytes", []byte("hello"), 5},
		{"empty bytes", []byte(nil), 0},
		{"topic", topic.Topic{Name: "x"}, 256},
		{"topic slice", []topic.Topic{{Name: "a"}, {Name: "b"}, {Name: "c"}}, 768},
		{"unknown type defaults to 256", "raw string", 256},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sizeOf(tc.val); got != tc.want {
				t.Fatalf("sizeOf(%v) = %d, want %d", tc.val, got, tc.want)
			}
		})
	}
}

func TestLRUEvictsOldestOverByteCap(t *testing.T) {
	t.Parallel()
	// Byte budget: 1KiB so a few schemas fill it.
	c := &lruCache{
		ll:       list.New(),
		byTopic:  make(map[string]map[string]struct{}),
		maxBytes: 1024,
	}

	// Insert 5 schemas of 400 bytes each → only the most-recent ~2 fit.
	val := make([]byte, 400)
	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	for _, k := range keys {
		c.store(k, val, "topicA")
	}

	// k1, k2, k3 should be evicted (oldest); k4, k5 should remain.
	for _, gone := range []string{"k1", "k2", "k3"} {
		if _, ok := c.get(gone); ok {
			t.Errorf("expected %s to be evicted", gone)
		}
	}
	for _, kept := range []string{"k4", "k5"} {
		if _, ok := c.get(kept); !ok {
			t.Errorf("expected %s to be retained", kept)
		}
	}
	if c.total.Load() > c.maxBytes {
		t.Fatalf("total %d exceeds maxBytes %d after eviction", c.total.Load(), c.maxBytes)
	}
}

func TestLRUBumpOnGetKeepsHotEntry(t *testing.T) {
	t.Parallel()
	c := newLRUCache(0) // default cap doesn't matter; we override below
	c.maxBytes = 1024   // fits 2 entries of 400 bytes

	val := make([]byte, 400)
	c.store("hot", val, "t")
	c.store("cold", val, "t")
	// LRU back == "hot". A read should bump it to front so the next
	// store evicts "cold" instead.
	if _, ok := c.get("hot"); !ok {
		t.Fatalf("hot missing after store")
	}
	c.store("new", val, "t")

	if _, ok := c.get("hot"); !ok {
		t.Errorf("hot should survive eviction (was bumped on get)")
	}
	if _, ok := c.get("cold"); ok {
		t.Errorf("cold should have been evicted (oldest after bump)")
	}
}

func TestDeleteTopicScopeOnlyRemovesThatTopic(t *testing.T) {
	t.Parallel()
	c := newLRUCache(8)

	c.store(topicCacheKey("a"), topic.Topic{Name: "a"}, "a")
	c.store(topicCacheKey("b"), topic.Topic{Name: "b"}, "b")
	c.store(schemaCacheKey("a", 1), []byte("schema-a-v1"), "a")
	c.store(schemaCacheKey("a", 2), []byte("schema-a-v2"), "a")
	c.store(schemaCacheKey("b", 1), []byte("schema-b-v1"), "b")
	c.store(listTopicsKey, []topic.Topic{{Name: "a"}, {Name: "b"}}, "")

	c.deleteTopicScope("a")

	// All a-scoped entries gone.
	for _, gone := range []string{
		topicCacheKey("a"),
		schemaCacheKey("a", 1),
		schemaCacheKey("a", 2),
	} {
		if _, ok := c.get(gone); ok {
			t.Errorf("expected %s to be removed by scope-delete", gone)
		}
	}
	// b's entries untouched.
	for _, kept := range []string{
		topicCacheKey("b"),
		schemaCacheKey("b", 1),
	} {
		if _, ok := c.get(kept); !ok {
			t.Errorf("expected %s to survive (not in scope)", kept)
		}
	}
	// Non-scoped entries (listTopicsKey) survive unless caller explicitly
	// invalidates — that's the contract (DeleteTopic does both calls).
	if _, ok := c.get(listTopicsKey); !ok {
		t.Errorf("listTopicsKey is non-scoped and must survive deleteTopicScope")
	}
	// byTopic["a"] cleaned up.
	c.mu.Lock()
	if _, ok := c.byTopic["a"]; ok {
		t.Errorf("byTopic[\"a\"] should be deleted after scope-delete")
	}
	c.mu.Unlock()
}

// TestConcurrentGetStoreSameKey is a regression test for the
// update-path data race: writers used to mutate *cacheEntry in place
// while readers dereferenced e.value lock-free. With the fix, store()
// allocates a new entry and atomically swaps the sync.Map pointer, so
// readers always see a fully-formed value. Run under -race.
func TestConcurrentGetStoreSameKey(t *testing.T) {
	t.Parallel()
	c := newLRUCache(8)

	const writers = 8
	const readers = 8
	const iterations = 1000

	var writersWG, readersWG sync.WaitGroup
	writersWG.Add(writers)
	readersWG.Add(readers)
	stop := make(chan struct{})

	for w := 0; w < writers; w++ {
		go func() {
			defer writersWG.Done()
			for i := 0; i < iterations; i++ {
				// Vary value size so a torn slice header would surface
				// as a length/capacity mismatch.
				val := make([]byte, (i%32)+1)
				c.store("k", val, "t")
			}
		}()
	}
	for r := 0; r < readers; r++ {
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if v, ok := c.get("k"); ok {
					b := v.([]byte)
					// Touch every element — exposes any torn write
					// where len() outruns the backing array.
					for j := range b {
						_ = b[j]
					}
				}
			}
		}()
	}
	writersWG.Wait()
	close(stop)
	readersWG.Wait()
}

func TestStoreUpdatesExistingEntry(t *testing.T) {
	t.Parallel()
	c := newLRUCache(8)
	c.store("k", []byte("v1"), "t")
	c.store("k", []byte("longer-value"), "t")

	v, ok := c.get("k")
	if !ok {
		t.Fatalf("entry missing after update")
	}
	if !strings.HasPrefix(string(v.([]byte)), "longer-value") {
		t.Fatalf("expected updated value, got %q", v)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.total.Load() != int64(len("longer-value")) {
		t.Fatalf("total should reflect updated size: got %d", c.total.Load())
	}
	if c.ll.Len() != 1 {
		t.Fatalf("list should contain one element after update, got %d", c.ll.Len())
	}
}
