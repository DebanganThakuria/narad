package storage

import (
	"sync"
	"testing"
)

func recs(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte("xx")
	}
	return out
}

func TestFrameCacheLRUEviction(t *testing.T) {
	c := newFrameCache(3, 1<<20)
	k1 := frameKey{0, 0}
	k2 := frameKey{0, 100}
	k3 := frameKey{0, 200}
	k4 := frameKey{0, 300}
	c.put(k1, recs(2))
	c.put(k2, recs(2))
	c.put(k3, recs(2))

	// Touch k1 so it's most-recently-used, then insert k4 → LRU (k2) evicts.
	if _, ok := c.get(k1); !ok {
		t.Fatal("k1 missing")
	}
	c.put(k4, recs(2))

	if _, ok := c.get(k2); ok {
		t.Fatal("k2 should have been evicted (LRU)")
	}
	for _, k := range []frameKey{k1, k3, k4} {
		if _, ok := c.get(k); !ok {
			t.Fatalf("%v should be present", k)
		}
	}
}

func TestFrameCacheByteCap(t *testing.T) {
	c := newFrameCache(1000, 10)                      // 10-byte budget
	c.put(frameKey{0, 0}, [][]byte{[]byte("aaaaaa")}) // 6 bytes
	c.put(frameKey{0, 1}, [][]byte{[]byte("bbbbbb")}) // +6 = 12 > 10 → evict first
	if _, ok := c.get(frameKey{0, 0}); ok {
		t.Fatal("first frame should be evicted by the byte cap")
	}
	if _, ok := c.get(frameKey{0, 1}); !ok {
		t.Fatal("second frame should be present")
	}
}

func TestFrameCacheInvalidateSegment(t *testing.T) {
	c := newFrameCache(16, 1<<20)
	c.put(frameKey{0, 0}, recs(1))
	c.put(frameKey{0, 100}, recs(1))
	c.put(frameKey{1, 0}, recs(1))

	c.invalidateSegment(0)

	if _, ok := c.get(frameKey{0, 0}); ok {
		t.Fatal("segment 0 frame should be invalidated")
	}
	if _, ok := c.get(frameKey{0, 100}); ok {
		t.Fatal("segment 0 frame should be invalidated")
	}
	if _, ok := c.get(frameKey{1, 0}); !ok {
		t.Fatal("segment 1 frame should survive")
	}
}

func TestFrameCacheConcurrent(t *testing.T) {
	c := newFrameCache(8, 1<<20)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range 500 {
				k := frameKey{0, int64(i % 16)}
				c.put(k, recs(1))
				c.get(k)
			}
		})
	}
	wg.Wait() // -race asserts no data race; no panic = bookkeeping holds
}
