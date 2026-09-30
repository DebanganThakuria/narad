package runtime

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// GetMany answers each partition as Get would: open logs from the map,
// the rest (never opened, closed, or opened under an older version of
// the record) through the slow path, all in idxs order.
func TestZZWP7bGetManyResolvesLikeGet(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 6})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	open := make(map[int]*storage.Log)
	for _, p := range []int{0, 2, 3} {
		l, err := g.Get("orders", p)
		if err != nil {
			t.Fatalf("Get(%d): %v", p, err)
		}
		open[p] = l
	}
	if err := g.ClosePartition("orders", 3); err != nil {
		t.Fatal(err)
	}

	idxs := []int{5, 3, 0, 2}
	buf := make([]*storage.Log, 0, 8)
	got, err := g.GetMany("orders", idxs, buf)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != len(idxs) || &got[0] != &buf[:1][0] {
		t.Fatalf("GetMany returned %d logs in a new array; want %d in dst's", len(got), len(idxs))
	}
	for i, p := range idxs {
		want, err := g.Get("orders", p)
		if err != nil {
			t.Fatalf("Get(%d): %v", p, err)
		}
		if got[i] != want {
			t.Errorf("GetMany[%d] (partition %d) is not the log Get serves", i, p)
		}
	}
	if got[2] != open[0] || got[3] != open[2] {
		t.Error("GetMany replaced a log that was open")
	}
	if got[1] == open[3] {
		t.Error("GetMany served the closed log of partition 3")
	}

	// A version bump with the same incarnation keeps the open logs; a
	// recreate under a new incarnation retires them.
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 6, RetentionMs: 3_600_000})
	again, err := g.GetMany("orders", idxs, nil)
	if err != nil {
		t.Fatalf("GetMany after an alter: %v", err)
	}
	for i := range idxs {
		if again[i] != got[i] {
			t.Errorf("GetMany[%d] after an alter reopened the log", i)
		}
	}
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 6})
	fresh, err := g.GetMany("orders", idxs, nil)
	if err != nil {
		t.Fatalf("GetMany after a recreate: %v", err)
	}
	for i := range idxs {
		if fresh[i] == got[i] {
			t.Errorf("GetMany[%d] after a recreate served the deleted incarnation's log", i)
		}
	}

	if logs, err := g.GetMany("missing", []int{0}, nil); !errors.Is(err, errs.ErrTopicNotFound) || logs != nil {
		t.Fatalf("GetMany(missing) = %v, %v; want nil, ErrTopicNotFound", logs, err)
	}
}

// GetMany is a real use, like Get: it takes a log back from the
// cold-retention walk and refreshes a stale lastAccess, so neither the
// walk nor idle eviction closes a log a consume scan just resolved.
func TestZZWP7bGetManyStampsUse(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 2})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	for p := range 2 {
		if _, err := g.Get("orders", p); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour).UnixNano()
	g.mu.RLock()
	for p := range 2 {
		e := g.logs[keyOf("orders", p)]
		e.walkOwned.Store(true)
		e.lastAccess.Store(old)
	}
	g.mu.RUnlock()

	if _, err := g.GetMany("orders", []int{0, 1}, nil); err != nil {
		t.Fatal(err)
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	for p := range 2 {
		e := g.logs[keyOf("orders", p)]
		if e.walkOwned.Load() || e.lastAccess.Load() <= old {
			t.Errorf("partition %d after GetMany: walkOwned=%v lastAccess moved=%v", p, e.walkOwned.Load(), e.lastAccess.Load() > old)
		}
	}
}

// A log that is being closed is not served: GetMany waits on the topic's
// guard in the slow path, like Get, and returns the reopened log.
func TestZZWP7bGetManyWaitsOutAClose(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 2})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	var before [2]*storage.Log
	for p := range 2 {
		l, err := g.Get("orders", p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = l
	}

	unlock := g.lockTopic("orders")
	key := keyOf("orders", 1)
	g.mu.Lock()
	e := g.logs[key]
	claimLocked(e)
	g.mu.Unlock()

	type result struct {
		logs []*storage.Log
		err  error
	}
	done := make(chan result, 1)
	go func() {
		logs, err := g.GetMany("orders", []int{0, 1}, nil)
		done <- result{logs, err}
	}()
	select {
	case r := <-done:
		unlock()
		t.Fatalf("GetMany returned during the close: %v", r.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := g.closeClaimed([]claimedEntry{{key: key, entry: e}}); err != nil {
		t.Fatal(err)
	}
	unlock()

	r := <-done
	if r.err != nil {
		t.Fatalf("GetMany: %v", r.err)
	}
	if r.logs[0] != before[0] {
		t.Error("GetMany reopened partition 0, which was not being closed")
	}
	if r.logs[1] == before[1] {
		t.Fatal("GetMany served partition 1's closed log")
	}
	if _, err := r.logs[1].Append([]byte("x")); err != nil {
		t.Fatalf("append to the log GetMany returned: %v", err)
	}
}

// A scan that keeps its slice resolves open logs without allocating.
func TestZZWP7bGetManyDoesNotAllocate(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders-events-v2", ID: "0000000000000001", Partitions: 12})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	idxs := make([]int, 12)
	for i := range idxs {
		idxs[i] = i
	}
	dst, err := g.GetMany("orders-events-v2", idxs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if dst, err = g.GetMany("orders-events-v2", idxs, dst); err != nil {
			t.Fatalf("GetMany: %v", err)
		}
	}); allocs != 0 {
		t.Errorf("GetMany over open logs allocated %.1f objects per call, want 0", allocs)
	}
}

// BenchmarkZZWP7bResolveScan resolves every local partition of a topic,
// as a consume scan does before it probes them. Get is today's shape
// (messaging's partitionLogs: a fresh slice and one Get per partition);
// GetMany reuses the scan's slice.
func BenchmarkZZWP7bResolveScan(b *testing.B) {
	for _, parts := range []int{1, 12} {
		idxs := make([]int, parts)
		for i := range idxs {
			idxs[i] = i
		}
		b.Run("parts="+strconv.Itoa(parts)+"/Get", func(b *testing.B) {
			g, name := zzWP7bBenchLogs(b, parts)
			b.ReportAllocs()
			for b.Loop() {
				logs := make([]*storage.Log, len(idxs))
				for i, idx := range idxs {
					l, err := g.Get(name, idx)
					if err != nil {
						b.Fatal(err)
					}
					logs[i] = l
				}
			}
		})
		b.Run("parts="+strconv.Itoa(parts)+"/GetMany", func(b *testing.B) {
			g, name := zzWP7bBenchLogs(b, parts)
			b.ReportAllocs()
			var dst []*storage.Log
			var err error
			for b.Loop() {
				if dst, err = g.GetMany(name, idxs, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkZZWP7bResolveScanParallel(b *testing.B) {
	const parts = 12
	idxs := make([]int, parts)
	for i := range idxs {
		idxs[i] = i
	}
	b.Run("Get", func(b *testing.B) {
		g, name := zzWP7bBenchLogs(b, parts)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				logs := make([]*storage.Log, len(idxs))
				for i, idx := range idxs {
					l, err := g.Get(name, idx)
					if err != nil {
						b.Error(err)
						return
					}
					logs[i] = l
				}
			}
		})
	})
	b.Run("GetMany", func(b *testing.B) {
		g, name := zzWP7bBenchLogs(b, parts)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var dst []*storage.Log
			var err error
			for pb.Next() {
				if dst, err = g.GetMany(name, idxs, dst); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
