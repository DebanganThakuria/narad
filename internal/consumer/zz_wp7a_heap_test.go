package consumer

import (
	"container/heap"
	"math/rand/v2"
	"testing"
)

// zzWP7aRefHeap is container/heap over the same entries, the reference
// the typed expiryHeap operations must match.
type zzWP7aRefHeap []expiryEntry

func (h zzWP7aRefHeap) Len() int           { return len(h) }
func (h zzWP7aRefHeap) Less(i, j int) bool { return h[i].expiresAtUnixMs < h[j].expiresAtUnixMs }
func (h zzWP7aRefHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *zzWP7aRefHeap) Push(x any)        { *h = append(*h, x.(expiryEntry)) }
func (h *zzWP7aRefHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// TestZZWP7aExpiryHeapMatchesContainerHeap drives the typed heap and
// container/heap with the same random pushes, pops and rebuilds: they
// must keep the same slice, element for element, since the typed
// operations are container/heap's sift steps without the interface.
func TestZZWP7aExpiryHeapMatchesContainerHeap(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	var got expiryHeap
	var want zzWP7aRefHeap
	for step := range 50_000 {
		switch op := rng.IntN(20); {
		case op < 10 || len(got) == 0:
			e := expiryEntry{offset: int64(step), expiresAtUnixMs: rng.Int64N(1000), nonce: rng.Int64()}
			got.push(e)
			heap.Push(&want, e)
		case op < 19:
			if g, w := got.pop(), heap.Pop(&want).(expiryEntry); g != w {
				t.Fatalf("step %d: pop = %+v, container/heap popped %+v", step, g, w)
			}
		default:
			rng.Shuffle(len(got), func(i, j int) {
				got[i], got[j] = got[j], got[i]
				want[i], want[j] = want[j], want[i]
			})
			got.init()
			heap.Init(&want)
		}
		if len(got) != len(want) {
			t.Fatalf("step %d: len %d, want %d", step, len(got), len(want))
		}
		if len(got) > 0 && got[0] != want[0] {
			t.Fatalf("step %d: head %+v, want %+v", step, got[0], want[0])
		}
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("final heaps differ at %d: %+v vs %+v", i, got[i], want[i])
		}
	}
}

// TestZZWP7aDropTopicDropsEveryShard: DropTopic on the lock-free shard
// table drops every shard of the topic, tells the drop notifier about
// each, and leaves other topics alone.
func TestZZWP7aDropTopicDropsEveryShard(t *testing.T) {
	f := NewInFlight(fixedCaps(8, 8), nil)
	var dropped []int
	f.SetDropNotifier(func(topic string, partition int) {
		if topic == testTopic {
			dropped = append(dropped, partition)
		}
	})
	for p := range 4 {
		if err := f.Init(t.Context(), testTopic, p, int64(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Init(t.Context(), "other", 0, 0); err != nil {
		t.Fatal(err)
	}
	f.DropTopic(testTopic)
	if len(dropped) != 4 {
		t.Fatalf("drop notifier saw %v, want four partitions", dropped)
	}
	for p := range 4 {
		if f.shard(testTopic, p) != nil {
			t.Fatalf("partition %d still has a shard", p)
		}
	}
	if f.shard("other", 0) == nil {
		t.Fatal("another topic's shard was dropped")
	}
}
