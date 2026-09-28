package partition

import (
	"fmt"
	"sync"
	"testing"
)

// Topic churn (thousands of topics created, produced to and deleted,
// each under a new name) must not grow the cursor table without bound,
// and a topic that keeps producing through the churn keeps its exact
// round-robin.
func TestHashRoundRobinBoundsCursorsUnderTopicChurn(t *testing.T) {
	const limit = 64
	m := &HashRoundRobin{limit: limit}

	const partitions = 3
	counts := make([]int, partitions)
	const rounds = 30_000
	for i := range rounds {
		counts[m.Pick("steady", "", partitions)]++
		m.Pick(fmt.Sprintf("churn-%d", i), "", 8)
		if n := m.trackedCursors(); n > 2*limit {
			t.Fatalf("after %d churned topics the table tracks %d cursors, want at most %d", i+1, n, 2*limit)
		}
	}
	for p, n := range counts {
		if n != rounds/partitions {
			t.Fatalf("steady topic per-partition counts %v: partition %d got %d, want %d each",
				counts, p, n, rounds/partitions)
		}
	}
}

// The default limit bounds the table the same way.
func TestHashRoundRobinDefaultLimitBoundsCursors(t *testing.T) {
	m := NewHashRoundRobin()

	for i := range 5 * defaultCursorLimit {
		m.Pick(fmt.Sprintf("churn-%d", i), "", 8)
	}
	if n := m.trackedCursors(); n > 2*defaultCursorLimit {
		t.Fatalf("after %d churned topics the table tracks %d cursors, want at most %d",
			5*defaultCursorLimit, n, 2*defaultCursorLimit)
	}
}

// Concurrent keyless picks over many topics, with generations retiring
// underneath them, stay in range and keep every topic evenly spread
// (run under -race).
func TestHashRoundRobinConcurrentPicksAcrossGenerations(t *testing.T) {
	m := &HashRoundRobin{limit: 8}

	const (
		workers    = 8
		partitions = 4
		perWorker  = 4000
	)
	var mu sync.Mutex
	counts := make([]int, partitions)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			local := make([]int, partitions)
			for i := range perWorker {
				p := m.Pick("steady", "", partitions)
				if p < 0 || p >= partitions {
					t.Errorf("Pick() = %d, out of [0, %d)", p, partitions)
					return
				}
				local[p]++
				if q := m.Pick(fmt.Sprintf("churn-%d-%d", w, i), "", partitions); q < 0 || q >= partitions {
					t.Errorf("Pick() = %d, out of [0, %d)", q, partitions)
					return
				}
			}
			mu.Lock()
			for p, n := range local {
				counts[p] += n
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	if n := m.trackedCursors(); n > 2*(8+workers) {
		t.Fatalf("table tracks %d cursors, want at most %d", n, 2*(8+workers))
	}
	// Concurrent retirements can hand a topic a fresh cursor now and
	// then (a racing miss creates one), so the spread is near even
	// rather than exact.
	want := workers * perWorker / partitions
	for p, n := range counts {
		if n < want*9/10 || n > want*11/10 {
			t.Fatalf("steady topic per-partition counts %v: partition %d got %d, want about %d", counts, p, n, want)
		}
	}
}

// trackedCursors is how many topic cursors both generations hold.
func (h *HashRoundRobin) trackedCursors() int {
	n := 0
	for _, gen := range []*cursorGeneration{h.cur.Load(), h.prev.Load()} {
		if gen == nil {
			continue
		}
		gen.cursors.Range(func(any, any) bool {
			n++
			return true
		})
	}
	return n
}
