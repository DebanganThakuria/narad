package controller

// The planner is the policy heart of rebalance: it must move the FEWEST
// partitions that still balances count, converge to a fixpoint (a second
// plan over the result is empty), and treat decommission as balancing with
// a zero-capacity node. These tests pin each property.

import (
	"fmt"
	"sort"
	"testing"
)

// buildLoad turns "node: n partitions" into Load + Movable, naming
// partitions t/0, t/1, ... uniquely across all nodes so refs never collide.
func buildLoad(counts map[string]int) ([]string, map[string]int, map[string][]PartitionRef) {
	nodes := make([]string, 0, len(counts))
	for n := range counts {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	load := map[string]int{}
	movable := map[string][]PartitionRef{}
	idx := 0
	for _, n := range nodes {
		load[n] = counts[n]
		for i := 0; i < counts[n]; i++ {
			movable[n] = append(movable[n], PartitionRef{Topic: "t", Partition: idx})
			idx++
		}
	}
	return nodes, load, movable
}

// apply mutates load/movable as if every move completed, so a follow-up
// plan can check convergence.
func apply(load map[string]int, movable map[string][]PartitionRef, moves []RebalanceMove) {
	for _, m := range moves {
		load[m.From]--
		load[m.To]++
		// move the ref from From's movable slice to To's
		src := movable[m.From]
		for i, p := range src {
			if p == m.Partition {
				movable[m.From] = append(src[:i:i], src[i+1:]...)
				break
			}
		}
		movable[m.To] = append(movable[m.To], m.Partition)
	}
}

func countsOf(load map[string]int, nodes []string) []int {
	out := make([]int, len(nodes))
	for i, n := range nodes {
		out[i] = load[n]
	}
	return out
}

func spread(counts []int) int {
	if len(counts) == 0 {
		return 0
	}
	lo, hi := counts[0], counts[0]
	for _, c := range counts {
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	return hi - lo
}

func TestPlanBalancesNewNodeWithMinimalMoves(t *testing.T) {
	// Three nodes each own 4; a fourth joins empty. 12 over 4 = 3 each.
	nodes, load, movable := buildLoad(map[string]int{"a": 4, "b": 4, "c": 4, "d": 0})
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes})

	// Minimal: exactly 3 partitions move (one off each of a,b,c onto d).
	if len(moves) != 3 {
		t.Fatalf("moves = %d, want 3 (minimal)", len(moves))
	}
	for _, m := range moves {
		if m.To != "d" {
			t.Fatalf("move %v should target the empty node d", m)
		}
	}
	apply(load, movable, moves)
	if got := countsOf(load, nodes); spread(got) != 0 {
		t.Fatalf("after plan counts = %v, want all equal", got)
	}
}

func TestPlanConvergesToFixpoint(t *testing.T) {
	nodes, load, movable := buildLoad(map[string]int{"a": 10, "b": 2, "c": 0, "d": 0})
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes})
	apply(load, movable, moves)

	// 12 over 4 = 3 each; spread must be 0 and a re-plan must be empty.
	if got := countsOf(load, nodes); spread(got) != 0 {
		t.Fatalf("counts after plan = %v, want balanced", got)
	}
	if again := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes}); len(again) != 0 {
		t.Fatalf("re-plan of a balanced cluster returned %d moves, want 0", len(again))
	}
}

func TestPlanUnevenRemainderIsMinimal(t *testing.T) {
	// 10 over 3 = counts {4,3,3}. Start {10,0,0}: b,c each need ~3.
	nodes, load, movable := buildLoad(map[string]int{"a": 10, "b": 0, "c": 0})
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes})
	apply(load, movable, moves)

	got := countsOf(load, nodes)
	if spread(got) > 1 {
		t.Fatalf("counts = %v, want spread <= 1", got)
	}
	// a keeps the ceil (4), so only 6 move.
	if len(moves) != 6 {
		t.Fatalf("moves = %d, want 6 (a keeps its ceil share)", len(moves))
	}
}

func TestPlanAlreadyBalancedIsNoop(t *testing.T) {
	nodes, load, movable := buildLoad(map[string]int{"a": 3, "b": 3, "c": 3})
	if moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes}); len(moves) != 0 {
		t.Fatalf("balanced cluster planned %d moves, want 0", len(moves))
	}
}

func TestPlanDecommissionDrainsNode(t *testing.T) {
	// c is draining: absent from Receivers, so all its partitions move off
	// and the two receivers end up balanced.
	_, load, movable := buildLoad(map[string]int{"a": 3, "b": 3, "c": 4})
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: []string{"a", "b"}})

	if len(moves) != 4 {
		t.Fatalf("moves = %d, want 4 (all of c's partitions)", len(moves))
	}
	for _, m := range moves {
		if m.From != "c" {
			t.Fatalf("move %v: decommission must only shed from c", m)
		}
		if m.To != "a" && m.To != "b" {
			t.Fatalf("move %v: must target a live receiver", m)
		}
	}
	apply(load, movable, moves)
	if load["c"] != 0 {
		t.Fatalf("c still owns %d after drain, want 0", load["c"])
	}
	if got := countsOf(load, []string{"a", "b"}); spread(got) != 0 {
		t.Fatalf("receivers unbalanced after drain: %v", got)
	}
}

func TestPlanInFlightNotReplanned(t *testing.T) {
	// a owns 4 settled, b owns 4 settled, c is empty, and ONE of a's
	// partitions is already in-flight to c (counted at c, excluded from
	// movable). 9 total over 3 = 3 each. The planner must not move that
	// in-flight partition again, and must reach balance counting it at c.
	load := map[string]int{"a": 4, "b": 4, "c": 1}
	movable := map[string][]PartitionRef{
		"a": {{Topic: "t", Partition: 0}, {Topic: "t", Partition: 1}, {Topic: "t", Partition: 2}, {Topic: "t", Partition: 3}},
		"b": {{Topic: "t", Partition: 4}, {Topic: "t", Partition: 5}, {Topic: "t", Partition: 6}, {Topic: "t", Partition: 7}},
		// partition 8 is in-flight a→c: absent from movable, counted in load[c].
	}
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: []string{"a", "b", "c"}})

	for _, m := range moves {
		if m.Partition.Partition == 8 {
			t.Fatalf("in-flight partition 8 was re-planned: %v", m)
		}
	}
	apply(load, movable, moves)
	if got := countsOf(load, []string{"a", "b", "c"}); spread(got) != 0 {
		t.Fatalf("counts after plan = %v, want balanced (in-flight counted at c)", got)
	}
}

func TestPlanAvoidsDiscouragedColocation(t *testing.T) {
	// a sheds 2 (cap 2); b and c each have one open slot. Avoid steers
	// partition 0 away from b — with c an equally-deficient alternative, the
	// plan must place partition 0 on c, not b.
	_, load, movable := buildLoad(map[string]int{"a": 4, "b": 0, "c": 0})
	avoid := func(m RebalanceMove) bool { return m.To == "b" && m.Partition.Partition == 0 }
	moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: []string{"a", "b", "c"}, Avoid: avoid})

	for _, m := range moves {
		if m.Partition.Partition == 0 && m.To == "b" {
			t.Fatalf("planner placed partition 0 on b despite an equal alternative: %v", m)
		}
	}
	apply(load, movable, moves)
	if got := countsOf(load, []string{"a", "b", "c"}); spread(got) > 1 {
		t.Fatalf("avoid preference broke balance: %v", got)
	}
}

func TestPlanNoReceiversIsNoop(t *testing.T) {
	_, load, movable := buildLoad(map[string]int{"a": 3})
	if moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nil}); moves != nil {
		t.Fatalf("no receivers must yield no moves, got %v", moves)
	}
}

// A larger randomized-ish sweep: many starting shapes must all converge to
// spread<=1 in one plan and be a fixpoint thereafter.
func TestPlanConvergenceSweep(t *testing.T) {
	shapes := []map[string]int{
		{"a": 7, "b": 0, "c": 0, "d": 0},
		{"a": 5, "b": 5, "c": 0},
		{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
		{"a": 100, "b": 1, "c": 1},
		{"a": 3, "b": 3, "c": 3, "d": 3, "e": 0},
	}
	for i, shape := range shapes {
		t.Run(fmt.Sprintf("shape%d", i), func(t *testing.T) {
			nodes, load, movable := buildLoad(shape)
			moves := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes})
			apply(load, movable, moves)
			if s := spread(countsOf(load, nodes)); s > 1 {
				t.Fatalf("did not converge: spread %d, counts %v", s, countsOf(load, nodes))
			}
			if again := PlanRebalance(PlanInput{Load: load, Movable: movable, Receivers: nodes}); len(again) != 0 {
				t.Fatalf("not a fixpoint: %d further moves", len(again))
			}
		})
	}
}
