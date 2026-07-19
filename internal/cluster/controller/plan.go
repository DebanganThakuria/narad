package controller

// The rebalance planner. Given the current partition placement and the set
// of nodes eligible to receive partitions, it computes the MINIMAL set of
// moves that balances owned partition count across those nodes. Pure and
// deterministic: the same inputs always yield the same plan, which is what
// makes the whole scheme level-triggered — the controller can recompute
// every tick and converge, never oscillate.
//
// Minimality. With T movable partitions over R receivers the balanced state
// gives each receiver either floor(T/R) or ceil(T/R). The number of moves
// equals the total surplus above capacity, which is the least any algorithm
// could do: a partition already on a node within its capacity is never
// touched. Nodes already holding the most keep the ceil slots, so we move
// off exactly the overflow and nothing more.
//
// Idempotency under in-flight moves. A partition mid-move (Owner=A,
// Target=B) is counted at B, not A, and is excluded from the movable pool.
// So a plan computed while moves are running already accounts for where
// those partitions are going and will not redundantly move more off A. When
// the in-flight moves complete the counts are unchanged — the plan has
// reached its fixpoint.
//
// Decommission is the same algorithm: a draining node is simply absent from
// the receiver set, so its capacity is zero and every partition it owns is
// shed to the receivers.

import "sort"

// PartitionRef identifies one partition instance.
type PartitionRef struct {
	Topic     string
	Partition int
}

// RebalanceMove is one planned relocation: the partition moves From→To.
type RebalanceMove struct {
	Partition PartitionRef
	From      string
	To        string
}

// PlanInput is the settled cluster placement the planner balances.
type PlanInput struct {
	// Load is the effective partition count per LIVE node: settled
	// partitions counted at their owner, in-flight ones at their target.
	// Dead nodes are absent (their partitions wait for the node to return
	// and rejoin a later plan). Must include every receiver, even at zero.
	Load map[string]int
	// Movable lists the settled partitions each node owns that MAY move
	// (owner alive, not already in-flight). In-flight partitions are
	// excluded so a running move is never re-planned.
	Movable map[string][]PartitionRef
	// Receivers are the nodes eligible to receive partitions: alive and
	// not draining. A node in Load/Movable but not here (a draining node)
	// gets capacity zero and sheds everything.
	Receivers []string
	// Avoid, if non-null, reports a move the planner should avoid when an
	// equally-balanced alternative exists (anti-affinity: keep a fan-out
	// child off its parent's node). It is a preference, never a hard
	// constraint — balance always wins.
	Avoid func(RebalanceMove) bool
}

// PlanRebalance returns the minimal moves that balance owned partition
// count across the receivers. Empty when already balanced or when no
// receiver can take work.
func PlanRebalance(in PlanInput) []RebalanceMove {
	receivers := append([]string(nil), in.Receivers...)
	sort.Strings(receivers)
	if len(receivers) == 0 {
		return nil
	}

	// Total movable load = everything on live nodes (receivers + draining).
	total := 0
	for _, n := range receivers {
		total += in.Load[n]
	}
	for node, count := range in.Load {
		if !contains(receivers, node) {
			total += count // draining node: its load must be absorbed
		}
	}

	caps := capacities(receivers, in.Load, total)

	// Shed pool: every partition a node holds beyond its capacity. Draining
	// nodes (capacity 0) shed all; over-capacity receivers shed the overflow.
	// Deterministic order (by node, then partition) keeps the plan stable.
	var pool []shedItem
	nodes := sortedKeys(in.Movable)
	for _, node := range nodes {
		surplus := len(in.Movable[node]) - caps[node] // caps[node]==0 for non-receivers
		if surplus <= 0 {
			continue
		}
		parts := append([]PartitionRef(nil), in.Movable[node]...)
		sort.Slice(parts, func(i, j int) bool { return less(parts[i], parts[j]) })
		for _, p := range parts[:surplus] {
			pool = append(pool, shedItem{from: node, part: p})
		}
	}
	if len(pool) == 0 {
		return nil
	}

	// Remaining open slots per receiver.
	deficit := make(map[string]int, len(receivers))
	for _, r := range receivers {
		if d := caps[r] - in.Load[r]; d > 0 {
			deficit[r] = d
		}
	}

	moves := make([]RebalanceMove, 0, len(pool))
	for _, item := range pool {
		to := pickReceiver(receivers, deficit, item, in.Avoid)
		if to == "" {
			break // no capacity left (shouldn't happen: pool size == total deficit)
		}
		deficit[to]--
		if deficit[to] == 0 {
			delete(deficit, to)
		}
		moves = append(moves, RebalanceMove{Partition: item.part, From: item.from, To: to})
	}
	return moves
}

type shedItem struct {
	from string
	part PartitionRef
}

// capacities assigns each receiver floor(total/R) plus, for the receivers
// currently holding the most, one of the R−remainder extra slots — so the
// nodes that already hold the ceil count keep it and don't shed needlessly.
func capacities(receivers []string, load map[string]int, total int) map[string]int {
	r := len(receivers)
	base, rem := total/r, total%r
	caps := make(map[string]int, r)
	for _, n := range receivers {
		caps[n] = base
	}
	// Hand the +1 slots to the highest-loaded receivers (ties broken by ID
	// for determinism).
	ranked := append([]string(nil), receivers...)
	sort.Slice(ranked, func(i, j int) bool {
		if load[ranked[i]] != load[ranked[j]] {
			return load[ranked[i]] > load[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})
	for i := 0; i < rem; i++ {
		caps[ranked[i]]++
	}
	return caps
}

// pickReceiver chooses the destination for one shed partition: the receiver
// with the most remaining deficit, avoiding a discouraged move when an
// equally-deficient alternative exists. Deterministic tie-break by ID.
func pickReceiver(receivers []string, deficit map[string]int, item shedItem, avoid func(RebalanceMove) bool) string {
	best, bestDef := "", 0
	bestAvoided := true
	for _, r := range receivers {
		d := deficit[r]
		if d <= 0 || r == item.from {
			continue
		}
		avoided := avoid != nil && avoid(RebalanceMove{Partition: item.part, From: item.from, To: r})
		// Prefer: not-avoided over avoided; then more deficit; then lower ID.
		better := best == ""
		if !better && bestAvoided != avoided {
			better = !avoided // a non-avoided candidate beats an avoided one
		} else if !better && d != bestDef {
			better = d > bestDef
		}
		if better {
			best, bestDef, bestAvoided = r, d, avoided
		}
	}
	return best
}

func contains(sorted []string, s string) bool {
	i := sort.SearchStrings(sorted, s)
	return i < len(sorted) && sorted[i] == s
}

func sortedKeys(m map[string][]PartitionRef) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func less(a, b PartitionRef) bool {
	if a.Topic != b.Topic {
		return a.Topic < b.Topic
	}
	return a.Partition < b.Partition
}
