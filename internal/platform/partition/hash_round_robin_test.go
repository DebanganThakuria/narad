package partition

import (
	"fmt"
	"testing"
)

func TestHashRoundRobinPickReturnsZeroForNonPositivePartitions(t *testing.T) {
	m := NewHashRoundRobin()

	if got := m.Pick("orders", "key", 0); got != 0 {
		t.Fatalf("Pick() with zero partitions = %d, want 0", got)
	}
	if got := m.Pick("orders", "key", -1); got != 0 {
		t.Fatalf("Pick() with negative partitions = %d, want 0", got)
	}
	if got := m.Pick("orders", "", 0); got != 0 {
		t.Fatalf("keyless Pick() with zero partitions = %d, want 0", got)
	}
}

func TestHashRoundRobinPickUsesStableHashForKeyedMessages(t *testing.T) {
	m := NewHashRoundRobin()

	first := m.Pick("orders", "customer-42", 8)
	for range 10 {
		if got := m.Pick("orders", "customer-42", 8); got != first {
			t.Fatalf("Pick() changed partition for same key: first=%d got=%d", first, got)
		}
	}
}

// A keyed pick is the FNV-1a hash of the key modulo the partition
// count, on every topic: fan-out relies on a key landing on the same
// partition index in a parent and its child.
func TestHashRoundRobinPickHashesKeysTheSameOnEveryTopic(t *testing.T) {
	m := NewHashRoundRobin()

	for _, key := range []string{"customer-42", "key-1", "key-zz", "a"} {
		want := m.Pick("orders", key, 12)
		if got := m.Pick("orders-replica", key, 12); got != want {
			t.Fatalf("Pick(%q) = %d on the child, %d on the parent", key, got, want)
		}
	}
	// Pinned values: the hash itself must not change across releases,
	// or a keyed record's parent and child copies would part ways
	// during a rolling upgrade.
	for key, want := range map[string]int{"customer-42": 10, "key-zz": 7, "a": 4} {
		if got := m.Pick("orders", key, 12); got != want {
			t.Fatalf("Pick(%q, 12) = %d, want %d", key, got, want)
		}
	}
}

func TestHashRoundRobinPickUsesRoundRobinForUnkeyedMessages(t *testing.T) {
	m := NewHashRoundRobin()

	first := m.Pick("orders", "", 3)
	for i := 1; i < 6; i++ {
		want := (first + i) % 3
		if got := m.Pick("orders", "", 3); got != want {
			t.Fatalf("Pick() call %d = %d, want %d", i, got, want)
		}
	}
}

// A producer that rotates through several topics must not steer every
// keyless message of one topic onto one partition: each topic keeps its
// own round-robin, however its picks interleave with other topics'.
// With a node-wide cursor, 3 topics of 3 partitions written A, B, C, A,
// B, C put every A message on one partition.
func TestHashRoundRobinPickKeepsARoundRobinPerTopic(t *testing.T) {
	for _, tc := range []struct{ topics, partitions int }{
		{topics: 3, partitions: 3},
		{topics: 2, partitions: 4},
		{topics: 4, partitions: 2},
		{topics: 6, partitions: 12},
	} {
		t.Run(fmt.Sprintf("topics=%d/partitions=%d", tc.topics, tc.partitions), func(t *testing.T) {
			m := NewHashRoundRobin()
			rounds := 10 * tc.partitions
			counts := make([][]int, tc.topics)
			for i := range counts {
				counts[i] = make([]int, tc.partitions)
			}
			for range rounds {
				for i := range tc.topics {
					counts[i][m.Pick(fmt.Sprintf("topic-%d", i), "", tc.partitions)]++
				}
			}
			for i, perPartition := range counts {
				for p, n := range perPartition {
					if n != rounds/tc.partitions {
						t.Fatalf("topic-%d per-partition counts %v: partition %d got %d, want %d each",
							i, perPartition, p, n, rounds/tc.partitions)
					}
				}
			}
		})
	}
}

// Each topic's round-robin starts at a random partition, so nodes (and
// a restarted node) do not all send a topic's first keyless message to
// partition 0.
func TestHashRoundRobinStartsEachTopicAtARandomPartition(t *testing.T) {
	m := NewHashRoundRobin()

	firsts := map[int]bool{}
	for i := range 64 {
		firsts[m.Pick(fmt.Sprintf("topic-%d", i), "", 3)] = true
	}
	if len(firsts) < 2 {
		t.Fatalf("64 topics all started at partition %v, want random starting partitions", firsts)
	}
}

// The zero value is ready for use, as it was before cursors were per
// topic.
func TestHashRoundRobinZeroValueIsUsable(t *testing.T) {
	var m HashRoundRobin

	first := m.Pick("orders", "", 4)
	if got := m.Pick("orders", "", 4); got != (first+1)%4 {
		t.Fatalf("second pick = %d, want %d", got, (first+1)%4)
	}
}

// BenchmarkHashRoundRobinPick is the partitioner alone: a keyed pick
// (hash), a keyless pick on one topic, and keyless picks rotating over
// 8 topics, serial and from parallel callers.
func BenchmarkHashRoundRobinPick(b *testing.B) {
	topics := make([]string, 8)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic-%d", i)
	}
	b.Run("keyed", func(b *testing.B) {
		m := NewHashRoundRobin()
		for b.Loop() {
			m.Pick("orders", "customer-42", 12)
		}
	})
	b.Run("keyless/one-topic", func(b *testing.B) {
		m := NewHashRoundRobin()
		for b.Loop() {
			m.Pick("orders", "", 12)
		}
	})
	b.Run("keyless/8-topics", func(b *testing.B) {
		m := NewHashRoundRobin()
		i := 0
		for b.Loop() {
			m.Pick(topics[i&7], "", 12)
			i++
		}
	})
	b.Run("keyless/one-topic/parallel", func(b *testing.B) {
		m := NewHashRoundRobin()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Pick("orders", "", 12)
			}
		})
	})
	b.Run("keyless/8-topics/parallel", func(b *testing.B) {
		m := NewHashRoundRobin()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Pick(topics[i&7], "", 12)
				i++
			}
		})
	})
}
