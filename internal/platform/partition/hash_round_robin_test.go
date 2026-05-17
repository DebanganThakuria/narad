package partition

import "testing"

func TestHashRoundRobinPickReturnsZeroForNonPositivePartitions(t *testing.T) {
	m := NewHashRoundRobin()

	if got := m.Pick("orders", "key", 0); got != 0 {
		t.Fatalf("Pick() with zero partitions = %d, want 0", got)
	}
	if got := m.Pick("orders", "key", -1); got != 0 {
		t.Fatalf("Pick() with negative partitions = %d, want 0", got)
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

func TestHashRoundRobinPickUsesRoundRobinForUnkeyedMessages(t *testing.T) {
	m := NewHashRoundRobin()

	for i := range 6 {
		want := i % 3
		if got := m.Pick("orders", "", 3); got != want {
			t.Fatalf("Pick() call %d = %d, want %d", i, got, want)
		}
	}
}
