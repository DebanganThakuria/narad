// Package partition selects the partition a produce request lands on.
//
// The PRD specifies hash(key) % N for keyed messages. For unkeyed
// messages we round-robin so producers don't pile up on partition 0.
package partition

// Manager picks a partition index in [0, partitions) for the given
// topic and key. Implementations must be safe for concurrent use.
type Manager interface {
	Pick(topic, key string, partitions int) int
}
