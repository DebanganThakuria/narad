// Package partition selects the partition a produce request lands on.
package partition

// Manager picks a partition index in [0, partitions) for the given
// topic and key, or 0 when partitions <= 0. Implementations must be safe
// for concurrent use.
type Manager interface {
	Pick(topic, key string, partitions int) int
}
