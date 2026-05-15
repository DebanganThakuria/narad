// Package partition selects the partition a produce request lands on.
package partition

// Manager picks a partition index in [0, partitions) for the given
// topic and key. Implementations must be safe for concurrent use.
type Manager interface {
	Pick(topic, key string, partitions int) int
}
