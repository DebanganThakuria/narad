package partition

import (
	"hash/fnv"
	"sync/atomic"
)

// HashRoundRobin is the default Manager: FNV-1a hash for keyed
// messages, atomic round-robin for unkeyed ones.
type HashRoundRobin struct {
	cursor atomic.Uint64
}

// NewHashRoundRobin returns a Manager ready for use.
func NewHashRoundRobin() *HashRoundRobin {
	return &HashRoundRobin{}
}

// Pick returns the partition index for (topic, key). The topic argument
// is unused today; it's kept on the interface for future per-topic
// strategies (e.g. sticky partitioning).
func (h *HashRoundRobin) Pick(_ string, key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}
	if key == "" {
		n := h.cursor.Add(1) - 1
		return int(n % uint64(partitions))
	}
	hh := fnv.New32a()
	_, _ = hh.Write([]byte(key))
	return int(hh.Sum32() % uint32(partitions))
}
