package messaging

import (
	"fmt"
	"sort"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// assignmentReader is the optional metastore capability for partition
// ownership lookups (implemented by *metastore.Store). Metastores
// without it — test fakes, embedded use — make every partition local.
type assignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
}

// localPartitions returns the sorted partitions this node owns for the
// topic, or every partition when the node has no cluster identity or
// the metastore has no assignment support. A nil, nil result means the
// node verifiably owns nothing.
func (e *Engine) localPartitions(topicName string, totalPartitions int) ([]int, error) {
	if e.selfID == "" {
		return allPartitions(totalPartitions), nil
	}
	if _, ok := e.metastore.(assignmentReader); !ok {
		return allPartitions(totalPartitions), nil
	}
	rows, err := e.listAssignments(topicName)
	if err != nil {
		// Not a routing verdict: surface as an internal error so callers
		// retry, instead of coercing a metastore hiccup into "owns
		// nothing" (which reads as ErrNotPartitionOwner).
		return nil, fmt.Errorf("messaging: list assignments for %s: %w", topicName, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	owned := ownerPartitions(rows, e.selfID)
	if len(owned) == 0 {
		return nil, nil
	}
	return sortPartitions(owned), nil
}

// localProbePartitions resolves the partitions a queue-mode Consume
// may scan: the pinned partition (after range and ownership checks) or
// every locally owned partition.
func (e *Engine) localProbePartitions(topicName string, totalPartitions int, pinnedPartition *int) ([]int, error) {
	if pinnedPartition != nil {
		if *pinnedPartition < 0 || *pinnedPartition >= totalPartitions {
			return nil, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		if !e.isLocalOwner(topicName, *pinnedPartition) {
			return nil, ErrNotPartitionOwner
		}
		return []int{*pinnedPartition}, nil
	}
	scan, err := e.localPartitions(topicName, totalPartitions)
	if err != nil {
		return nil, err
	}
	if len(scan) == 0 {
		return nil, ErrNotPartitionOwner
	}
	return scan, nil
}

// pickProducePartition picks the produce target for a key, then walks
// forward past partitions whose owner is missing or not alive so a
// dead node doesn't blackhole its share of the keyspace.
func (e *Engine) pickProducePartition(topicName, key string, partitions int) (int, error) {
	start := e.partitions.Pick(topicName, key, partitions)
	if _, ok := e.metastore.(assignmentReader); !ok {
		return start, nil
	}
	for i := range partitions {
		candidate := (start + i) % partitions
		assignment, err := e.getAssignment(topicName, candidate)
		if err != nil {
			continue
		}
		if !e.produceAssignmentWritable(assignment) {
			continue
		}
		return candidate, nil
	}
	return 0, fmt.Errorf("messaging: no alive partition owner for topic %s", topicName)
}

// produceAssignmentWritable reports whether the assignment's owner is
// registered and alive.
func (e *Engine) produceAssignmentWritable(assignment metastore.Assignment) bool {
	// A partition this node owns but has paused for a rebalance handoff
	// is momentarily not writable — produce reroutes to a live partition.
	if assignment.OwnerID == e.selfID && e.isProducePaused(assignment.Topic, assignment.Partition) {
		return false
	}
	owner, err := e.getRoutingMember(assignment.OwnerID)
	return err == nil && owner.Status == metastore.MemberAlive
}

// isWritableLocalProducePartition reports whether this node owns the
// partition and is alive per the metastore, i.e. may accept a pinned
// synchronous produce for it.
func (e *Engine) isWritableLocalProducePartition(topicName string, partition int) bool {
	if e.selfID == "" {
		return true
	}
	if _, ok := e.metastore.(assignmentReader); !ok {
		return true
	}
	assignment, err := e.getAssignment(topicName, partition)
	if err != nil {
		return false
	}
	return assignment.OwnerID == e.selfID && e.produceAssignmentWritable(assignment)
}

// isLocalOwner reports whether this node owns the partition. Nodes
// without a cluster identity, or metastores without assignment
// support, own everything.
func (e *Engine) isLocalOwner(topicName string, partition int) bool {
	if e.selfID == "" {
		return true
	}
	if _, ok := e.metastore.(assignmentReader); !ok {
		return true
	}
	assignment, err := e.getAssignment(topicName, partition)
	if err != nil {
		return false
	}
	return assignment.OwnerID == e.selfID
}

// ownerPartitions filters assignments down to the partitions owned by
// ownerID.
func ownerPartitions(assignments []metastore.Assignment, ownerID string) []int {
	owned := make([]int, 0, len(assignments))
	for _, assignment := range assignments {
		if assignment.OwnerID != ownerID {
			continue
		}
		owned = append(owned, assignment.Partition)
	}
	return owned
}

func sortPartitions(partitions []int) []int {
	out := append([]int(nil), partitions...)
	sort.Ints(out)
	return out
}

// allPartitions returns [0, n).
func allPartitions(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
