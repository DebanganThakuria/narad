package messaging

// Reclaiming the stale local copy a completed rebalance move leaves on the
// OLD owner. After the ownership flip the source's partition directory is
// harmless (nothing routes to a non-owner) but wastes disk until reclaimed.
// Deleting partition data is the most dangerous operation in the broker —
// without replication a wrong delete destroys the only copy — so the guard
// here is AFFIRMATIVE: the assignment must be readable and must name
// another live home for the partition. Any doubt refuses.

import (
	"context"
	"fmt"
	"os"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ReclaimMovedPartition deletes this node's local copy of a partition that
// a completed move relocated to another node. It refuses unless the local
// metastore replica AFFIRMATIVELY shows the partition owned by a different
// node with no move targeting this node — an unreadable assignment, an
// unassigned partition, or any reference to this node all refuse. The open
// log (if any) is closed first so no writer resurrects the directory.
//
// The caller (the move runner's sweep) additionally confirms the same view
// with the Raft LEADER before calling; this method's own check is defense
// in depth against a caller with a stale view.
func (e *Engine) ReclaimMovedPartition(ctx context.Context, topicName string, partition int) error {
	if e.logs == nil {
		return unavailableError("partition logs")
	}
	if e.selfID == "" {
		// No cluster identity: this process owns everything; there is no
		// such thing as a moved-away partition.
		return fmt.Errorf("%w: no cluster identity", ErrInvalid)
	}
	assignment, err := e.getAssignment(topicName, partition)
	if err != nil {
		return fmt.Errorf("reclaim refused: assignment unreadable: %w", err)
	}
	if assignment.OwnerID == "" || assignment.OwnerID == e.selfID || assignment.TargetID == e.selfID {
		return fmt.Errorf("%w: partition is (or is becoming) locally owned", ErrInvalid)
	}
	if err := e.logs.ClosePartition(topicName, partition); err != nil {
		return fmt.Errorf("reclaim: close partition log: %w", err)
	}
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("reclaim: remove partition dir: %w", err)
	}
	return nil
}
