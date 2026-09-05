package messaging

// Reclaiming the stale local copy a completed rebalance move leaves on the
// OLD owner. After the ownership flip the source's partition directory is
// harmless (nothing routes to a non-owner) but wastes disk until reclaimed.
// Deleting partition data is the most dangerous operation in the broker —
// without replication a wrong delete destroys the only copy — so the guard
// here is AFFIRMATIVE: the assignment must be readable and must name
// another live home for the partition. Any doubt refuses.
//
// Even an affirmative "owned elsewhere" is not proof the local copy holds
// nothing unique: a force-promote promotes the destination's copy at the
// source's LAST-SEEN high-watermark, and a source that merely lost contact
// (a long network partition, not a crash) kept committing past it. When
// such a source returns, its copy is AHEAD of the promoted position, and
// deleting it would destroy the only copy of those records. So a reclaim
// that knows the promoted HWM compares first, and QUARANTINES (renames)
// instead of deleting when the local copy is ahead.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ErrPartitionQuarantined reports that a reclaim found the local copy
// ahead of the position the partition was promoted at elsewhere and set
// it aside instead of deleting it. An operator must reconcile the
// quarantined directory by hand.
var ErrPartitionQuarantined = errors.New("stale partition copy quarantined: local copy is ahead of the promoted high-watermark")

// QuarantineSuffix is appended to a partition directory's name when a
// reclaim sets it aside instead of deleting it.
const QuarantineSuffix = ".quarantine"

// ReclaimGuard carries what the caller learned about where the partition
// went. Known=false means the current owner reported no move marker (an
// older node, or a copy not installed by a move); the reclaim then
// deletes as it always did.
type ReclaimGuard struct {
	// PromotedHWM is the high-watermark at which the current owner's copy
	// was installed by the move that relocated the partition there.
	PromotedHWM int64
	Known       bool
}

// ReclaimMovedPartition deletes this node's local copy of a partition that
// a completed move relocated to another node, with no knowledge of the
// promoted position. See ReclaimMovedPartitionGuarded.
func (e *Engine) ReclaimMovedPartition(ctx context.Context, topicName string, partition int) error {
	return e.ReclaimMovedPartitionGuarded(ctx, topicName, partition, ReclaimGuard{})
}

// ReclaimMovedPartitionGuarded deletes this node's local copy of a
// partition that a completed move relocated to another node. It refuses
// unless the local metastore replica AFFIRMATIVELY shows the partition
// owned by a different node with no move targeting this node; an
// unreadable assignment, an unassigned partition, or any reference to
// this node all refuse. The open log (if any) is closed first so no
// writer resurrects the directory.
//
// When guard.Known, the local copy is recovered and its next offset
// compared with guard.PromotedHWM: a copy that is AHEAD holds records the
// new owner never received, so it is renamed to <dir>.quarantine (never
// deleted) and ErrPartitionQuarantined is returned.
//
// The caller (the move runner's sweep) additionally confirms the same view
// with the Raft LEADER before calling; this method's own check is defense
// in depth against a caller with a stale view.
func (e *Engine) ReclaimMovedPartitionGuarded(ctx context.Context, topicName string, partition int, guard ReclaimGuard) error {
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
	// The in-memory reservation shard would otherwise outlive the data and
	// be resumed verbatim if the partition ever moved back here.
	e.ResetPartitionConsumerState(topicName, partition)
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	if guard.Known {
		next, err := recoveredNextOffset(dir)
		if err != nil {
			return fmt.Errorf("reclaim refused: recover local copy: %w", err)
		}
		if next > guard.PromotedHWM {
			quarantined, qerr := quarantinePartitionDir(dir)
			if qerr != nil {
				return fmt.Errorf("reclaim refused: local copy is ahead of the promoted hwm (%d > %d) and quarantine failed: %w", next, guard.PromotedHWM, qerr)
			}
			e.logger.Error("reclaim: local partition copy is AHEAD of the position it was promoted at elsewhere; quarantined instead of deleted, records after the promoted hwm exist only here",
				"topic", topicName, "partition", partition, "owner", assignment.OwnerID,
				"local_next_offset", next, "promoted_hwm", guard.PromotedHWM, "quarantine_dir", quarantined)
			return fmt.Errorf("%w: %s/%d local next offset %d > promoted hwm %d, kept at %s",
				ErrPartitionQuarantined, topicName, partition, next, guard.PromotedHWM, quarantined)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("reclaim: remove partition dir: %w", err)
	}
	return nil
}

// recoveredNextOffset reopens a closed partition directory and reports
// the offset its log recovers to: an upper bound on what the copy holds.
// A directory with no log is empty (next offset 0).
func recoveredNextOffset(dir string) (int64, error) {
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	log, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		return 0, err
	}
	next := log.NextOffset()
	return next, log.Close()
}

// quarantinePartitionDir renames dir to dir + QuarantineSuffix, picking a
// timestamped name if that already exists, and returns the new path.
func quarantinePartitionDir(dir string) (string, error) {
	target := dir + QuarantineSuffix
	if _, err := os.Stat(target); err == nil {
		target = dir + QuarantineSuffix + "." + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if err := os.Rename(dir, target); err != nil {
		return "", err
	}
	return target, nil
}

// ResetPartitionConsumerState drops this node's in-memory reservation
// state for a partition so the next consume rebuilds it from the
// persisted consumer.offset. The move runner calls it on the destination
// after installing a copied partition (a node that owned the partition
// earlier may still hold the old shard), and reclaim calls it on the
// source.
func (e *Engine) ResetPartitionConsumerState(topicName string, partition int) {
	if e.offsets != nil {
		e.offsets.DropPartition(topicName, partition)
	}
}
