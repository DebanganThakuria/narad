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

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ErrPartitionQuarantined reports that a reclaim set the local copy aside
// instead of deleting it: the copy is ahead of the position the partition
// was promoted at elsewhere, or the new owner cannot vouch for it. An
// operator must reconcile the quarantined directory by hand.
var ErrPartitionQuarantined = errors.New("stale partition copy quarantined instead of deleted")

// QuarantineSuffix is appended to a partition directory's name when a
// reclaim sets it aside instead of deleting it.
const QuarantineSuffix = ".quarantine"

// ReclaimGuard carries what the caller learned about where the partition
// went. Known=false is the plain ReclaimMovedPartition, which deletes
// without comparing; the move runner's stale-copy sweep never sends it:
// it compares the new owner's listing with the local copy first and
// always sends a KNOWN guard, at the position the owner vouches for, or
// with SetAside when the owner cannot vouch for the copy.
type ReclaimGuard struct {
	// PromotedHWM is the position the current owner vouches for: the
	// high-watermark its copy was installed at by the move that relocated
	// the partition there, or its own lower one.
	PromotedHWM int64
	Known       bool
	// SetAside, when set, says why the new owner cannot vouch for the
	// local copy (it lists no records, holds records that did not come
	// from this copy, or lacks part of what it was given). The copy may
	// hold the only instance of its records, so it is renamed to
	// <dir>.quarantine without being recovered, never deleted.
	SetAside string
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
// writer resurrects the directory. The directory is then acted on under
// the topic's guard, and only while the topic marker is absent or names
// the incarnation the reclaim read: a directory of another incarnation
// under the path (a recreate this node served meanwhile) refuses.
//
// When guard.Known, the local copy is recovered and its next offset
// compared with guard.PromotedHWM: a copy that is AHEAD holds records the
// new owner never received, so it is renamed to <dir>.quarantine (never
// deleted) and ErrPartitionQuarantined is returned. When guard.SetAside
// is set, the new owner cannot vouch for the copy at all: it is renamed
// to <dir>.quarantine without being recovered, and
// ErrPartitionQuarantined is returned.
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
	// The incarnation this reclaim is about. The directory is acted on
	// only under the topic's guard and while its marker still names this
	// incarnation (or none): a delete and recreate of the name after the
	// checks below, with this node opening the successor's partition,
	// quarantines the old topic directory and makes the successor's
	// directory under the path, and removing or renaming that destroys
	// the successor's records and consumer state.
	//
	// The record is read BEFORE the assignment, so the assignment the
	// reclaim approves belongs to this incarnation or a later one. Read
	// the other way round, a delete and recreate applied between the two
	// reads made the record the successor's while the approved assignment
	// was the deleted incarnation's: the marker check below then passed on
	// the successor's own directory, and the reclaim removed it. In this
	// order a later incarnation's assignment either names this node
	// (refused below) or another node, and then any successor directory
	// this node made carries a marker other than rec.ID, which the check
	// under the guard refuses.
	rec, err := e.getTopic(ctx, topicName)
	if err != nil {
		return fmt.Errorf("reclaim refused: topic unreadable: %w", err)
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
	// ReplacePartitionDir holds the partition's produce mutex and the
	// topic's guard across fn, with the partition's log closed, so no
	// open of the name runs between the marker check and the removal.
	return e.logs.ReplacePartitionDir(topicName, partition, func() error {
		marker, marked, err := storage.ReadTopicIncarnation(storage.TopicDir(e.logs.DataDir(), topicName))
		if err != nil {
			return fmt.Errorf("reclaim refused: read topic incarnation: %w", err)
		}
		if marked && marker != rec.ID {
			return fmt.Errorf("%w: topic directory belongs to incarnation %s, not %s", ErrInvalid, marker, rec.ID)
		}
		return e.reclaimPartitionDirGuarded(topicName, partition, assignment.OwnerID, guard)
	})
}

// reclaimPartitionDirGuarded is the reclaim's action on the partition
// directory: set it aside when the owner cannot vouch for it, quarantine a
// copy ahead of guard.PromotedHWM, remove it otherwise. Caller holds the
// topic's guard with the log closed.
func (e *Engine) reclaimPartitionDirGuarded(topicName string, partition int, owner string, guard ReclaimGuard) error {
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	if guard.SetAside != "" {
		// Not recovered first: a copy the owner cannot vouch for is set
		// aside whatever it holds, a damaged one included.
		quarantined, err := QuarantinePartitionDir(dir)
		if err != nil {
			return fmt.Errorf("reclaim refused: the new owner cannot vouch for the local copy (%s) and quarantine failed: %w", guard.SetAside, err)
		}
		e.logger.Error("reclaim: the new owner cannot vouch for the local partition copy; quarantined instead of deleted, its records may exist only here",
			"topic", topicName, "partition", partition, "owner", owner, "reason", guard.SetAside, "quarantine_dir", quarantined)
		return fmt.Errorf("%w: %s/%d set aside at %s: %s", ErrPartitionQuarantined, topicName, partition, quarantined, guard.SetAside)
	}
	if guard.Known {
		next, err := RecoveredNextOffset(dir)
		if err != nil {
			return fmt.Errorf("reclaim refused: recover local copy: %w", err)
		}
		if next > guard.PromotedHWM {
			quarantined, qerr := QuarantinePartitionDir(dir)
			if qerr != nil {
				return fmt.Errorf("reclaim refused: local copy is ahead of the promoted hwm (%d > %d) and quarantine failed: %w", next, guard.PromotedHWM, qerr)
			}
			e.logger.Error("reclaim: local partition copy is AHEAD of the position it was promoted at elsewhere; quarantined instead of deleted, records after the promoted hwm exist only here",
				"topic", topicName, "partition", partition, "owner", owner,
				"local_next_offset", next, "promoted_hwm", guard.PromotedHWM, "quarantine_dir", quarantined)
			return fmt.Errorf("%w: %s/%d local next offset %d is ahead of the promoted hwm %d, kept at %s",
				ErrPartitionQuarantined, topicName, partition, next, guard.PromotedHWM, quarantined)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("reclaim: remove partition dir: %w", err)
	}
	return nil
}

// RecoveredNextOffset reopens a closed partition directory and reports
// the offset its log recovers to: an upper bound on what the copy holds.
// A directory with no log is empty (next offset 0). The caller must hold
// the partition's log closed (the reclaim and the move's install run it
// under ReplacePartitionDir).
func RecoveredNextOffset(dir string) (int64, error) {
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

// QuarantinePartitionDir renames dir to dir + QuarantineSuffix, picking a
// timestamped name if that already exists, and returns the new path. The
// stale-copy reclaim uses it, and so does a move's install when the
// partition's path holds records the incoming copy lacks.
func QuarantinePartitionDir(dir string) (string, error) {
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
// persisted consumer.offset and consumer.ahead. The move runner calls it
// on the destination before and after installing a copied partition (a
// node that owned the partition earlier may still hold the old shard,
// whose acks must not reach the copy), and reclaim calls it on the
// source.
//
// It also lifts any handoff freeze this node still holds for the
// partition. A node that receives a partition back within the freeze
// TTL of a move it sourced moments earlier (a rebalance onto a joining
// node, then that node's decommission returning the partition) would
// otherwise refuse every commit for it as a non-owner until the TTL
// lapsed: a minute-long fan-out and produce stall seen on every
// join-then-decommission cycle. The freeze only ever protects a move
// this node is the source of; at install this node is the destination,
// and at reclaim the move is over.
func (e *Engine) ResetPartitionConsumerState(topicName string, partition int) {
	if e.offsets != nil {
		e.offsets.DropPartition(topicName, partition)
	}
	e.ResumeProduce(topicName, partition)
}

// InstallPartitionDir runs swap, which replaces the partition's directory
// with a copied one, with the partition's open log closed and the
// topic's open guard held (see runtime.Logs.ReplacePartitionDir). The
// move runner uses it on the destination so a log this node still had
// open from an earlier ownership can never serve or write through the
// replaced directory.
func (e *Engine) InstallPartitionDir(topicName string, partition int, swap func() error) error {
	if e.logs == nil {
		return swap()
	}
	return e.logs.ReplacePartitionDir(topicName, partition, swap)
}

// ReclaimOrphanTopicDir purges this node's directory of the deleted
// topic incarnation id, whose purge never reached this node, under the
// topic's guard and only while the local record shows the topic absent
// and the directory still carries id's marker (see
// runtime.Logs.ReclaimOrphanTopicDir). The move runner's periodic sweep
// calls it once the LEADER confirmed the incarnation gone; the broker
// facade reaches it through the embedded engine.
func (e *Engine) ReclaimOrphanTopicDir(topicName, id string) (bool, error) {
	if e.logs == nil {
		return false, unavailableError("partition logs")
	}
	return e.logs.ReclaimOrphanTopicDir(topicName, id)
}

// QuarantinedCopies takes the inventory of this node's quarantined
// copies and keeps it for the quarantine gauges (see
// runtime.Logs.QuarantinedCopies). The move runner's sweep calls it on
// its cadence; the broker facade reaches it through the embedded engine.
func (e *Engine) QuarantinedCopies() (runtime.QuarantineSummary, error) {
	if e.logs == nil {
		return runtime.QuarantineSummary{}, unavailableError("partition logs")
	}
	return e.logs.QuarantinedCopies()
}
