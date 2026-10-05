package metastore

// Insert-only placement and orphan-row pruning. opAssignPartition is a
// blind write: it records an owner for any topic name and partition
// index, replacing whatever owner is on record. A placement pass that
// listed a topic before it was deleted wrote rows for the gone topic,
// which a later topic of the same name inherited, and a pass computed
// from a replica that lagged the previous leader's placements replaced
// an owner that might already hold the partition's records.
//
// opAssignPartitionIfAbsent places a partition of the incarnation the
// placement was computed for, inside its range, on a member that was not
// removed, and only while the partition has no owner on record. Moves
// still change owners through opCompleteMove. opPruneAssignment deletes
// a row that belongs to no partition (its topic is gone, or the index is
// at or beyond the partition count) and refuses a live one, so a prune
// computed from a lagging read never removes an owner.

import (
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrPartitionAssigned is the refusal of an insert-only placement whose
// partition already has an owner on record. Benign for a placement
// pass: the partition is placed.
var ErrPartitionAssigned = errors.New("metastore: partition already has an owner on record")

// ErrAssignmentLive is the refusal of a prune whose row belongs to a
// partition of an existing topic.
var ErrAssignmentLive = errors.New("metastore: assignment row belongs to a live partition")

func (f *fsmState) applyAssignPartitionIfAbsent(data []byte) error {
	var p assignIfAbsentPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		t, err := getTopicRecord(tx, p.Topic)
		if err != nil {
			return fmt.Errorf("cannot place %s/%d: %w", p.Topic, p.Partition, err)
		}
		if err := expectIncarnation(t, p.ExpectID); err != nil {
			return err
		}
		switch {
		case p.Partition < 0 || p.Partition >= t.Partitions:
			return fmt.Errorf("%w: cannot place %s/%d: the topic has %d partitions", errs.ErrInvalidArgument, p.Topic, p.Partition, t.Partitions)
		case p.OwnerID == "":
			return fmt.Errorf("%w: cannot place %s/%d: no owner named", errs.ErrInvalidArgument, p.Topic, p.Partition)
		case memberTombstoned(tx, p.OwnerID):
			return fmt.Errorf("%w: cannot place %s/%d on %q: the member was removed from the cluster", errs.ErrInvalidArgument, p.Topic, p.Partition, p.OwnerID)
		}
		b := tx.Bucket(bucketAssignments)
		key := assignmentKey(p.Topic, p.Partition)
		if b.Get(key) != nil {
			return fmt.Errorf("%w: %s/%d; placement never replaces an owner", ErrPartitionAssigned, p.Topic, p.Partition)
		}
		v, err := json.Marshal(Assignment{Topic: p.Topic, Partition: p.Partition, OwnerID: p.OwnerID})
		if err != nil {
			return err
		}
		return b.Put(key, v)
	})
	if err == nil {
		f.versions.bumpAssignment(p.Topic)
	}
	return err
}

func (f *fsmState) applyPruneAssignment(data []byte) error {
	var p pruneAssignmentPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	topicGone := false
	err := f.update(func(tx *bolt.Tx) error {
		topicGone = false
		b := tx.Bucket(bucketAssignments)
		key := assignmentKey(p.Topic, p.Partition)
		if b.Get(key) == nil {
			return fmt.Errorf("%w: no assignment row for %s/%d", ErrNotFound, p.Topic, p.Partition)
		}
		t, err := getTopicRecord(tx, p.Topic)
		switch {
		case errors.Is(err, ErrNotFound):
			topicGone = true
		case err != nil:
			return err
		case p.Partition >= 0 && p.Partition < t.Partitions:
			return fmt.Errorf("%w: %s/%d (the topic has %d partitions); only rows that belong to no partition are pruned",
				ErrAssignmentLive, p.Topic, p.Partition, t.Partitions)
		}
		return b.Delete(key)
	})
	if err == nil {
		if topicGone {
			f.versions.retireAssignments(p.Topic)
		} else {
			f.versions.bumpAssignment(p.Topic)
		}
	}
	return err
}
