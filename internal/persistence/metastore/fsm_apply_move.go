package metastore

// FSM handlers for the partition-move state machine (rebalance /
// decommission). The controller sets a Target on an assignment; a
// caught-up destination flips ownership via a guarded compare-and-swap;
// a stale move is aborted. All three preserve the single-owner
// invariant: OwnerID names exactly one node at every point in the log.

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// applySetAssignmentTarget records (or clears, when TargetID=="") the
// move target on an existing assignment. Owner is untouched — the
// partition keeps being served by its current owner until the flip.
func (f *fsmState) applySetAssignmentTarget(data []byte) error {
	var p assignmentTargetPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var changed bool
	err := f.update(func(tx *bolt.Tx) error {
		a, err := readAssignment(tx, p.Topic, p.Partition)
		if err != nil {
			return err
		}
		if a.TargetID == p.TargetID {
			return nil // idempotent
		}
		a.TargetID = p.TargetID
		changed = true
		return putAssignment(tx, a)
	})
	if err == nil && changed {
		f.versions.bumpAssignment(p.Topic)
	}
	return err
}

// applyCompleteMove is the atomic ownership flip, guarded as a
// compare-and-swap: it sets OwnerID=TargetID and clears the target ONLY
// if the current owner still equals ExpectedOwner and the target still
// equals TargetID. A precondition miss returns an error so the proposing
// destination knows the flip did not happen (owner changed, or the move
// was retargeted/aborted) and can discard its copy.
func (f *fsmState) applyCompleteMove(data []byte) error {
	var p completeMovePayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		a, err := readAssignment(tx, p.Topic, p.Partition)
		if err != nil {
			return err
		}
		if a.OwnerID != p.ExpectedOwner {
			return fmt.Errorf("%w: complete-move owner is %q, expected %q", errs.ErrInvalidArgument, a.OwnerID, p.ExpectedOwner)
		}
		if a.TargetID != p.TargetID {
			return fmt.Errorf("%w: complete-move target is %q, expected %q", errs.ErrInvalidArgument, a.TargetID, p.TargetID)
		}
		a.OwnerID = p.TargetID
		a.TargetID = ""
		return putAssignment(tx, a)
	})
	if err == nil {
		f.versions.bumpAssignment(p.Topic)
	}
	return err
}

// applyAbortMove clears a move target, but only if it still matches
// ExpectedTarget — so aborting a stale move never clobbers a target a
// re-plan just installed. Owner is untouched.
func (f *fsmState) applyAbortMove(data []byte) error {
	var p abortMovePayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var changed bool
	err := f.update(func(tx *bolt.Tx) error {
		a, err := readAssignment(tx, p.Topic, p.Partition)
		if err != nil {
			return err
		}
		if a.TargetID == "" || a.TargetID != p.ExpectedTarget {
			return nil // already cleared, or retargeted — no-op
		}
		a.TargetID = ""
		changed = true
		return putAssignment(tx, a)
	})
	if err == nil && changed {
		f.versions.bumpAssignment(p.Topic)
	}
	return err
}

// readAssignment loads an assignment inside a txn, ErrNotFound if absent.
func readAssignment(tx *bolt.Tx, topicName string, partition int) (Assignment, error) {
	v := tx.Bucket(bucketAssignments).Get(assignmentKey(topicName, partition))
	if v == nil {
		return Assignment{}, errs.ErrNotFound
	}
	var a Assignment
	if err := json.Unmarshal(v, &a); err != nil {
		return Assignment{}, err
	}
	return a, nil
}

func putAssignment(tx *bolt.Tx, a Assignment) error {
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return tx.Bucket(bucketAssignments).Put(assignmentKey(a.Topic, a.Partition), v)
}
