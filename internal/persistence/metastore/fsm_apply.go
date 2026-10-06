package metastore

// The apply* handlers below run on Raft's FSM goroutine for every
// committed log entry, on every node. They must stay deterministic:
// identical inputs must produce identical bbolt state and identical
// business errors. Domain version bumps happen only after a successful
// update, outside the bbolt transaction.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

func (f *fsmState) applyCreateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(t.Name)) != nil {
			return ErrAlreadyExists
		}
		v, err := json.Marshal(t)
		if err != nil {
			return err
		}
		return b.Put([]byte(t.Name), v)
	})
	if err == nil {
		f.versions.bumpTopic(t.Name)
	}
	return err
}

// applyUpdateTopic overwrites the topic's config. The incarnation ID
// and the fan-out link fields (Role/Children/Parent) are preserved
// from the stored record:
// they change only through attach/detach/delete, so a read-modify-write
// config update that raced an attach on another node cannot clobber
// the link.
func (f *fsmState) applyUpdateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		current, err := getTopicRecord(tx, t.Name)
		if err != nil {
			return err
		}
		// The incarnation ID is fixed at create: a proposer that read
		// an older copy of the record (or an older binary that does
		// not know the field) must not clear or replace it, or the
		// on-disk directories stamped with it would read as stale.
		t.ID = current.ID
		t.Role = current.Role
		t.Children = current.Children
		t.Parent = current.Parent
		t.AttachEpoch = current.AttachEpoch
		t.FanoutDelayMs = current.FanoutDelayMs
		t.AttachOffsets = current.AttachOffsets
		t.Remote = current.Remote
		if err := checkConfigUpdate(tx, current, t); err != nil {
			return err
		}
		return putTopicRecord(tx, t)
	})
	if err == nil {
		f.versions.bumpTopic(t.Name)
	}
	return err
}

// applyDeleteTopic removes the topic together with all of its schemas
// and partition assignments in a single transaction. Fan-out links are
// dissolved rather than cascaded: deleting a parent detaches all of
// its children (they keep their data and schemas and become
// standalone), and deleting a child unlinks it from its parent.
func (f *fsmState) applyDeleteTopic(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	var linkedTopics, deletedStubs []string
	err := f.update(func(tx *bolt.Tx) error {
		var err error
		linkedTopics, deletedStubs, err = deleteTopicTx(tx, name)
		return err
	})
	if err == nil {
		f.retireDeletedTopic(name, linkedTopics, deletedStubs)
	}
	return err
}

// retireDeletedTopic advances the versions a topic delete changed.
// It retires rather than bumps the deleted name's three versions: they
// advance exactly as the bumps would, and its cells become tombstones
// that are pruned in batches, so a churn of uniquely named topics does
// not leave a cell per name ever deleted. The fan-out partners stay live
// and are bumped; a deleted parent's remote children went with it and
// are retired too.
func (f *fsmState) retireDeletedTopic(name string, linkedTopics, deletedStubs []string) {
	f.versions.retireTopic(name)
	for _, stub := range deletedStubs {
		f.versions.retireTopic(stub)
	}
	for _, linked := range linkedTopics {
		f.versions.bumpTopic(linked)
	}
}

// deleteTopicTx removes the topic record, its schemas and its
// assignment rows, and dissolves its fan-out links. It returns the
// other ends of those links and the remote children's stubs deleted
// with a parent.
func deleteTopicTx(tx *bolt.Tx, name string) (linked, deletedStubs []string, err error) {
	if tx.Bucket(bucketTopics).Get([]byte(name)) == nil {
		return nil, nil, ErrNotFound
	}
	linked, deletedStubs, err = dissolveFanoutLinks(tx, name)
	if err != nil {
		return nil, nil, err
	}
	if err := deleteTopicRecords(tx, name); err != nil {
		return nil, nil, err
	}
	return linked, deletedStubs, nil
}

// deleteTopicRecords removes the topic record with its schemas and
// partition assignments.
func deleteTopicRecords(tx *bolt.Tx, name string) error {
	if err := tx.Bucket(bucketTopics).Delete([]byte(name)); err != nil {
		return err
	}
	if _, err := deletePrefix(tx.Bucket(bucketSchemas), name+":"); err != nil {
		return err
	}
	_, err := deletePrefix(tx.Bucket(bucketAssignments), name+":")
	return err
}

// checkConfigUpdate is what a config update (opUpdateTopic,
// opUpdateTopicIf) checks beyond its own rules, given the stored record
// and the one it would store (link fields already carried over).
//
// A remote child's stub has no config of its own to change: its
// partitions would turn it into a local child whose cursor advances
// without sending anything, and pause and resume have their own op. An
// update that changes nothing still applies.
//
// A parent's retained log is the delay buffer for its delay children:
// shrinking retention below what an attached child's delay requires
// would let scheduled records age out before they are due. It is also
// its remote children's only buffer while a remote is down, so a shrink
// below the remote floor is refused; a retention already below it may
// still grow.
func checkConfigUpdate(tx *bolt.Tx, current, next topic.Topic) error {
	if current.IsRemoteChild() {
		same, err := sameTopicRecord(next, current)
		if err != nil {
			return err
		}
		if !same {
			return errs.RemoteChildError(errs.ErrRemoteStubImmutable, fmt.Sprintf(
				"%q is a remote child of %q; its topic record cannot be changed (pause and resume have their own routes)",
				next.Name, current.Parent))
		}
	}
	if !next.IsParent() {
		return nil
	}
	hasRemote := false
	for _, childName := range next.Children {
		child, err := getTopicRecord(tx, childName)
		if err != nil {
			continue
		}
		if err := checkDelayAgainstRetention(child.FanoutDelayMs, next.RetentionMs, next.Name); err != nil {
			return err
		}
		hasRemote = hasRemote || child.IsRemoteChild()
	}
	if hasRemote && retentionShrinks(current.RetentionMs, next.RetentionMs) {
		return checkRemoteSourceRetention(next.RetentionMs, next.Name)
	}
	return nil
}

// sameTopicRecord reports whether a and b encode to the same record.
func sameTopicRecord(a, b topic.Topic) (bool, error) {
	ra, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	rb, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ra, rb), nil
}

// retentionShrinks reports whether next keeps records for less time
// than current (0 keeps them forever).
func retentionShrinks(current, next int64) bool {
	switch {
	case next == 0:
		return false
	case current == 0:
		return true
	}
	return next < current
}

// deletePrefix deletes every key of b that starts with prefix and
// returns how many it deleted.
func deletePrefix(b *bolt.Bucket, prefix string) (int, error) {
	p := []byte(prefix)
	n := 0
	c := b.Cursor()
	for k, _ := c.Seek(p); k != nil && bytes.HasPrefix(k, p); k, _ = c.Next() {
		if err := c.Delete(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// dissolveFanoutLinks detaches every fan-out link involving the topic
// being deleted and returns the other endpoints so the caller can bump
// their versions. A deleted parent's remote children are deleted with
// it, their stubs returned apart: a stub standalone would be a topic
// with no partitions and no purpose. A linked record that is
// unexpectedly missing is skipped: the delete must not fail on an
// already-broken link.
func dissolveFanoutLinks(tx *bolt.Tx, name string) (linked, deletedStubs []string, err error) {
	t, err := getTopicRecord(tx, name)
	if err != nil {
		return nil, nil, err
	}
	if t.IsParent() {
		for _, childName := range t.Children {
			child, err := getTopicRecord(tx, childName)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			if child.IsRemoteChild() && child.Parent == name {
				if err := deleteTopicRecords(tx, childName); err != nil {
					return nil, nil, err
				}
				deletedStubs = append(deletedStubs, childName)
				continue
			}
			child.Role = topic.RoleStandalone
			child.Parent = ""
			child.AttachEpoch = ""
			child.FanoutDelayMs = 0
			child.AttachOffsets = nil
			if err := putTopicRecord(tx, child); err != nil {
				return nil, nil, err
			}
			linked = append(linked, childName)
		}
	}
	if t.IsChild() && t.Parent != "" {
		parent, err := getTopicRecord(tx, t.Parent)
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return nil, nil, err
		default:
			unlinkChild(&parent, name)
			if err := putTopicRecord(tx, parent); err != nil {
				return nil, nil, err
			}
			linked = append(linked, t.Parent)
		}
	}
	return linked, deletedStubs, nil
}

// unlinkChild removes child from parent's children; a parent whose last
// child goes reverts to standalone.
func unlinkChild(parent *topic.Topic, child string) {
	parent.Children = slices.DeleteFunc(parent.Children, func(c string) bool { return c == child })
	if len(parent.Children) == 0 {
		parent.Children = nil
		parent.Role = topic.RoleStandalone
	}
}

// applyPutSchema appends a schema version. An attached child's schema
// is parent-managed, so targeting one directly is rejected; a schema
// stored on a fan-out parent is propagated to every child in the same
// transaction so parent and child histories never drift.
//
// The version must be exactly one past the topic's persisted latest
// (and each child's). The proposer computes it from the persisted
// history it read, so a proposer working from a stale view (a leader
// whose local registry never saw the topic, or one that lost a race
// with another put) is refused instead of silently overwriting an
// earlier version: schema history is append-only and every replica
// applies the same decision. The payload format is unchanged, so log
// entries written before this rule replay exactly as they did.
func (f *fsmState) applyPutSchema(data []byte) error {
	var p schemaPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var childTopics []string
	err := f.update(func(tx *bolt.Tx) error {
		t, err := getTopicRecord(tx, p.Topic)
		if err != nil {
			return err
		}
		if t.IsChild() {
			return fmt.Errorf("%w: %q is attached to %q", errs.ErrFanoutSchemaManaged, p.Topic, t.Parent)
		}
		// A remote child's stub holds no schema: its records are
		// validated by the parent here and by the target's own schema
		// there.
		childTopics, err = localChildren(tx, t.Children)
		if err != nil {
			return err
		}
		if err := checkNextSchemaVersion(tx, p.Topic, p.Version); err != nil {
			return err
		}
		for _, child := range childTopics {
			if err := checkNextSchemaVersion(tx, child, p.Version); err != nil {
				return err
			}
		}
		b := tx.Bucket(bucketSchemas)
		if err := b.Put(schemaKey(p.Topic, p.Version), p.Schema); err != nil {
			return err
		}
		for _, child := range childTopics {
			if err := b.Put(schemaKey(child, p.Version), p.Schema); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		f.versions.bumpSchema(p.Topic)
		for _, child := range childTopics {
			f.versions.bumpSchema(child)
		}
	}
	return err
}

func (f *fsmState) applyAssignPartition(data []byte) error {
	var a Assignment
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}
	err = f.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put(assignmentKey(a.Topic, a.Partition), v)
	})
	if err == nil {
		f.versions.bumpAssignment(a.Topic)
	}
	return err
}

// applyMemberJoin upserts the member record. The routing-members version
// only advances when routing-relevant fields change, so heartbeat-only
// re-registrations do not invalidate route caches. A member removed by
// decommission (tombstoned) is refused with ErrMemberRemoved: the
// removed pod keeps heartbeating through the leader until it is torn
// down, and that heartbeat must not resurrect it as an alive, draining
// member forever.
func (f *fsmState) applyMemberJoin(data []byte) error {
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	routingChanged := false
	err := f.update(func(tx *bolt.Tx) error {
		if memberTombstoned(tx, m.ID) {
			return ErrMemberRemoved
		}
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(m.ID))
		if raw == nil {
			routingChanged = true
		} else {
			var current Member
			if err := json.Unmarshal(raw, &current); err != nil {
				return err
			}
			// A re-registration (restart, heartbeat re-register) carries the
			// join defaults, not the drain flag — preserve an in-progress
			// decommission so a node restarting mid-drain stays draining.
			m.Draining = current.Draining
			routingChanged = !sameRoutingMember(current, m)
		}
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put([]byte(m.ID), v)
	})
	if err == nil && routingChanged {
		f.versions.bumpRoutingMembers()
	}
	return err
}

func (f *fsmState) applySetMemberDraining(data []byte) error {
	var p memberDrainingPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(p.ID))
		if raw == nil {
			return ErrNotFound
		}
		var m Member
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		m.Draining = p.Draining
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put([]byte(p.ID), v)
	})
}

func (f *fsmState) applyMemberHeartbeat(data []byte) error {
	var p heartbeatPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(p.ID))
		if raw == nil {
			return ErrNotFound
		}
		var m Member
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		m.LastHeartbeat = p.At
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put([]byte(p.ID), v)
	})
}

func (f *fsmState) applyMemberDead(data []byte) error {
	var id string
	if err := json.Unmarshal(data, &id); err != nil {
		return err
	}
	routingChanged := false
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		var m Member
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		routingChanged = m.Status != MemberDead
		m.Status = MemberDead
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), v)
	})
	if err == nil && routingChanged {
		f.versions.bumpRoutingMembers()
	}
	return err
}

// sameRoutingMember compares only the fields routing depends on;
// LastHeartbeat is deliberately excluded.
func sameRoutingMember(a, b Member) bool {
	return a.ID == b.ID &&
		a.Addr == b.Addr &&
		a.ClusterAddr == b.ClusterAddr &&
		a.Status == b.Status
}

// localChildren filters remote children's stubs out of children. A
// child whose record is missing is kept, as the schema put always did.
func localChildren(tx *bolt.Tx, children []string) ([]string, error) {
	out := make([]string, 0, len(children))
	for _, name := range children {
		child, err := getTopicRecord(tx, name)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil && child.IsRemoteChild() {
			continue
		}
		out = append(out, name)
	}
	return out, nil
}
