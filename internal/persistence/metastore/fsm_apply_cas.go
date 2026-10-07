package metastore

// Compare-and-set topic writes. opUpdateTopic, opDeleteTopic,
// opPutSchema, opAttachChild and opDetachChild act on whatever topic
// holds the name when they apply. The leader checks a write against the
// record it read under the topic's lock, and a Raft barrier per term
// keeps that read current, but nothing tied the entry to that record: a
// write checked against one incarnation of a name could land on another
// (a topic deleted and recreated in between, or a read from a replica
// that had not applied the previous leader's last writes), and a config
// update was a blind overwrite of the whole record that could shrink
// the partition count or rewrite the owner.
//
// The entry types here carry the incarnation each topic was read as and
// are refused (errs.ErrTopicChanged, nothing written) when the stored
// record is another one. Each apply is a pure function of the
// transaction and the payload: IDs, epochs and times come from the
// proposer.

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// expectIncarnation refuses when the stored record is not incarnation
// expected.
func expectIncarnation(current topic.Topic, expected string) error {
	if current.ID == expected {
		return nil
	}
	return topicChanged(current.Name, expected, current.ID)
}

// applyUpdateTopicIf applies a config update to the incarnation it was
// read from. Partitions only grow; the incarnation, owner, creation
// time, visibility timeout and fan-out link stay as stored: no config
// update changes them, so a value in the payload that differs was read
// from a stale copy.
func (f *fsmState) applyUpdateTopicIf(data []byte) error {
	var p updateTopicIfPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	t := p.Topic
	err := f.update(func(tx *bolt.Tx) error {
		current, err := getTopicRecord(tx, t.Name)
		if err != nil {
			return err
		}
		if err := expectIncarnation(current, p.ExpectID); err != nil {
			return err
		}
		if t.Partitions < current.Partitions {
			return fmt.Errorf("%w: topic %q has %d partitions but the update carries %d: it was built from an earlier read and would shrink the topic; read it again",
				errs.ErrTopicChanged, t.Name, current.Partitions, t.Partitions)
		}
		next := t
		next.ID = current.ID
		next.Owner = current.Owner
		next.CreatedAt = current.CreatedAt
		next.VisibilityTimeoutMs = current.VisibilityTimeoutMs
		next.Role = current.Role
		next.Children = current.Children
		next.Parent = current.Parent
		next.AttachEpoch = current.AttachEpoch
		next.FanoutDelayMs = current.FanoutDelayMs
		next.AttachOffsets = current.AttachOffsets
		next.Remote = current.Remote
		if err := checkConfigUpdate(tx, current, next); err != nil {
			return err
		}
		return putTopicRecord(tx, next)
	})
	if err == nil {
		f.versions.bumpTopic(t.Name)
	}
	return err
}

// applyDeleteTopicIf deletes the topic, exactly as applyDeleteTopic
// does, if it is the incarnation the delete was checked against.
func (f *fsmState) applyDeleteTopicIf(data []byte) error {
	var p deleteTopicIfPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var linkedTopics, deletedStubs []string
	err := f.update(func(tx *bolt.Tx) error {
		current, err := getTopicRecord(tx, p.Name)
		if err != nil {
			return err
		}
		if err := expectIncarnation(current, p.ExpectID); err != nil {
			return err
		}
		linkedTopics, deletedStubs, err = deleteTopicTx(tx, p.Name)
		return err
	})
	if err == nil {
		f.retireDeletedTopic(p.Name, linkedTopics, deletedStubs)
	}
	return err
}

// applyPutSchemaIf appends a schema version to the incarnation it was
// checked against, and to every fan-out child's copy, as applyPutSchema
// does, within the schema byte budgets (each child's copy counts
// against the child).
func (f *fsmState) applyPutSchemaIf(data []byte) error {
	var p putSchemaIfPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var childTopics []string
	err := f.update(func(tx *bolt.Tx) error {
		childTopics = nil
		t, err := getTopicRecord(tx, p.Topic)
		if err != nil {
			return err
		}
		if err := expectIncarnation(t, p.ExpectID); err != nil {
			return err
		}
		if t.IsChild() {
			return fmt.Errorf("%w: %q is attached to %q", errs.ErrFanoutSchemaManaged, p.Topic, t.Parent)
		}
		if err := checkNextSchemaVersion(tx, p.Topic, p.Version); err != nil {
			return err
		}
		// A remote child's stub holds no schema (see applyPutSchema).
		children, err := localChildren(tx, t.Children)
		if err != nil {
			return err
		}
		added := map[string]int64{p.Topic: int64(len(p.Schema))}
		for _, child := range children {
			if err := checkNextSchemaVersion(tx, child, p.Version); err != nil {
				return err
			}
			added[child] += int64(len(p.Schema))
		}
		if err := f.schemaBudget.check(tx, added); err != nil {
			return err
		}
		b := tx.Bucket(bucketSchemas)
		if err := b.Put(schemaKey(p.Topic, p.Version), p.Schema); err != nil {
			return err
		}
		for _, child := range children {
			if err := b.Put(schemaKey(child, p.Version), p.Schema); err != nil {
				return err
			}
		}
		childTopics = children
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

// applyAttachChildIf links a fan-out child, as applyAttachChild does,
// if parent and child are the incarnations the attach was checked
// against. The schema histories compare by JSON value, and a history
// the child adopts counts against the schema byte budgets.
func (f *fsmState) applyAttachChildIf(data []byte) error {
	var p attachChildIfPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	schemaAdopted := false
	err := f.update(func(tx *bolt.Tx) error {
		schemaAdopted = false
		if err := expectIncarnations(tx, p.Link.Parent, p.ParentID, p.Link.Child, p.ChildID); err != nil {
			return err
		}
		budgets := f.schemaBudget
		var err error
		schemaAdopted, err = attachChildTx(tx, p.Link, attachRules{byValue: true, budgets: &budgets})
		return err
	})
	if err == nil {
		f.bumpAttached(p.Link.Parent, p.Link.Child, schemaAdopted)
	}
	return err
}

// applyDetachChildIf unlinks a fan-out child, as applyDetachChild does,
// if parent and child are the incarnations the detach was checked
// against.
func (f *fsmState) applyDetachChildIf(data []byte) error {
	var p detachChildIfPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	stubDeleted := false
	err := f.update(func(tx *bolt.Tx) error {
		if err := expectIncarnations(tx, p.Parent, p.ParentID, p.Child, p.ChildID); err != nil {
			return err
		}
		parent, err := getTopicRecord(tx, p.Parent)
		if err != nil {
			return err
		}
		child, err := getTopicRecord(tx, p.Child)
		if err != nil {
			return err
		}
		stubDeleted, err = detachChildTx(tx, p.Parent, p.Child, parent, child)
		return err
	})
	if err == nil {
		f.bumpDetached(p.Parent, p.Child, stubDeleted)
	}
	return err
}

// expectIncarnations refuses unless both topics exist as the expected
// incarnations: ErrNotFound for a missing one, errs.ErrTopicChanged for
// another incarnation.
func expectIncarnations(tx *bolt.Tx, parent, parentID, child, childID string) error {
	for _, side := range [...]struct{ name, id string }{{parent, parentID}, {child, childID}} {
		t, err := getTopicRecord(tx, side.name)
		if err != nil {
			return err
		}
		if err := expectIncarnation(t, side.id); err != nil {
			return err
		}
	}
	return nil
}
