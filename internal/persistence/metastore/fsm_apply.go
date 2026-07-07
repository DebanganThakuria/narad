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

// applyUpdateTopic overwrites the topic's config. The fan-out link
// fields (Role/Children/Parent) are preserved from the stored record:
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
		t.Role = current.Role
		t.Children = current.Children
		t.Parent = current.Parent
		t.AttachEpoch = current.AttachEpoch
		t.FanoutDelayMs = current.FanoutDelayMs
		// A parent's retained log is the delay buffer for its delay
		// children: shrinking retention below what an attached child's
		// delay requires would let scheduled records age out before
		// they are due.
		if t.IsParent() {
			for _, childName := range t.Children {
				child, err := getTopicRecord(tx, childName)
				if err != nil {
					continue
				}
				if err := checkDelayAgainstRetention(child.FanoutDelayMs, t.RetentionMs, t.Name); err != nil {
					return err
				}
			}
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
	var linkedTopics []string
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(name)) == nil {
			return ErrNotFound
		}
		var err error
		linkedTopics, err = dissolveFanoutLinks(tx, name)
		if err != nil {
			return err
		}
		if err := b.Delete([]byte(name)); err != nil {
			return err
		}
		prefix := []byte(name + ":")
		sc := tx.Bucket(bucketSchemas).Cursor()
		for k, _ := sc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = sc.Next() {
			if err := sc.Delete(); err != nil {
				return err
			}
		}
		ac := tx.Bucket(bucketAssignments).Cursor()
		for k, _ := ac.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = ac.Next() {
			if err := ac.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		f.versions.bumpTopic(name)
		f.versions.bumpAssignment(name)
		f.versions.bumpSchema(name)
		for _, linked := range linkedTopics {
			f.versions.bumpTopic(linked)
		}
	}
	return err
}

// dissolveFanoutLinks detaches every fan-out link involving the topic
// being deleted and returns the other endpoints so the caller can bump
// their versions. A linked record that is unexpectedly missing is
// skipped: the delete must not fail on an already-broken link.
func dissolveFanoutLinks(tx *bolt.Tx, name string) ([]string, error) {
	t, err := getTopicRecord(tx, name)
	if err != nil {
		return nil, err
	}
	var linked []string
	if t.IsParent() {
		for _, childName := range t.Children {
			child, err := getTopicRecord(tx, childName)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			child.Role = topic.RoleStandalone
			child.Parent = ""
			child.AttachEpoch = ""
			child.FanoutDelayMs = 0
			if err := putTopicRecord(tx, child); err != nil {
				return nil, err
			}
			linked = append(linked, childName)
		}
	}
	if t.IsChild() && t.Parent != "" {
		parent, err := getTopicRecord(tx, t.Parent)
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return nil, err
		default:
			parent.Children = slices.DeleteFunc(parent.Children, func(c string) bool { return c == name })
			if len(parent.Children) == 0 {
				parent.Children = nil
				parent.Role = topic.RoleStandalone
			}
			if err := putTopicRecord(tx, parent); err != nil {
				return nil, err
			}
			linked = append(linked, t.Parent)
		}
	}
	return linked, nil
}

// applyPutSchema stores a schema version. An attached child's schema
// is parent-managed, so targeting one directly is rejected; a schema
// stored on a fan-out parent is propagated to every child in the same
// transaction so parent and child histories never drift.
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
		childTopics = t.Children
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
// re-registrations do not invalidate route caches.
func (f *fsmState) applyMemberJoin(data []byte) error {
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	routingChanged := false
	err = f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(m.ID))
		if raw == nil {
			routingChanged = true
		} else {
			var current Member
			if err := json.Unmarshal(raw, &current); err != nil {
				return err
			}
			routingChanged = !sameRoutingMember(current, m)
		}
		return b.Put([]byte(m.ID), v)
	})
	if err == nil && routingChanged {
		f.versions.bumpRoutingMembers()
	}
	return err
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
