package metastore

// The apply* handlers below run on Raft's FSM goroutine for every
// committed log entry, on every node. They must stay deterministic:
// identical inputs must produce identical bbolt state and identical
// business errors. Domain version bumps happen only after a successful
// update, outside the bbolt transaction.

import (
	"bytes"
	"encoding/json"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
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

func (f *fsmState) applyUpdateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(t.Name)) == nil {
			return ErrNotFound
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

// applyDeleteTopic removes the topic together with all of its schemas
// and partition assignments in a single transaction.
func (f *fsmState) applyDeleteTopic(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(name)) == nil {
			return ErrNotFound
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
	}
	return err
}

func (f *fsmState) applyPutSchema(data []byte) error {
	var p schemaPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchemas).Put(schemaKey(p.Topic, p.Version), p.Schema)
	})
	if err == nil {
		f.versions.bumpSchema(p.Topic)
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
