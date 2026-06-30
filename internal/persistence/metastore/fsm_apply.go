package metastore

import (
	"bytes"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func (f *fsmState) applyCreateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return fmt.Errorf("metastore: decode create_topic: %w", err)
	}
	err := f.update("create_topic", func(tx *bolt.Tx) error {
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
		return fmt.Errorf("metastore: decode update_topic: %w", err)
	}
	err := f.update("update_topic", func(tx *bolt.Tx) error {
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

func (f *fsmState) applyDeleteTopic(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return fmt.Errorf("metastore: decode delete_topic: %w", err)
	}
	err := f.update("delete_topic", func(tx *bolt.Tx) error {
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
		return fmt.Errorf("metastore: decode put_schema: %w", err)
	}
	err := f.update("put_schema", func(tx *bolt.Tx) error {
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
		return fmt.Errorf("metastore: decode assign_partition: %w", err)
	}
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}
	err = f.update("assign_partition", func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put(assignmentKey(a.Topic, a.Partition), v)
	})
	if err == nil {
		f.versions.bumpAssignment(a.Topic)
	}
	return err
}

func (f *fsmState) applyMemberJoin(data []byte) error {
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("metastore: decode member_join: %w", err)
	}
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	routingChanged := false
	err = f.update("member_join", func(tx *bolt.Tx) error {
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
	return f.bumpRoutingOnSuccess(err, routingChanged)
}

func (f *fsmState) applyMemberHeartbeat(data []byte) error {
	var p heartbeatPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return fmt.Errorf("metastore: decode member_heartbeat: %w", err)
	}
	return f.update("member_heartbeat", func(tx *bolt.Tx) error {
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
		return fmt.Errorf("metastore: decode member_dead: %w", err)
	}
	routingChanged := false
	err := f.update("member_dead", func(tx *bolt.Tx) error {
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
	return f.bumpRoutingOnSuccess(err, routingChanged)
}

// bumpRoutingOnSuccess advances the routing-members version when a member
// mutation committed (err == nil) and changed routing-relevant fields. It
// returns err unchanged so callers can propagate it directly.
func (f *fsmState) bumpRoutingOnSuccess(err error, routingChanged bool) error {
	if err == nil && routingChanged {
		f.versions.bumpRoutingMembers()
	}
	return err
}

func sameRoutingMember(a, b Member) bool {
	return a.ID == b.ID &&
		a.Addr == b.Addr &&
		a.ClusterAddr == b.ClusterAddr &&
		a.Status == b.Status
}
