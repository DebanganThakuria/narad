package metastore

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
	return f.update("create_topic", func(tx *bolt.Tx) error {
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
}

func (f *fsmState) applyUpdateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	return f.update("update_topic", func(tx *bolt.Tx) error {
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
}

func (f *fsmState) applyDeleteTopic(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	return f.update("delete_topic", func(tx *bolt.Tx) error {
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
}

func (f *fsmState) applyPutSchema(data []byte) error {
	var p schemaPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return f.update("put_schema", func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchemas).Put(schemaKey(p.Topic, p.Version), p.Schema)
	})
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
	return f.update("assign_partition", func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put(assignmentKey(a.Topic, a.Partition), v)
	})
}

func (f *fsmState) applyMemberJoin(data []byte) error {
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return f.update("member_join", func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMembers).Put([]byte(m.ID), v)
	})
}

func (f *fsmState) applyMemberHeartbeat(data []byte) error {
	var p heartbeatPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
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
		return err
	}
	return f.update("member_dead", func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMembers)
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		var m Member
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		m.Status = MemberDead
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), v)
	})
}
