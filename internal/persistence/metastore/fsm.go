package metastore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrNotFound and ErrAlreadyExists are aliases of the canonical
// sentinels in internal/errs.
var (
	ErrNotFound      = errs.ErrNotFound
	ErrAlreadyExists = errs.ErrAlreadyExists
)

type opCode byte

const (
	opCreateTopic opCode = iota + 1
	opUpdateTopic
	opDeleteTopic
	opPutSchema
	opAssignPartition
	opMemberJoin
	opMemberHeartbeat
	opMemberDead
)

var (
	bucketTopics      = []byte("topics")
	bucketSchemas     = []byte("schemas")
	bucketAssignments = []byte("assignments")
	bucketMembers     = []byte("members")
)

// cmd is the envelope written to the Raft log.
type cmd struct {
	Op   opCode `json:"o"`
	Data []byte `json:"d"`
}

// schemaPayload is the body of an opPutSchema command.
type schemaPayload struct {
	Topic   string `json:"t"`
	Version int    `json:"v"`
	Schema  []byte `json:"s"`
}

func schemaKey(topicName string, version int) []byte {
	return []byte(fmt.Sprintf("%s:%d", topicName, version))
}

func assignmentKey(topicName string, partition int) []byte {
	return []byte(fmt.Sprintf("%s:%d", topicName, partition))
}

// heartbeatPayload is the body of an opMemberHeartbeat command.
// At is a Unix timestamp (seconds) — passed in by the caller so Apply stays deterministic.
type heartbeatPayload struct {
	ID string `json:"id"`
	At int64  `json:"at"`
}

// fsmState is the Raft FSM backed by bbolt.
//
// mu protects the db pointer only during Restore (which swaps it).
// Apply is serialised with Restore by Raft's runFSM goroutine so it
// does not need to hold mu. Read methods hold RLock to prevent using a
// closed db while Restore is swapping the pointer.
type fsmState struct {
	mu     sync.RWMutex
	db     *bolt.DB
	dbPath string
}

func newFSM(path string) (*fsmState, error) {
	db, err := openBolt(path)
	if err != nil {
		return nil, err
	}
	return &fsmState{db: db, dbPath: path}, nil
}

func openBolt(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return db, db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketTopics, bucketSchemas, bucketAssignments, bucketMembers} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

// Apply is called by Raft when a log entry is committed.
func (f *fsmState) Apply(l *raft.Log) interface{} {
	var c cmd
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return err
	}
	switch c.Op {
	case opCreateTopic:
		return f.applyCreateTopic(c.Data)
	case opUpdateTopic:
		return f.applyUpdateTopic(c.Data)
	case opDeleteTopic:
		return f.applyDeleteTopic(c.Data)
	case opPutSchema:
		return f.applyPutSchema(c.Data)
	case opAssignPartition:
		return f.applyAssignPartition(c.Data)
	case opMemberJoin:
		return f.applyMemberJoin(c.Data)
	case opMemberHeartbeat:
		return f.applyMemberHeartbeat(c.Data)
	case opMemberDead:
		return f.applyMemberDead(c.Data)
	default:
		return fmt.Errorf("metastore: unknown op %d", c.Op)
	}
}

func (f *fsmState) applyCreateTopic(data []byte) error {
	var t topic.Topic
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	return f.db.Update(func(tx *bolt.Tx) error {
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
	return f.db.Update(func(tx *bolt.Tx) error {
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
	return f.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(name)) == nil {
			return ErrNotFound
		}
		if err := b.Delete([]byte(name)); err != nil {
			return err
		}
		prefix := []byte(name + ":")
		// Remove schemas.
		sc := tx.Bucket(bucketSchemas).Cursor()
		for k, _ := sc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = sc.Next() {
			if err := sc.Delete(); err != nil {
				return err
			}
		}
		// Remove partition assignments.
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
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSchemas).Put(schemaKey(p.Topic, p.Version), p.Schema)
	})
}

// -- assignment apply --

func (f *fsmState) applyAssignPartition(data []byte) error {
	var a Assignment
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAssignments).Put(assignmentKey(a.Topic, a.Partition), v)
	})
}

// -- member apply --

func (f *fsmState) applyMemberJoin(data []byte) error {
	var m Member
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMembers).Put([]byte(m.ID), v)
	})
}

func (f *fsmState) applyMemberHeartbeat(data []byte) error {
	var p heartbeatPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return f.db.Update(func(tx *bolt.Tx) error {
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
	return f.db.Update(func(tx *bolt.Tx) error {
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

// Snapshot serialises the full bbolt database.
func (f *fsmState) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	err := f.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	})
	return &fsmSnapshot{data: buf.Bytes()}, err
}

// Restore replaces current state with the snapshot.
func (f *fsmState) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	tmp := f.dbPath + ".restore"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
	if err := os.Rename(tmp, f.dbPath); err != nil {
		return err
	}
	db, err := openBolt(f.dbPath)
	if err != nil {
		return err
	}
	f.db = db
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
