package metastore

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

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
