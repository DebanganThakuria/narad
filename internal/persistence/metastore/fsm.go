package metastore

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

func (op opCode) metricName() string {
	switch op {
	case opCreateTopic:
		return "create_topic"
	case opUpdateTopic:
		return "update_topic"
	case opDeleteTopic:
		return "delete_topic"
	case opPutSchema:
		return "put_schema"
	case opAssignPartition:
		return "assign_partition"
	case opMemberJoin:
		return "member_join"
	case opMemberHeartbeat:
		return "member_heartbeat"
	case opMemberDead:
		return "member_dead"
	default:
		return "unknown"
	}
}

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
	return fmt.Appendf(nil, "%s:%d", topicName, version)
}

func assignmentKey(topicName string, partition int) []byte {
	return fmt.Appendf(nil, "%s:%d", topicName, partition)
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
	mu      sync.RWMutex
	db      *bolt.DB
	dbPath  string
	metric  MetricsRecorder
	version atomic.Uint64
}

func newFSM(path string, metric MetricsRecorder) (*fsmState, error) {
	db, err := openBolt(path)
	if err != nil {
		return nil, err
	}
	return &fsmState{db: db, dbPath: path, metric: metric}, nil
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

func (f *fsmState) view(operation string, fn func(*bolt.Tx) error) error {
	start := time.Now()
	err := f.db.View(fn)
	f.observeTx(operation, "read", err, time.Since(start))
	return err
}

func (f *fsmState) update(operation string, fn func(*bolt.Tx) error) error {
	start := time.Now()
	err := f.db.Update(fn)
	f.observeTx(operation, "write", err, time.Since(start))
	return err
}

func (f *fsmState) observeTx(operation, mode string, err error, duration time.Duration) {
	if f.metric == nil {
		return
	}
	f.metric.ObserveMetastoreTx(operation, mode, statusForErr(err), duration)
}

// Apply is called by Raft when a log entry is committed.
func (f *fsmState) Apply(l *raft.Log) any {
	var c cmd
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return err
	}
	var err error
	switch c.Op {
	case opCreateTopic:
		err = f.applyCreateTopic(c.Data)
	case opUpdateTopic:
		err = f.applyUpdateTopic(c.Data)
	case opDeleteTopic:
		err = f.applyDeleteTopic(c.Data)
	case opPutSchema:
		err = f.applyPutSchema(c.Data)
	case opAssignPartition:
		err = f.applyAssignPartition(c.Data)
	case opMemberJoin:
		err = f.applyMemberJoin(c.Data)
	case opMemberHeartbeat:
		err = f.applyMemberHeartbeat(c.Data)
	case opMemberDead:
		err = f.applyMemberDead(c.Data)
	default:
		return fmt.Errorf("metastore: unknown op %d", c.Op)
	}
	if err == nil {
		f.version.Add(1)
	}
	return err
}

func (f *fsmState) metadataVersion() uint64 {
	return f.version.Load()
}
