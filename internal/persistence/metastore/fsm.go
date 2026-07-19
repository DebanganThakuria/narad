package metastore

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketTopics      = []byte("topics")
	bucketSchemas     = []byte("schemas")
	bucketAssignments = []byte("assignments")
	bucketMembers     = []byte("members")
	bucketUsers       = []byte("users")
)

func schemaKey(topicName string, version int) []byte {
	return fmt.Appendf(nil, "%s:%d", topicName, version)
}

func assignmentKey(topicName string, partition int) []byte {
	return fmt.Appendf(nil, "%s:%d", topicName, partition)
}

// fsmState is the Raft FSM backed by bbolt.
//
// mu protects the db pointer only during Restore (which swaps it).
// Apply is serialised with Restore by Raft's runFSM goroutine so it
// does not need to hold mu. Read methods hold RLock to prevent using a
// closed db while Restore is swapping the pointer.
type fsmState struct {
	mu       sync.RWMutex
	db       *bolt.DB
	dbPath   string
	version  atomic.Uint64
	versions metadataDomainVersions
}

func newFSM(path string) (*fsmState, error) {
	db, err := openBolt(path)
	if err != nil {
		return nil, err
	}
	return &fsmState{db: db, dbPath: path, versions: newMetadataDomainVersions()}, nil
}

func openBolt(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	return db, db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketTopics, bucketSchemas, bucketAssignments, bucketMembers, bucketUsers} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (f *fsmState) view(fn func(*bolt.Tx) error) error {
	return f.db.View(fn)
}

func (f *fsmState) update(fn func(*bolt.Tx) error) error {
	return f.db.Update(fn)
}

// Apply is called by Raft when a log entry is committed. A business
// error (e.g. ErrAlreadyExists) is returned as the FSM response so the
// caller sees it, and the metadata version only advances on success.
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
	case opCreateUser:
		err = f.applyCreateUser(c.Data)
	case opUpdateUser:
		err = f.applyUpdateUser(c.Data)
	case opDeleteUser:
		err = f.applyDeleteUser(c.Data)
	case opSeedRootUser:
		err = f.applySeedRootUser(c.Data)
	case opAttachChild:
		err = f.applyAttachChild(c.Data)
	case opDetachChild:
		err = f.applyDetachChild(c.Data)
	case opSetAssignmentTarget:
		err = f.applySetAssignmentTarget(c.Data)
	case opCompleteMove:
		err = f.applyCompleteMove(c.Data)
	case opAbortMove:
		err = f.applyAbortMove(c.Data)
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
