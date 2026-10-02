package metastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// boltOpenTimeout bounds how long a bbolt open waits for the file lock.
// bbolt takes an exclusive flock and, with no timeout, waits for it
// forever: a stale process on the same data directory, a double start
// on bare metal, or an operator's inspection tool holding fsm.db made
// `narad serve` hang inside metastore.New with no log line and no
// error, so /healthz never came up and Kubernetes restarted the pod in
// a loop with nothing to show for it. With the timeout the open fails
// with bolt.ErrTimeout and the error names the file. A var so tests can
// shorten it.
var boltOpenTimeout = 5 * time.Second

// boltOptions is the option set every bbolt open in this package uses.
func boltOptions() *bolt.Options {
	return &bolt.Options{Timeout: boltOpenTimeout}
}

var (
	bucketTopics      = []byte("topics")
	bucketSchemas     = []byte("schemas")
	bucketAssignments = []byte("assignments")
	bucketMembers     = []byte("members")
	bucketUsers       = []byte("users")
	// bucketRemovedMembers holds tombstones for members removed by
	// decommission, keyed by member ID; see applyRemoveMember.
	bucketRemovedMembers = []byte("removed_members")
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
	// applied is the index of the last log entry whose effects are
	// committed to db. Raft's own applied_index advances when a batch is
	// handed to the FSM goroutine, and fsm_pending only counts batches
	// still queued, so neither says the entry's bbolt transaction has
	// finished; this does. Store.WaitApplied builds read-your-writes on it.
	// It starts at the index db records about itself (fsm_applied.go)
	// when that can be trusted, and Apply skips entries at or below it.
	applied atomic.Uint64
	// lastSeen is the highest index Raft has handed Apply, or that a
	// restored snapshot covers. It trails applied only after a start
	// that kept a database ahead of the snapshot Raft resumed from, until
	// the replay reaches applied; Snapshot waits for that.
	lastSeen atomic.Uint64
	// meta is what db recorded about itself when it was opened; the
	// start-up rules in store.go read it before Raft starts.
	meta fsmMeta

	// cur is the entry Apply is applying (zero outside Apply). Only the
	// FSM goroutine touches it.
	cur appliedEntry

	log   *slog.Logger
	build string
	halt  haltState

	// For the metastore metrics: apply errors by kind, and whether a
	// storage failure is being retried right now.
	applyErrors [applyErrKinds]atomic.Uint64
	stalled     atomic.Bool
}

// appliedEntry is the entry being applied.
type appliedEntry struct {
	index     uint64
	entryType uint32
	// persisted: the entry's index was committed with its effects.
	persisted bool
}

// fsmOptions configures an FSM before it opens its database.
type fsmOptions struct {
	// log receives the FSM's error and warning lines; nil discards them.
	log *slog.Logger
	// build names this binary in those lines and in the stop error.
	build string
}

func newFSM(path string) (*fsmState, error) {
	return newFSMWith(path, fsmOptions{})
}

// newFSMWith opens the database at path. It refuses one that has
// applied an entry type newer than this build knows (openBolt): this
// binary would read it with older semantics, and its log tail holds
// entries it cannot apply.
func newFSMWith(path string, opts fsmOptions) (*fsmState, error) {
	log := opts.log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	db, meta, err := openBolt(path, opts.build)
	if err != nil {
		return nil, err
	}
	f := &fsmState{db: db, dbPath: path, versions: newMetadataDomainVersions(), meta: meta, log: log, build: opts.build}
	f.applied.Store(meta.trustedApplied())
	return f, nil
}

func (f *fsmState) view(fn func(*bolt.Tx) error) error {
	return f.db.View(fn)
}

// entryEnvelope decodes a log entry's envelope (cmd). The type is read
// wider than opCode so an entry type past 255 still reads as unknown
// rather than undecodable.
type entryEnvelope struct {
	Op   uint64 `json:"o"`
	Data []byte `json:"d"`
}

// Apply is called by Raft when a log entry is committed. A business
// error (e.g. ErrAlreadyExists) is returned as the FSM response so the
// caller sees it, and the metadata version only advances on success.
//
// An entry the database already holds (a replay after a restart) is
// skipped. An entry type this build does not know, or a write the
// local database refuses, stops the FSM instead of being consumed
// (fsm_failstop.go).
func (f *fsmState) Apply(l *raft.Log) any {
	if err := f.stopErr(); err != nil {
		return err
	}
	f.lastSeen.Store(l.Index)
	if l.Index <= f.applied.Load() {
		return nil
	}
	var env entryEnvelope
	if err := json.Unmarshal(l.Data, &env); err != nil {
		// The same bytes are in every replica's log, so every replica
		// refuses them alike: consume the entry.
		f.applyErrors[applyErrUndecodable].Add(1)
		f.log.Error("metastore: consumed a raft entry it cannot decode", "index", l.Index, "error", err)
		f.recordConsumed(l.Index, 0)
		f.applied.Store(l.Index)
		return err
	}
	if env.Op == 0 || env.Op > uint64(MaxEntryType) {
		return f.stopOnUnknownEntryType(l.Index, env.Op)
	}

	f.cur = appliedEntry{index: l.Index, entryType: uint32(env.Op)}
	defer func() { f.cur = appliedEntry{} }()
	err := f.dispatch(opCode(env.Op), env.Data)
	if errors.Is(err, errNoHandler) {
		return f.stopOnUnknownEntryType(l.Index, env.Op)
	}
	if stopped := f.stopErr(); stopped != nil {
		// The entry's write failed for good: not applied, not consumed.
		return stopped
	}
	if err == nil {
		f.version.Add(1)
	}
	if !f.cur.persisted {
		// Refused before or inside its transaction: record the index
		// on its own.
		f.recordConsumed(l.Index, f.cur.entryType)
	}
	// Every path above has finished its bbolt transaction (or refused
	// to start one), so the entry's effects are in db before the index
	// moves. Set on error too: a rejected command is consumed all the same.
	f.applied.Store(l.Index)
	return err
}

// errNoHandler is dispatch's answer for a type with no handler, which
// Apply treats like any type it does not know.
var errNoHandler = errors.New("metastore: no handler for this entry type")

// dispatch runs the handler for op.
func (f *fsmState) dispatch(op opCode, data []byte) error {
	switch op {
	case opCreateTopic:
		return f.applyCreateTopic(data)
	case opUpdateTopic:
		return f.applyUpdateTopic(data)
	case opDeleteTopic:
		return f.applyDeleteTopic(data)
	case opPutSchema:
		return f.applyPutSchema(data)
	case opAssignPartition:
		return f.applyAssignPartition(data)
	case opMemberJoin:
		return f.applyMemberJoin(data)
	case opMemberHeartbeat:
		return f.applyMemberHeartbeat(data)
	case opMemberDead:
		return f.applyMemberDead(data)
	case opCreateUser:
		return f.applyCreateUser(data)
	case opUpdateUser:
		return f.applyUpdateUser(data)
	case opDeleteUser:
		return f.applyDeleteUser(data)
	case opSeedRootUser:
		return f.applySeedRootUser(data)
	case opAttachChild:
		return f.applyAttachChild(data)
	case opDetachChild:
		return f.applyDetachChild(data)
	case opSetAssignmentTarget:
		return f.applySetAssignmentTarget(data)
	case opCompleteMove:
		return f.applyCompleteMove(data)
	case opAbortMove:
		return f.applyAbortMove(data)
	case opSetMemberDraining:
		return f.applySetMemberDraining(data)
	case opRemoveMember:
		return f.applyRemoveMember(data)
	case opReadmitMember:
		return f.applyReadmitMember(data)
	case opSetUserPassword:
		return f.applySetUserPassword(data)
	case opSetUserGrants:
		return f.applySetUserGrants(data)
	default:
		return errNoHandler
	}
}

func (f *fsmState) metadataVersion() uint64 {
	return f.version.Load()
}
