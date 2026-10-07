package metastore

// The FSM stops applying (and the node leaves Raft) instead of
// consuming an entry it cannot apply: an entry type this build does not
// know, or a write its local database refuses. A deterministic refusal
// and an undecodable entry are still consumed.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// newTestFSM opens an FSM on a fresh fsm.db and closes it when the test
// ends.
func newTestFSM(t *testing.T) *fsmState {
	t.Helper()
	f, err := newFSM(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatalf("newFSM: %v", err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	return f
}

// encodeEntry is the Raft log data Store.apply proposes for op.
func encodeEntry(t *testing.T, op opCode, payload any) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cmd{Op: op, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// applyEntry hands one command to the FSM the way Raft does and returns
// its response as an error (nil on success).
func applyEntry(t *testing.T, f *fsmState, index uint64, op opCode, payload any) error {
	t.Helper()
	return applyRaw(f, index, encodeEntry(t, op, payload))
}

func applyRaw(f *fsmState, index uint64, data []byte) error {
	resp := f.Apply(&raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data})
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

// unknownEntry is a well-formed entry whose type is one past the newest
// this build knows: what a newer release's leader would propose.
func unknownEntry() []byte {
	return fmt.Appendf(nil, `{"o":%d,"d":"e30="}`, MaxEntryType+1)
}

// storedMeta reads one fsm_meta key of db (0 when absent).
func storedMeta(t *testing.T, db *bolt.DB, key string) uint64 {
	t.Helper()
	var v uint64
	if err := db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte("fsm_meta")); b != nil {
			if raw := b.Get([]byte(key)); len(raw) == 8 {
				v = binary.BigEndian.Uint64(raw)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("read fsm_meta %s: %v", key, err)
	}
	return v
}

func hasTopic(t *testing.T, f *fsmState, name string) bool {
	t.Helper()
	var found bool
	if err := f.view(func(tx *bolt.Tx) error {
		found = tx.Bucket(bucketTopics).Get([]byte(name)) != nil
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	return found
}

// shortApplyRetries shrinks the storage retry ladder so a test reaches
// the stop in milliseconds.
func shortApplyRetries(t *testing.T) {
	t.Helper()
	initial, maxDelay, budget := applyRetryInitial, applyRetryMax, applyRetryBudget
	applyRetryInitial, applyRetryMax, applyRetryBudget = time.Millisecond, 5*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { applyRetryInitial, applyRetryMax, applyRetryBudget = initial, maxDelay, budget })
}

// failCommits makes the next n commits of an applied entry fail with
// err, then commit for real, and counts the attempts.
func failCommits(t *testing.T, n int, err error) *int {
	t.Helper()
	prev := commitTx
	calls := new(int)
	commitTx = func(tx *bolt.Tx) error {
		*calls++
		if *calls <= n {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	t.Cleanup(func() { commitTx = prev })
	return calls
}

func TestUnknownEntryTypeStopsApplying(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("create: %v", err)
	}

	err := applyRaw(f, 2, unknownEntry())
	if !errors.Is(err, ErrStoppedApplying) || !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("unknown entry type answered %v; want an error wrapping ErrStoppedApplying and errs.ErrUnavailable", err)
	}
	if got := f.applied.Load(); got != 1 {
		t.Fatalf("applied index = %d after an unknown entry type, want 1: the entry must not be consumed", got)
	}
	if got := storedMeta(t, f.db, "applied"); got != 1 {
		t.Fatalf("persisted applied index = %d, want 1", got)
	}
	if err := applyEntry(t, f, 3, opCreateTopic, topic.Topic{Name: "events", Partitions: 1}); !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("an entry after the stop answered %v, want ErrStoppedApplying", err)
	}
	if hasTopic(t, f, "events") {
		t.Fatal("an entry after the stop was applied")
	}
	if _, err := f.Snapshot(); !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("Snapshot after the stop = %v, want ErrStoppedApplying", err)
	}
	stopped := f.stopErr()
	if stopped == nil {
		t.Fatal("no stop error recorded")
	}
	for _, want := range []string{"index 2", fmt.Sprintf("entry type %d", MaxEntryType+1)} {
		if !strings.Contains(stopped.Error(), want) {
			t.Fatalf("stop error %q does not name %q", stopped, want)
		}
	}
}

// Guard for the stop above: every type this build proposes reaches its
// handler, so a type missing from Apply's switch would stop a node on an
// entry it does know.
func TestEveryKnownEntryTypeReachesItsHandler(t *testing.T) {
	if legacyMaxEntryType != 22 {
		t.Fatalf("legacyMaxEntryType = %d; it is the frozen set every 3.0.x release applies (22)", legacyMaxEntryType)
	}
	if MaxEntryType < legacyMaxEntryType {
		t.Fatalf("MaxEntryType %d is below the legacy set %d", MaxEntryType, legacyMaxEntryType)
	}
	// The entry types newer than 3.0.x keep the numbers they shipped
	// with: a log written by one release replays on the next.
	for op, want := range map[opCode]uint32{
		opCreateTopicWith: 23, opUpdateTopicIf: 24, opDeleteTopicIf: 25, opPutSchemaIf: 26, opAttachChildIf: 27,
		opDetachChildIf: 28, opAssignPartitionIfAbsent: 29, opPruneAssignment: 30, opMarkMemberDeadIf: 31,
		opDeleteUserReleaseTopics: 32, opAttachRemoteChild: 33, opSetRemoteChildState: 34, opPutRemote: 35,
		opUpdateRemote: 36, opDeleteRemote: 37,
	} {
		if uint32(op) != want || entryTypeNames[op] == "" {
			t.Fatalf("entry type %q is %d, want %d with a name for its log lines", entryTypeNames[op], op, want)
		}
	}
	if MaxEntryType != 37 {
		t.Fatalf("MaxEntryType = %d, want 37", MaxEntryType)
	}
	f := newTestFSM(t)
	for et := uint32(1); et <= MaxEntryType; et++ {
		// An empty payload: every handler refuses it while decoding.
		err := applyRaw(f, uint64(et), fmt.Appendf(nil, `{"o":%d,"d":""}`, et))
		if errors.Is(err, ErrStoppedApplying) || f.stopErr() != nil {
			t.Fatalf("entry type %d stopped the FSM: %v", et, err)
		}
		if err == nil {
			t.Fatalf("entry type %d with an empty payload was accepted", et)
		}
	}
	if got := f.applied.Load(); got != uint64(MaxEntryType) {
		t.Fatalf("applied = %d, want %d", got, MaxEntryType)
	}
}

func TestStorageFailureOnAClosedDatabaseIsNotConsumed(t *testing.T) {
	f := newTestFSM(t)
	shortApplyRetries(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "first", Partitions: 1}); err != nil {
		t.Fatalf("create first: %v", err)
	}
	_ = f.db.Close()

	resp := applyEntry(t, f, 2, opCreateTopic, topic.Topic{Name: "orders", Partitions: 3})
	if !errors.Is(resp, ErrStoppedApplying) || !errors.Is(resp, berrors.ErrDatabaseNotOpen) {
		t.Fatalf("response = %v; want a stop that names the closed database", resp)
	}
	if got := f.applied.Load(); got != 1 {
		t.Fatalf("applied index = %d after a storage failure, want 1: the entry must not be consumed", got)
	}
}

func TestStorageFailureAtCommitIsNotConsumed(t *testing.T) {
	f := newTestFSM(t)
	shortApplyRetries(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "first", Partitions: 1}); err != nil {
		t.Fatalf("create first: %v", err)
	}
	eio := &fs.PathError{Op: "write", Path: "fsm.db", Err: syscall.EIO}
	calls := failCommits(t, 1<<30, eio)

	resp := applyEntry(t, f, 2, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1})
	if !errors.Is(resp, ErrStoppedApplying) || !errors.Is(resp, syscall.EIO) {
		t.Fatalf("response = %v; want a stop that carries the EIO", resp)
	}
	if *calls < 2 {
		t.Fatalf("commit attempted %d times; want retries before the stop", *calls)
	}
	if got := f.applied.Load(); got != 1 {
		t.Fatalf("applied index = %d, want 1", got)
	}
	if got := storedMeta(t, f.db, "applied"); got != 1 {
		t.Fatalf("persisted applied index = %d, want 1", got)
	}
	if hasTopic(t, f, "orders") {
		t.Fatal("the entry whose commit failed is in the database")
	}
}

func TestStorageFailureIsRetriedBeforeStopping(t *testing.T) {
	f := newTestFSM(t)
	shortApplyRetries(t)
	calls := failCommits(t, 2, &fs.PathError{Op: "write", Path: "fsm.db", Err: syscall.ENOSPC})

	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if *calls != 3 {
		t.Fatalf("commit attempts = %d, want 3 (two failures, then the write)", *calls)
	}
	if f.stopErr() != nil {
		t.Fatalf("stopped after a failure that cleared: %v", f.stopErr())
	}
	if !hasTopic(t, f, "orders") || f.applied.Load() != 1 || storedMeta(t, f.db, "applied") != 1 {
		t.Fatalf("after the retry: topic %v, applied %d, persisted %d; want the entry applied once at 1",
			hasTopic(t, f, "orders"), f.applied.Load(), storedMeta(t, f.db, "applied"))
	}
}

// bbolt allocates pages at Commit, so the Put inside an apply closure
// succeeds and the growth failure surfaces from Commit. This is why a
// closure error is a deterministic refusal and a Begin or Commit error
// is the storage's.
func TestBoltGrowthFailsAtCommitNotInTheClosure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsm.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("b"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	db, err = bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second, MaxSize: int(st.Size())})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Bucket([]byte("b")).Put([]byte("k"), make([]byte, 1<<20)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Put = %v; want nil (pages are allocated at commit)", err)
	}
	if err := tx.Commit(); !errors.Is(err, berrors.ErrMaxSizeReached) {
		t.Fatalf("Commit = %v, want ErrMaxSizeReached", err)
	}
}

func TestUndecodableEntryIsConsumed(t *testing.T) {
	f := newTestFSM(t)
	if err := applyRaw(f, 1, []byte("not json")); err == nil {
		t.Fatal("an undecodable entry was applied without an error")
	}
	if f.stopErr() != nil {
		t.Fatalf("an undecodable entry stopped the FSM: %v", f.stopErr())
	}
	if got := f.applied.Load(); got != 1 {
		t.Fatalf("applied = %d, want 1: the entry is identical on every replica and is consumed", got)
	}
	if got := storedMeta(t, f.db, "applied"); got != 1 {
		t.Fatalf("persisted applied = %d, want 1", got)
	}
	if err := applyEntry(t, f, 2, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("the next entry: %v", err)
	}
}

func TestDeterministicRefusalIsConsumedAndRecorded(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := applyEntry(t, f, 2, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate create = %v, want ErrAlreadyExists", err)
	}
	if f.stopErr() != nil {
		t.Fatalf("a refusal stopped the FSM: %v", f.stopErr())
	}
	if got, persisted := f.applied.Load(), storedMeta(t, f.db, "applied"); got != 2 || persisted != 2 {
		t.Fatalf("applied %d, persisted %d; want 2 and 2 (a refused entry is recorded in a transaction of its own)", got, persisted)
	}
}

// singleNodeConfig is a one-voter store configuration on a free port in
// its own directory, reusable across a Close and reopen.
func singleNodeConfig(t *testing.T) Config {
	t.Helper()
	addr := freeAddr(t)
	return Config{NodeID: "solo", DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr}
}

// storeHandle closes whichever store it holds when the test ends.
type storeHandle struct{ s *Store }

func (h *storeHandle) close(t *testing.T) {
	t.Helper()
	if h.s == nil {
		return
	}
	if err := h.s.Close(); err != nil {
		t.Logf("close: %v", err)
	}
	h.s = nil
}

// openStore opens cfg and waits until it leads and has caught up.
func openStore(t *testing.T, cfg Config) *storeHandle {
	t.Helper()
	h := &storeHandle{}
	t.Cleanup(func() { h.close(t) })
	h.reopen(t, cfg)
	return h
}

func (h *storeHandle) reopen(t *testing.T, cfg Config) {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.s = s
	waitUntil(t, 15*time.Second, "leader and caught up", func() bool { return s.IsLeader() && s.AppliedCaughtUp() })
}

func TestStoppedStoreIsNotReadyAndLeavesRaft(t *testing.T) {
	h := openStore(t, singleNodeConfig(t))
	s := h.s
	if err := s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	// What a newer release's leader would propose.
	resp := s.r.Apply(unknownEntry(), 5*time.Second)
	if err := resp.Error(); err == nil {
		if rerr, ok := resp.Response().(error); !ok || !errors.Is(rerr, ErrStoppedApplying) {
			t.Fatalf("unknown entry type answered %v, want ErrStoppedApplying", resp.Response())
		}
	}
	select {
	case <-s.Halted():
	case <-time.After(5 * time.Second):
		t.Fatal("Halted() did not close after an unknown entry type")
	}
	if err := s.HaltErr(); !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("HaltErr = %v", err)
	}
	if err := s.ClusterReady(); !errors.Is(err, ErrNotReady) || !strings.Contains(err.Error(), "stopped applying") {
		t.Fatalf("ClusterReady = %v; want not ready, naming the stop", err)
	}
	if s.AppliedCaughtUp() {
		t.Fatal("a stopped store reports caught up")
	}
	// A barriered read on a stopped node must not count as the leader's
	// view: its database lacks the entry it stopped on.
	if err := s.Barrier(); err == nil {
		t.Fatal("Barrier succeeded on a stopped store")
	}
	waitUntil(t, 10*time.Second, "raft shut down", func() bool { return s.r.State() == raft.Shutdown })
	if err := s.CreateTopic(context.Background(), topic.Topic{Name: "events", Partitions: 1}); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("a write after the stop = %v, want errs.ErrUnavailable", err)
	}
}
