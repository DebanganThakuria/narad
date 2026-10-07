package metastore

// A restarted node replays its Raft log after its last snapshot on top
// of its persisted database. Remote ops are not idempotent (revisions
// and credential versions count up, a re-encrypt is a compare-and-swap,
// every seal is counted), so each records its index and a replay skips
// it. These tests apply entries through Apply as Raft does, then replay
// every one of them, and require identical state.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// raftLog applies commands through Apply with increasing indexes and
// keeps them for a replay.
type raftLog struct {
	t       *testing.T
	f       *fsmState
	entries []*raft.Log
}

func (l *raftLog) apply(op opCode, body any) any {
	l.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		l.t.Fatal(err)
	}
	raw, err := json.Marshal(cmd{Op: op, Data: data})
	if err != nil {
		l.t.Fatal(err)
	}
	e := &raft.Log{Index: uint64(len(l.entries) + 1), Type: raft.LogCommand, Data: raw}
	l.entries = append(l.entries, e)
	return l.f.Apply(e)
}

// replay applies every entry again, as a restart without a snapshot past
// them does.
func (l *raftLog) replay() {
	for _, e := range l.entries {
		l.f.Apply(e)
	}
}

// dump is every bucket's content, for byte-for-byte comparison.
func dump(t *testing.T, f *fsmState) []byte {
	t.Helper()
	var buf bytes.Buffer
	err := f.view(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketTopics, bucketRemotes} {
			if err := tx.Bucket(name).ForEach(func(k, v []byte) error {
				fmt.Fprintf(&buf, "%s/%s = %s\n", name, k, v)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRemoteRegistryOpsSurviveLogReplay(t *testing.T) {
	l := &raftLog{t: t, f: openTestFSM(t)}
	if r := l.apply(opPutRemote, PutRemoteOp{Record: record("b"), Salt: testSalt, SealedAtMs: 1, Actor: "alice"}); r != nil {
		t.Fatalf("put: %v", r)
	}
	base := readRecord(t, l.f, "b")
	// A password change (credential version 2).
	if r := l.apply(opUpdateRemote, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKV, 2)), Fingerprint: "bbbbbbbbbbbb", SealedAgainst: tupleOf(t, base), ReadRevision: 1, SealedAtMs: 2, Actor: "alice"}); r != nil {
		t.Fatalf("password change: %v", r)
	}
	// A re-encrypt under a new key, a compare-and-swap on version 2.
	cv := uint64(2)
	if r := l.apply(opUpdateRemote, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKVNew, 3)), Fingerprint: "cccccccccccc", SealedAgainst: tupleOf(t, base), ReadRevision: 2, ExpectCredentialVersion: &cv, Reseal: true, SealedAtMs: 3, Actor: "alice"}); r != nil {
		t.Fatalf("reencrypt: %v", r)
	}
	// A refused entry (a duplicate create) still counts its seal.
	if r := l.apply(opPutRemote, PutRemoteOp{Record: record("b"), SealedAtMs: 4, Actor: "alice"}); r == nil {
		t.Fatal("a duplicate create applied")
	}
	// A delete, then a create of the same name with another id.
	if r := l.apply(opPutRemote, PutRemoteOp{Record: record("c"), SealedAtMs: 5, Actor: "alice"}); r != nil {
		t.Fatalf("put c: %v", r)
	}
	if r := l.apply(opDeleteRemote, DeleteRemoteOp{Name: "c", Actor: "alice"}); r != nil {
		t.Fatalf("delete c: %v", r)
	}
	again := record("c")
	again.ID = "id-c-2"
	if r := l.apply(opPutRemote, PutRemoteOp{Record: again, SealedAtMs: 6, Actor: "alice"}); r != nil {
		t.Fatalf("put c again: %v", r)
	}

	before := dump(t, l.f)
	b := readRecord(t, l.f, "b")
	if b.Credential.KV != testKVNew || b.CredentialVersion != 3 || b.Revision != 3 {
		t.Fatalf("before the replay: kv=%s cv=%d rev=%d", b.Credential.KV, b.CredentialVersion, b.Revision)
	}
	seals := readKeys(t, l.f).Versions
	for range 3 {
		l.replay()
	}
	if after := dump(t, l.f); !bytes.Equal(before, after) {
		a := readRecord(t, l.f, "b")
		t.Fatalf("replay changed the registry: b kv %s cv %d rev %d (was %s %d %d); seals %v (was %v)",
			a.Credential.KV, a.CredentialVersion, a.Revision, b.Credential.KV, b.CredentialVersion, b.Revision,
			readKeys(t, l.f).Versions, seals)
	}
	if c := readRecord(t, l.f, "c"); c.ID != "id-c-2" {
		t.Fatalf("after replay c has id %s, want the second create's", c.ID)
	}
}

func TestRemoteChildOpsSurviveLogReplay(t *testing.T) {
	f := remoteChildFSM(t)
	l := &raftLog{t: t, f: f}
	if r := l.apply(opAttachRemoteChild, remoteAttachOp("orders", "orders-to-b", "b", "orders")); r != nil {
		t.Fatalf("attach: %v", r)
	}
	if r := l.apply(opSetRemoteChildState, RemoteChildStateOp{Parent: "orders", Stub: "orders-to-b", Epoch: "ep-orders-to-b", Pause: &RemotePauseState{Paused: true, Reason: "maint", By: "alice", AtMs: 1}}); r != nil {
		t.Fatalf("pause: %v", r)
	}
	// Refused: remote "c" does not exist yet. Applied against a later
	// state (c created below) it would create a stub no replica has.
	if r := l.apply(opAttachRemoteChild, remoteAttachOp("orders", "x", "c", "orders")); r == nil {
		t.Fatal("an attach naming a missing remote applied")
	}
	if r := l.apply(opPutRemote, PutRemoteOp{Record: record("c"), Salt: testSalt, SealedAtMs: 1, Actor: "alice"}); r != nil {
		t.Fatalf("put c: %v", r)
	}
	before := dump(t, f)
	l.replay()
	if after := dump(t, f); !bytes.Equal(before, after) {
		t.Fatalf("replay changed the topics or the registry:\nbefore %s\nafter  %s", before, after)
	}
	if fsmTopicExists(t, f, "x") {
		t.Fatal("the refused attach applied on replay")
	}
	stub, err := getStub(f, "orders-to-b")
	if err != nil || !stub.Remote.Paused {
		t.Fatalf("stub after replay: %+v %v", stub, err)
	}
}

// A snapshot restore replaces the replay guard's index with the rest of
// the database: entries after the snapshot apply once, whatever the
// database held before.
func TestRemoteOpsAfterASnapshotRestoreApplyOnce(t *testing.T) {
	l := &raftLog{t: t, f: openTestFSM(t)}
	if r := l.apply(opPutRemote, PutRemoteOp{Record: record("b"), Salt: testSalt, SealedAtMs: 1, Actor: "alice"}); r != nil {
		t.Fatal(r)
	}
	image := snapshotImage(t, l.f)
	base := readRecord(t, l.f, "b")
	if r := l.apply(opUpdateRemote, UpdateRemoteOp{Name: "b", Credential: ptr(env(testKV, 2)), Fingerprint: "bbbbbbbbbbbb", SealedAgainst: tupleOf(t, base), ReadRevision: 1, SealedAtMs: 2, Actor: "alice"}); r != nil {
		t.Fatal(r)
	}
	want := dump(t, l.f)
	if err := l.f.Restore(io.NopCloser(bytes.NewReader(image))); err != nil {
		t.Fatal(err)
	}
	for _, e := range l.entries[1:] {
		l.f.Apply(e)
	}
	if got := dump(t, l.f); !bytes.Equal(want, got) {
		t.Fatalf("restore then replay:\nwant %s\ngot  %s", want, got)
	}
	if b := readRecord(t, l.f, "b"); b.CredentialVersion != 2 {
		t.Fatalf("credential version %d after the restore and replay, want 2", b.CredentialVersion)
	}
}

func getStub(f *fsmState, name string) (topic.Topic, error) {
	var t topic.Topic
	err := f.view(func(tx *bolt.Tx) error {
		var err error
		t, err = getTopicRecord(tx, name)
		return err
	})
	return t, err
}
