package metastore

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Audit finding 1.1: applyPutSchema was a blind Put, so a proposer that
// computed its version from a stale local registry (a leader that never
// loaded the topic's history) overwrote v1 on every replica and erased
// the history the compatibility contract depends on. The FSM now only
// accepts latest+1.
func TestApplyPutSchema_RejectsOverwriteAndGaps(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "orders")
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	v2 := []byte(`{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"}},"required":["id"]}`)
	stale := []byte(`{"type":"object","properties":{"name":{"type":"integer"}},"required":["name"]}`)

	if err := fsmPutSchema(t, f, "orders", 1, v1); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	before := f.versions.schemaVersion("orders")

	// A stale proposer replays "v1" with different content.
	err := fsmPutSchema(t, f, "orders", 1, stale)
	if !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("put v1 again error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	if got, _ := fsmSchema(t, f, "orders", 1); string(got) != string(v1) {
		t.Fatalf("schema v1 = %s after rejected overwrite, want the original", got)
	}
	if got := f.versions.schemaVersion("orders"); got != before {
		t.Fatalf("schema version bumped to %d by a rejected put (was %d)", got, before)
	}

	// Version 0 and a gap are refused too.
	if err := fsmPutSchema(t, f, "orders", 0, v2); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("put v0 error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	if err := fsmPutSchema(t, f, "orders", 3, v2); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("put v3 with latest 1 error = %v, want %v", err, errs.ErrInvalidArgument)
	}
	if _, found := fsmSchema(t, f, "orders", 3); found {
		t.Fatal("v3 was stored despite the gap")
	}

	// The correct next version lands.
	if err := fsmPutSchema(t, f, "orders", 2, v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	if got, found := fsmSchema(t, f, "orders", 2); !found || string(got) != string(v2) {
		t.Fatalf("schema v2 = %s (found=%v), want %s", got, found, v2)
	}
	if got := f.versions.schemaVersion("orders"); got <= before {
		t.Fatalf("schema version = %d after a successful put, want > %d", got, before)
	}
}

// A fan-out child's copy follows the same rule: a parent put whose
// version is not latest+1 for every child is rejected as a whole, so
// parent and child histories can never diverge.
func TestApplyPutSchema_ChildCopiesFollowTheSameRule(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child")
	v1 := []byte(`{"type":"object"}`)
	v2 := []byte(`{"type":"object","properties":{"a":{"type":"string"}}}`)
	if err := fsmPutSchema(t, f, "parent", 1, v1); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if err := fsmAttach(t, f, "parent", "child"); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Replaying v1 on the parent is refused for the parent; nothing is
	// written to the child either (single transaction).
	if err := fsmPutSchema(t, f, "parent", 1, v2); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("replay v1 on parent error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	if got, _ := fsmSchema(t, f, "child", 1); string(got) != string(v1) {
		t.Fatalf("child v1 = %s after rejected parent put, want %s", got, v1)
	}

	// The regular next version propagates.
	if err := fsmPutSchema(t, f, "parent", 2, v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	if got, found := fsmSchema(t, f, "child", 2); !found || string(got) != string(v2) {
		t.Fatalf("child v2 = %s (found=%v), want propagated %s", got, found, v2)
	}
}

// Log entries written before the append-only rule use the same payload
// and apply through the same switch, so replaying a healthy history
// (create, put 1, put 2) through the public Apply path yields the same
// state it always did.
func TestApplyPutSchema_ReplaysHistoricalLog(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "orders")
	v1 := []byte(`{"type":"object"}`)
	v2 := []byte(`{"type":"object","properties":{"a":{"type":"string"}}}`)
	for i, entry := range []schemaPayload{
		{Topic: "orders", Version: 1, Schema: v1},
		{Topic: "orders", Version: 2, Schema: v2},
	} {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(cmd{Op: opPutSchema, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		if resp := f.Apply(&raft.Log{Index: uint64(i + 1), Data: raw}); resp != nil {
			if err, ok := resp.(error); ok && err != nil {
				t.Fatalf("Apply(log %d) error = %v", i+1, err)
			}
		}
	}
	for version, want := range map[int][]byte{1: v1, 2: v2} {
		got, found := fsmSchema(t, f, "orders", version)
		if !found || string(got) != string(want) {
			t.Fatalf("schema v%d = %s (found=%v), want %s", version, got, found, want)
		}
	}
}

func TestLatestSchemaVersionTakesNumericMax(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "orders")
	// Versions 1..10: the lexically last key is "orders:9", the numeric
	// max is 10.
	for v := 1; v <= 10; v++ {
		if err := fsmPutSchema(t, f, "orders", v, []byte(`{"type":"object"}`)); err != nil {
			t.Fatalf("put v%d: %v", v, err)
		}
	}
	if err := fsmPutSchema(t, f, "orders", 10, []byte(`{}`)); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("put v10 again error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	if err := fsmPutSchema(t, f, "orders", 11, []byte(`{}`)); err != nil {
		t.Fatalf("put v11: %v", err)
	}
}
