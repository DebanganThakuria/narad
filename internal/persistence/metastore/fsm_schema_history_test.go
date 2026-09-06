package metastore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

func fsmHistory(t *testing.T, f *fsmState, topicName string) map[int][]byte {
	t.Helper()
	var out map[int][]byte
	err := f.view(func(tx *bolt.Tx) error {
		var err error
		out, err = loadSchemaHistory(tx, topicName)
		return err
	})
	if err != nil {
		t.Fatalf("loadSchemaHistory(%s): %v", topicName, err)
	}
	return out
}

// The FSM refuses the version past MaxSchemaVersions, deterministically
// and without touching the history or the version counter.
func TestApplyPutSchema_EnforcesVersionCap(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "orders")
	for v := 1; v <= MaxSchemaVersions; v++ {
		if err := fsmPutSchema(t, f, "orders", v, []byte(fmt.Sprintf(`{"title":"v%d"}`, v))); err != nil {
			t.Fatalf("put v%d: %v", v, err)
		}
	}
	before := f.versions.schemaVersion("orders")
	err := fsmPutSchema(t, f, "orders", MaxSchemaVersions+1, []byte(`{"title":"too many"}`))
	if !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("put v%d error = %v, want %v", MaxSchemaVersions+1, err, errs.ErrSchemaHistoryFull)
	}
	if _, found := fsmSchema(t, f, "orders", MaxSchemaVersions+1); found {
		t.Fatal("version past the cap was stored")
	}
	if got := f.versions.schemaVersion("orders"); got != before {
		t.Fatalf("schema version bumped to %d by a refused put (was %d)", got, before)
	}
	if got := len(fsmHistory(t, f, "orders")); got != MaxSchemaVersions {
		t.Fatalf("history has %d versions, want %d", got, MaxSchemaVersions)
	}
}

// A node restored from a snapshot holds the complete schema history of
// every topic, including a child's adopted copy, and its version
// counters move so cached registries reload.
func TestFSMSnapshotRestoreCarriesSchemaHistory(t *testing.T) {
	source := newFanoutFSM(t)
	fsmCreateTopic(t, source, "parent")
	fsmCreateTopic(t, source, "child")
	fsmCreateTopic(t, source, "plain")
	for v := 1; v <= 3; v++ {
		if err := fsmPutSchema(t, source, "parent", v, []byte(fmt.Sprintf(`{"title":"v%d"}`, v))); err != nil {
			t.Fatalf("put v%d: %v", v, err)
		}
	}
	if err := fsmAttach(t, source, "parent", "child"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	snap, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	data := snap.(*fsmSnapshot).data

	target, err := newFSM(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("newFSM(target): %v", err)
	}
	t.Cleanup(func() { target.db.Close() })
	// The target had its own, different idea of the child's schema.
	fsmCreateTopic(t, target, "child")
	if err := fsmPutSchema(t, target, "child", 1, []byte(`{"title":"stale"}`)); err != nil {
		t.Fatal(err)
	}
	versionBefore := target.versions.schemaVersion("child")
	if err := target.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, name := range []string{"parent", "child"} {
		history := fsmHistory(t, target, name)
		if len(history) != 3 {
			t.Fatalf("%s: restored history has %d versions, want 3", name, len(history))
		}
		for v := 1; v <= 3; v++ {
			if string(history[v]) != fmt.Sprintf(`{"title":"v%d"}`, v) {
				t.Fatalf("%s v%d = %s after restore", name, v, history[v])
			}
		}
	}
	if len(fsmHistory(t, target, "plain")) != 0 {
		t.Fatal("schema-less topic gained a history through restore")
	}
	if got := target.versions.schemaVersion("child"); got <= versionBefore {
		t.Fatalf("schema version after restore = %d, want > %d so cached registries reload", got, versionBefore)
	}
	// The restored history is append-only from where the snapshot left it.
	if err := fsmPutSchema(t, target, "parent", 3, []byte(`{"title":"overwrite"}`)); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("overwrite after restore error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	if err := fsmPutSchema(t, target, "parent", 4, []byte(`{"title":"v4"}`)); err != nil {
		t.Fatalf("v4 after restore: %v", err)
	}
	if _, found := fsmSchema(t, target, "child", 4); !found {
		t.Fatal("restored parent link did not propagate v4 to the child")
	}
}

// Detach keeps the child's adopted history, and a re-attach anywhere is
// gated on that history matching the new parent's exactly.
func TestApplyAttachChild_DetachAndReattachSchemaRules(t *testing.T) {
	f := newFanoutFSM(t)
	for _, name := range []string{"p1", "p2", "p3", "child"} {
		fsmCreateTopic(t, f, name)
	}
	v1 := []byte(`{"title":"v1"}`)
	v2 := []byte(`{"title":"v2"}`)
	if err := fsmPutSchema(t, f, "p1", 1, v1); err != nil {
		t.Fatal(err)
	}
	if err := fsmPutSchema(t, f, "p2", 1, []byte(`{"title":"other"}`)); err != nil {
		t.Fatal(err)
	}
	if err := fsmPutSchema(t, f, "p3", 1, v1); err != nil {
		t.Fatal(err)
	}

	if err := fsmAttach(t, f, "p1", "child"); err != nil {
		t.Fatalf("attach to p1: %v", err)
	}
	if got, _ := fsmSchema(t, f, "child", 1); string(got) != string(v1) {
		t.Fatalf("child did not adopt p1's v1: %s", got)
	}
	if err := fsmDetach(t, f, "p1", "child"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if got, found := fsmSchema(t, f, "child", 1); !found || string(got) != string(v1) {
		t.Fatalf("child lost its history on detach: %s (found=%v)", got, found)
	}

	// A parent update after detach no longer reaches the child.
	if err := fsmPutSchema(t, f, "p1", 2, v2); err != nil {
		t.Fatal(err)
	}
	if _, found := fsmSchema(t, f, "child", 2); found {
		t.Fatal("detached child received the former parent's v2")
	}

	// Re-attach to a parent with a different history is refused; to
	// one with an identical history it is allowed.
	if err := fsmAttach(t, f, "p2", "child"); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
		t.Fatalf("attach to p2 error = %v, want %v", err, errs.ErrFanoutSchemaMismatch)
	}
	if err := fsmAttach(t, f, "p1", "child"); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
		t.Fatalf("re-attach to p1 (now at v2) error = %v, want %v", err, errs.ErrFanoutSchemaMismatch)
	}
	if err := fsmAttach(t, f, "p3", "child"); err != nil {
		t.Fatalf("attach to p3 with identical history: %v", err)
	}
	if err := fsmDetach(t, f, "p3", "child"); err != nil {
		t.Fatal(err)
	}

	// The standalone child may evolve on its own, and catching up to the
	// old parent's exact history makes it attachable there again.
	if err := fsmPutSchema(t, f, "child", 2, v2); err != nil {
		t.Fatalf("child v2: %v", err)
	}
	if err := fsmAttach(t, f, "p1", "child"); err != nil {
		t.Fatalf("re-attach to p1 after catching up: %v", err)
	}
	if err := fsmPutSchema(t, f, "child", 3, []byte(`{"title":"v3"}`)); !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("attached child put error = %v, want %v", err, errs.ErrFanoutSchemaManaged)
	}
}

// Delete drops the whole history; a recreated topic of the same name
// starts at v1 with whatever it is given.
func TestApplyDeleteTopic_DropsSchemaHistory(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "orders")
	for v := 1; v <= 2; v++ {
		if err := fsmPutSchema(t, f, "orders", v, []byte(fmt.Sprintf(`{"title":"v%d"}`, v))); err != nil {
			t.Fatal(err)
		}
	}
	name, err := json.Marshal("orders")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.applyDeleteTopic(name); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := len(fsmHistory(t, f, "orders")); got != 0 {
		t.Fatalf("history has %d versions after delete, want 0", got)
	}
	fsmCreateTopic(t, f, "orders")
	if err := fsmPutSchema(t, f, "orders", 2, []byte(`{"title":"resumed"}`)); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("v2 on a fresh incarnation error = %v, want a gap error", err)
	}
	if err := fsmPutSchema(t, f, "orders", 1, []byte(`{"title":"fresh"}`)); err != nil {
		t.Fatalf("v1 on a fresh incarnation: %v", err)
	}
	if got, _ := fsmSchema(t, f, "orders", 1); string(got) != `{"title":"fresh"}` {
		t.Fatalf("v1 = %s, want the recreated topic's schema", got)
	}
}
