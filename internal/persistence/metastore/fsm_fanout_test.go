package metastore

// White-box tests for the fan-out FSM handlers: every attach/detach
// invariant, the schema gate matrix, link dissolution on delete, link
// preservation on update, and schema propagation to children. These
// call the apply* handlers directly (no Raft) — the same way committed
// log entries reach them — so error values and bbolt state are asserted
// deterministically.

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

func newFanoutFSM(t *testing.T) *fsmState {
	t.Helper()
	f, err := newFSM(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("newFSM: %v", err)
	}
	t.Cleanup(func() { f.db.Close() })
	return f
}

func fsmCreateTopic(t *testing.T, f *fsmState, name string) {
	t.Helper()
	data, err := json.Marshal(topic.Topic{Name: name, Partitions: 3, RetentionMs: 3_600_000})
	if err != nil {
		t.Fatalf("Marshal(%s): %v", name, err)
	}
	if err := f.applyCreateTopic(data); err != nil {
		t.Fatalf("applyCreateTopic(%s): %v", name, err)
	}
}

func fsmAttach(t *testing.T, f *fsmState, parent, child string) error {
	t.Helper()
	data, err := json.Marshal(childLinkPayload{Parent: parent, Child: child})
	if err != nil {
		t.Fatalf("Marshal link: %v", err)
	}
	return f.applyAttachChild(data)
}

func fsmDetach(t *testing.T, f *fsmState, parent, child string) error {
	t.Helper()
	data, err := json.Marshal(childLinkPayload{Parent: parent, Child: child})
	if err != nil {
		t.Fatalf("Marshal link: %v", err)
	}
	return f.applyDetachChild(data)
}

func fsmPutSchema(t *testing.T, f *fsmState, topicName string, version int, schema []byte) error {
	t.Helper()
	data, err := json.Marshal(schemaPayload{Topic: topicName, Version: version, Schema: schema})
	if err != nil {
		t.Fatalf("Marshal schema payload: %v", err)
	}
	return f.applyPutSchema(data)
}

func fsmGetTopic(t *testing.T, f *fsmState, name string) topic.Topic {
	t.Helper()
	var out topic.Topic
	err := f.view(func(tx *bolt.Tx) error {
		var err error
		out, err = getTopicRecord(tx, name)
		return err
	})
	if err != nil {
		t.Fatalf("getTopicRecord(%s): %v", name, err)
	}
	return out
}

func fsmSchema(t *testing.T, f *fsmState, topicName string, version int) ([]byte, bool) {
	t.Helper()
	var out []byte
	found := false
	err := f.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSchemas).Get(schemaKey(topicName, version))
		if v != nil {
			found = true
			out = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read schema %s v%d: %v", topicName, version, err)
	}
	return out, found
}

func TestApplyAttachChild_LinksBothRecords(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child-a")
	fsmCreateTopic(t, f, "child-b")

	parentVer := f.versions.topicVersion("parent")
	childVer := f.versions.topicVersion("child-a")

	if err := fsmAttach(t, f, "parent", "child-a"); err != nil {
		t.Fatalf("attach child-a: %v", err)
	}
	if err := fsmAttach(t, f, "parent", "child-b"); err != nil {
		t.Fatalf("attach child-b: %v", err)
	}

	p := fsmGetTopic(t, f, "parent")
	if !p.IsParent() || !slices.Equal(p.Children, []string{"child-a", "child-b"}) {
		t.Fatalf("parent record = %+v, want role=parent children=[child-a child-b]", p)
	}
	if p.Parent != "" {
		t.Fatalf("parent.Parent = %q, want empty", p.Parent)
	}
	c := fsmGetTopic(t, f, "child-a")
	if !c.IsChild() || c.Parent != "parent" || len(c.Children) != 0 {
		t.Fatalf("child record = %+v, want role=child parent=parent no children", c)
	}
	if got := f.versions.topicVersion("parent"); got <= parentVer {
		t.Fatalf("parent topic version = %d, want > %d", got, parentVer)
	}
	if got := f.versions.topicVersion("child-a"); got <= childVer {
		t.Fatalf("child topic version = %d, want > %d", got, childVer)
	}
}

func TestApplyAttachChild_RejectsInvariantViolations(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child")
	fsmCreateTopic(t, f, "other-parent")
	fsmCreateTopic(t, f, "other-child")
	fsmCreateTopic(t, f, "spare")
	if err := fsmAttach(t, f, "parent", "child"); err != nil {
		t.Fatalf("setup attach: %v", err)
	}
	if err := fsmAttach(t, f, "other-parent", "other-child"); err != nil {
		t.Fatalf("setup attach: %v", err)
	}

	cases := []struct {
		name          string
		parent, child string
		wantErr       error
	}{
		{"missing parent", "ghost", "spare", ErrNotFound},
		{"missing child", "parent", "ghost", ErrNotFound},
		{"self attach", "spare", "spare", errs.ErrFanoutRoleConflict},
		{"parent is a child", "child", "spare", errs.ErrFanoutRoleConflict},
		{"child is a parent", "spare", "parent", errs.ErrFanoutRoleConflict},
		{"child attached elsewhere", "parent", "other-child", errs.ErrFanoutRoleConflict},
		{"re-attach same pair", "parent", "child", ErrAlreadyExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := fsmAttach(t, f, tc.parent, tc.child)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("attach(%s, %s) error = %v, want %v", tc.parent, tc.child, err, tc.wantErr)
			}
		})
	}

	// A rejected attach must leave no partial link behind.
	if got := fsmGetTopic(t, f, "spare"); got.EffectiveRole() != topic.RoleStandalone {
		t.Fatalf("spare role after rejected attaches = %q, want standalone", got.EffectiveRole())
	}
}

func TestApplyAttachChild_EnforcesChildCap(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	for i := range topic.MaxChildrenPerParent {
		name := fmt.Sprintf("child-%03d", i)
		fsmCreateTopic(t, f, name)
		if err := fsmAttach(t, f, "parent", name); err != nil {
			t.Fatalf("attach %s: %v", name, err)
		}
	}
	fsmCreateTopic(t, f, "one-too-many")
	if err := fsmAttach(t, f, "parent", "one-too-many"); !errors.Is(err, errs.ErrFanoutChildLimit) {
		t.Fatalf("attach past cap error = %v, want %v", err, errs.ErrFanoutChildLimit)
	}
	if p := fsmGetTopic(t, f, "parent"); len(p.Children) != topic.MaxChildrenPerParent {
		t.Fatalf("parent has %d children, want %d", len(p.Children), topic.MaxChildrenPerParent)
	}
}

func TestApplyAttachChild_SchemaGate(t *testing.T) {
	schemaA := []byte(`{"type":"object","properties":{"a":{"type":"string"}}}`)
	schemaA2 := []byte(`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}`)
	schemaB := []byte(`{"type":"object","properties":{"z":{"type":"boolean"}}}`)

	setup := func(t *testing.T, parentSchemas, childSchemas [][]byte) *fsmState {
		f := newFanoutFSM(t)
		fsmCreateTopic(t, f, "parent")
		fsmCreateTopic(t, f, "child")
		for i, s := range parentSchemas {
			if err := fsmPutSchema(t, f, "parent", i+1, s); err != nil {
				t.Fatalf("put parent schema v%d: %v", i+1, err)
			}
		}
		for i, s := range childSchemas {
			if err := fsmPutSchema(t, f, "child", i+1, s); err != nil {
				t.Fatalf("put child schema v%d: %v", i+1, err)
			}
		}
		return f
	}

	t.Run("neither has a schema", func(t *testing.T) {
		f := setup(t, nil, nil)
		if err := fsmAttach(t, f, "parent", "child"); err != nil {
			t.Fatalf("attach: %v", err)
		}
		if _, found := fsmSchema(t, f, "child", 1); found {
			t.Fatal("child gained a schema from a schema-less parent")
		}
	})

	t.Run("child adopts the parent's full history", func(t *testing.T) {
		f := setup(t, [][]byte{schemaA, schemaA2}, nil)
		before := f.versions.schemaVersion("child")
		if err := fsmAttach(t, f, "parent", "child"); err != nil {
			t.Fatalf("attach: %v", err)
		}
		for version, want := range map[int][]byte{1: schemaA, 2: schemaA2} {
			got, found := fsmSchema(t, f, "child", version)
			if !found || string(got) != string(want) {
				t.Fatalf("child schema v%d = %q (found=%v), want adopted %q", version, got, found, want)
			}
		}
		if got := f.versions.schemaVersion("child"); got <= before {
			t.Fatalf("child schema version = %d after adoption, want > %d", got, before)
		}
	})

	t.Run("identical schemas attach", func(t *testing.T) {
		f := setup(t, [][]byte{schemaA, schemaA2}, [][]byte{schemaA, schemaA2})
		if err := fsmAttach(t, f, "parent", "child"); err != nil {
			t.Fatalf("attach: %v", err)
		}
	})

	t.Run("different schema is rejected", func(t *testing.T) {
		f := setup(t, [][]byte{schemaA}, [][]byte{schemaB})
		if err := fsmAttach(t, f, "parent", "child"); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
			t.Fatalf("attach error = %v, want %v", err, errs.ErrFanoutSchemaMismatch)
		}
	})

	t.Run("schema'd child under schema-less parent is rejected", func(t *testing.T) {
		f := setup(t, nil, [][]byte{schemaB})
		if err := fsmAttach(t, f, "parent", "child"); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
			t.Fatalf("attach error = %v, want %v", err, errs.ErrFanoutSchemaMismatch)
		}
	})

	t.Run("shorter child history is rejected", func(t *testing.T) {
		f := setup(t, [][]byte{schemaA, schemaA2}, [][]byte{schemaA})
		if err := fsmAttach(t, f, "parent", "child"); !errors.Is(err, errs.ErrFanoutSchemaMismatch) {
			t.Fatalf("attach error = %v, want %v", err, errs.ErrFanoutSchemaMismatch)
		}
	})
}

func TestApplyDetachChild(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child-a")
	fsmCreateTopic(t, f, "child-b")
	schemaA := []byte(`{"type":"object"}`)
	if err := fsmPutSchema(t, f, "parent", 1, schemaA); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	if err := fsmAttach(t, f, "parent", "child-a"); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if err := fsmAttach(t, f, "parent", "child-b"); err != nil {
		t.Fatalf("attach: %v", err)
	}

	if err := fsmDetach(t, f, "parent", "child-a"); err != nil {
		t.Fatalf("detach child-a: %v", err)
	}
	if c := fsmGetTopic(t, f, "child-a"); c.EffectiveRole() != topic.RoleStandalone || c.Parent != "" {
		t.Fatalf("detached child = %+v, want standalone with no parent", c)
	}
	// The detached child keeps its adopted schema and the parent keeps
	// its remaining child.
	if _, found := fsmSchema(t, f, "child-a", 1); !found {
		t.Fatal("detached child lost its adopted schema")
	}
	if p := fsmGetTopic(t, f, "parent"); !p.IsParent() || !slices.Equal(p.Children, []string{"child-b"}) {
		t.Fatalf("parent after first detach = %+v, want role=parent children=[child-b]", p)
	}

	// Detaching the last child reverts the parent to standalone.
	if err := fsmDetach(t, f, "parent", "child-b"); err != nil {
		t.Fatalf("detach child-b: %v", err)
	}
	if p := fsmGetTopic(t, f, "parent"); p.EffectiveRole() != topic.RoleStandalone || len(p.Children) != 0 {
		t.Fatalf("parent after last detach = %+v, want standalone with no children", p)
	}

	// Not-attached detaches are ErrNotFound.
	if err := fsmDetach(t, f, "parent", "child-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("detach unattached error = %v, want %v", err, ErrNotFound)
	}
	if err := fsmDetach(t, f, "ghost", "child-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("detach from missing parent error = %v, want %v", err, ErrNotFound)
	}
}

func TestApplyDeleteTopic_DissolvesParentLinks(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child-a")
	fsmCreateTopic(t, f, "child-b")
	if err := fsmPutSchema(t, f, "parent", 1, []byte(`{"type":"object"}`)); err != nil {
		t.Fatalf("put schema: %v", err)
	}
	for _, c := range []string{"child-a", "child-b"} {
		if err := fsmAttach(t, f, "parent", c); err != nil {
			t.Fatalf("attach %s: %v", c, err)
		}
	}

	childVer := f.versions.topicVersion("child-a")
	name, _ := json.Marshal("parent")
	if err := f.applyDeleteTopic(name); err != nil {
		t.Fatalf("applyDeleteTopic(parent): %v", err)
	}

	for _, c := range []string{"child-a", "child-b"} {
		got := fsmGetTopic(t, f, c)
		if got.EffectiveRole() != topic.RoleStandalone || got.Parent != "" {
			t.Fatalf("child %s after parent delete = %+v, want standalone", c, got)
		}
		// Children keep their adopted schemas even though the parent's
		// schema rows were deleted with it.
		if _, found := fsmSchema(t, f, c, 1); !found {
			t.Fatalf("child %s lost its schema when the parent was deleted", c)
		}
	}
	if _, found := fsmSchema(t, f, "parent", 1); found {
		t.Fatal("parent schema row survived the delete")
	}
	if got := f.versions.topicVersion("child-a"); got <= childVer {
		t.Fatalf("child topic version = %d after parent delete, want > %d", got, childVer)
	}
}

func TestApplyDeleteTopic_UnlinksDeletedChild(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child-a")
	fsmCreateTopic(t, f, "child-b")
	for _, c := range []string{"child-a", "child-b"} {
		if err := fsmAttach(t, f, "parent", c); err != nil {
			t.Fatalf("attach %s: %v", c, err)
		}
	}

	name, _ := json.Marshal("child-a")
	if err := f.applyDeleteTopic(name); err != nil {
		t.Fatalf("applyDeleteTopic(child-a): %v", err)
	}
	if p := fsmGetTopic(t, f, "parent"); !slices.Equal(p.Children, []string{"child-b"}) || !p.IsParent() {
		t.Fatalf("parent after child delete = %+v, want children=[child-b]", p)
	}

	name, _ = json.Marshal("child-b")
	if err := f.applyDeleteTopic(name); err != nil {
		t.Fatalf("applyDeleteTopic(child-b): %v", err)
	}
	if p := fsmGetTopic(t, f, "parent"); p.EffectiveRole() != topic.RoleStandalone || len(p.Children) != 0 {
		t.Fatalf("parent after losing all children = %+v, want standalone", p)
	}
}

func TestApplyUpdateTopic_PreservesFanoutLinks(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child")
	if err := fsmAttach(t, f, "parent", "child"); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// A config update whose payload carries stale (or hostile) link
	// fields must not change the stored link.
	update := topic.Topic{
		Name:        "parent",
		Partitions:  6,
		RetentionMs: 7_200_000,
		Role:        topic.RoleChild,
		Parent:      "someone-else",
		Children:    []string{"bogus"},
	}
	data, _ := json.Marshal(update)
	if err := f.applyUpdateTopic(data); err != nil {
		t.Fatalf("applyUpdateTopic: %v", err)
	}
	got := fsmGetTopic(t, f, "parent")
	if got.Partitions != 6 || got.RetentionMs != 7_200_000 {
		t.Fatalf("config update not applied: %+v", got)
	}
	if !got.IsParent() || !slices.Equal(got.Children, []string{"child"}) || got.Parent != "" {
		t.Fatalf("links after config update = %+v, want role=parent children=[child]", got)
	}
}

func TestApplyPutSchema_PropagatesToChildrenAndGuardsChild(t *testing.T) {
	f := newFanoutFSM(t)
	fsmCreateTopic(t, f, "parent")
	fsmCreateTopic(t, f, "child-a")
	fsmCreateTopic(t, f, "child-b")
	v1 := []byte(`{"type":"object"}`)
	v2 := []byte(`{"type":"object","properties":{"a":{"type":"string"}}}`)
	if err := fsmPutSchema(t, f, "parent", 1, v1); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	for _, c := range []string{"child-a", "child-b"} {
		if err := fsmAttach(t, f, "parent", c); err != nil {
			t.Fatalf("attach %s: %v", c, err)
		}
	}

	childSchemaVer := f.versions.schemaVersion("child-a")
	if err := fsmPutSchema(t, f, "parent", 2, v2); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	for _, c := range []string{"child-a", "child-b"} {
		got, found := fsmSchema(t, f, c, 2)
		if !found || string(got) != string(v2) {
			t.Fatalf("child %s schema v2 = %q (found=%v), want propagated %q", c, got, found, v2)
		}
	}
	if got := f.versions.schemaVersion("child-a"); got <= childSchemaVer {
		t.Fatalf("child schema version = %d after propagation, want > %d", got, childSchemaVer)
	}

	// Direct schema writes to an attached child are parent-managed.
	if err := fsmPutSchema(t, f, "child-a", 3, v2); !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("put schema on attached child error = %v, want %v", err, errs.ErrFanoutSchemaManaged)
	}

	// After detach the child manages its schema again.
	if err := fsmDetach(t, f, "parent", "child-a"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if err := fsmPutSchema(t, f, "child-a", 3, v2); err != nil {
		t.Fatalf("put schema on detached child: %v", err)
	}
}

// Topic records persisted before fan-out existed carry no role field;
// they must read as standalone and be attachable.
func TestFanout_LegacyRecordsReadAsStandalone(t *testing.T) {
	f := newFanoutFSM(t)
	legacy := []byte(`{"name":"legacy","partitions":3,"retention_ms":3600000,` +
		`"visibility_timeout_ms":30000,"max_in_flight_per_partition":1,` +
		`"max_acked_ahead_per_partition":1,"created_at":1}`)
	err := f.update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTopics).Put([]byte("legacy"), legacy)
	})
	if err != nil {
		t.Fatalf("seed legacy record: %v", err)
	}
	fsmCreateTopic(t, f, "child")

	got := fsmGetTopic(t, f, "legacy")
	if got.EffectiveRole() != topic.RoleStandalone || got.IsParent() || got.IsChild() {
		t.Fatalf("legacy record role = %+v, want standalone", got)
	}
	if err := fsmAttach(t, f, "legacy", "child"); err != nil {
		t.Fatalf("attach under legacy parent: %v", err)
	}
	if p := fsmGetTopic(t, f, "legacy"); !p.IsParent() {
		t.Fatalf("legacy parent after attach = %+v, want role=parent", p)
	}
}
