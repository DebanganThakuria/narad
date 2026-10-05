package topics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// Re-registering the current schema (same JSON value, different
// formatting) registers nothing: a client retrying a lost response
// must not grow the history, and the compatibility check is skipped.
func TestUpdateTopicSchema_IdenticalSchemaIsIdempotent(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"integer"}}}`)
	if err := ms.PutSchema(context.Background(), testTopicName, 1, v1); err != nil {
		t.Fatal(err)
	}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	putsBefore := ms.putSchemaCalls

	reformatted := []byte("{ \"properties\": {\"id\": {\"type\": \"integer\"}},\n \"type\": \"object\" }")
	updated, err := manager.UpdateTopicSchema(context.Background(), testTopicName, reformatted, 0)
	if err != nil {
		t.Fatalf("UpdateTopicSchema() error = %v", err)
	}
	if updated.Name != testTopicName {
		t.Fatalf("UpdateTopicSchema() topic = %q", updated.Name)
	}
	if ms.putSchemaCalls != putsBefore {
		t.Fatalf("PutSchema() called %d times for an unchanged schema, want 0", ms.putSchemaCalls-putsBefore)
	}
	if reg.compatCalls != 0 {
		t.Fatalf("CheckCompatible() called %d times for an unchanged schema, want 0", reg.compatCalls)
	}
	if len(ms.schemas[testTopicName]) != 1 {
		t.Fatalf("history has %d versions after an idempotent update, want 1", len(ms.schemas[testTopicName]))
	}

	// With the matching precondition it is still a no-op.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, v1, 1); err != nil {
		t.Fatalf("UpdateTopicSchema() with matching base version error = %v", err)
	}
	if ms.putSchemaCalls != putsBefore {
		t.Fatal("PutSchema() called for an unchanged schema with a matching base version")
	}
}

// schema_base_version is a precondition on the current version.
func TestUpdateTopicSchema_BaseVersionPrecondition(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)

	// No schema yet: a base version cannot match anything.
	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v1"}`), 1)
	if !errors.Is(err, errs.ErrSchemaVersionConflict) {
		t.Fatalf("base version on a schema-less topic error = %v, want %v", err, errs.ErrSchemaVersionConflict)
	}
	if ms.putSchemaCalls != 0 {
		t.Fatal("PutSchema() called despite a failed precondition")
	}
	// Negative is a malformed request, not a conflict.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v1"}`), -1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative base version error = %v, want %v", err, ErrInvalid)
	}

	// Unconditional first version, then a matching precondition.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v1"}`), 0); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v2"}`), 1); err != nil {
		t.Fatalf("v2 with base 1: %v", err)
	}
	if ms.lastSchemaVersion != 2 {
		t.Fatalf("PutSchema() version = %d, want 2", ms.lastSchemaVersion)
	}

	// Two clients that both read v2: the second one loses.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v3-a"}`), 2); err != nil {
		t.Fatalf("v3 with base 2: %v", err)
	}
	putsBefore := ms.putSchemaCalls
	_, err = manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"v3-b"}`), 2)
	if !errors.Is(err, errs.ErrSchemaVersionConflict) {
		t.Fatalf("stale base version error = %v, want %v", err, errs.ErrSchemaVersionConflict)
	}
	if ms.putSchemaCalls != putsBefore {
		t.Fatal("PutSchema() called despite a stale base version")
	}
	if got := string(ms.schemas[testTopicName][3]); got != `{"x-rev":"v3-a"}` {
		t.Fatalf("v3 = %s, want the first writer's schema", got)
	}
	if len(ms.schemas[testTopicName]) != 3 {
		t.Fatalf("history has %d versions, want 3", len(ms.schemas[testTopicName]))
	}
}

// The per-topic version cap is enforced before anything is proposed;
// an unchanged schema is still accepted on a full history.
func TestUpdateTopicSchema_HistoryCap(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	for v := 1; v <= metastore.MaxSchemaVersions; v++ {
		if err := ms.PutSchema(context.Background(), testTopicName, v, []byte(fmt.Sprintf(`{"x-rev":"v%d"}`, v))); err != nil {
			t.Fatal(err)
		}
	}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	putsBefore := ms.putSchemaCalls

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"x-rev":"one too many"}`), 0)
	if !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("UpdateTopicSchema() on a full history error = %v, want %v", err, errs.ErrSchemaHistoryFull)
	}
	if ms.putSchemaCalls != putsBefore || reg.compatCalls != 0 {
		t.Fatalf("PutSchema()/CheckCompatible() called (%d/%d) on a full history", ms.putSchemaCalls-putsBefore, reg.compatCalls)
	}
	latest := []byte(fmt.Sprintf(`{"x-rev":"v%d"}`, metastore.MaxSchemaVersions))
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, latest, 0); err != nil {
		t.Fatalf("re-registering the latest on a full history: %v", err)
	}
}

// TopicSchemaHistory and GetTopicDetails read the persisted history:
// every version in order, the latest as the current one, and nothing
// for a topic without a schema.
func TestTopicSchemaHistoryAndDetails(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	ms.topics["plain"] = topic.Topic{Name: "plain", Partitions: 3}
	manager := newTestManager(t, ms, &fakeSchemaRegistry{})

	history, err := manager.TopicSchemaHistory(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("TopicSchemaHistory() on a schema-less topic error = %v", err)
	}
	if history.Topic != testTopicName || history.Version != 0 || len(history.Versions) != 0 {
		t.Fatalf("schema-less history = %+v, want version 0 and no versions", history)
	}
	details, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails(): %v", err)
	}
	if details.SchemaVersion != 0 || details.Schema != nil {
		t.Fatalf("schema-less details = version %d schema %s, want none", details.SchemaVersion, details.Schema)
	}

	for v := 1; v <= 3; v++ {
		if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(fmt.Sprintf(`{"x-rev":"v%d"}`, v)), 0); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
	}
	history, err = manager.TopicSchemaHistory(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("TopicSchemaHistory(): %v", err)
	}
	if history.Version != 3 || len(history.Versions) != 3 {
		t.Fatalf("history = %+v, want 3 versions with current 3", history)
	}
	for i, v := range history.Versions {
		if v.Version != i+1 || string(v.Schema) != fmt.Sprintf(`{"x-rev":"v%d"}`, i+1) {
			t.Fatalf("versions[%d] = %d %s", i, v.Version, v.Schema)
		}
	}
	details, err = manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails(): %v", err)
	}
	if details.SchemaVersion != 3 || string(details.Schema) != `{"x-rev":"v3"}` {
		t.Fatalf("details = version %d schema %s, want v3", details.SchemaVersion, details.Schema)
	}

	if _, err := manager.TopicSchemaHistory(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TopicSchemaHistory(missing) error = %v, want %v", err, ErrNotFound)
	}
	if _, err := manager.TopicSchemaHistory(context.Background(), ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("TopicSchemaHistory(\"\") error = %v, want %v", err, ErrInvalid)
	}
}

// A schema-managed child cannot be updated directly even with a
// matching base version, and its history is readable.
func TestUpdateTopicSchema_ChildIsParentManagedRegardlessOfBaseVersion(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	ms.topics["child"] = topic.Topic{Name: "child", Partitions: 3}
	manager := newTestManager(t, ms, &fakeSchemaRegistry{})
	if _, err := manager.UpdateTopicSchema(context.Background(), "parent", []byte(`{"x-rev":"v1"}`), 0); err != nil {
		t.Fatal(err)
	}
	if err := ms.AttachChild(context.Background(), "parent", "child", 0); err != nil {
		t.Fatal(err)
	}
	_, err := manager.UpdateTopicSchema(context.Background(), "child", []byte(`{"x-rev":"v2"}`), 1)
	if !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("child update error = %v, want %v", err, errs.ErrFanoutSchemaManaged)
	}
	// Detached, the child manages its own history again.
	if err := ms.DetachChild(context.Background(), "parent", "child"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.UpdateTopicSchema(context.Background(), "child", []byte(`{"x-rev":"child-v1"}`), 0); err != nil {
		t.Fatalf("detached child update: %v", err)
	}
	if ms.lastSchemaTopic != "child" || ms.lastSchemaVersion != 1 {
		t.Fatalf("PutSchema() = %s v%d, want child v1", ms.lastSchemaTopic, ms.lastSchemaVersion)
	}
}

// TestAnnotationOnlySchemaChangeRegistersNothing is the audit's repro
// (verify-schemas-2): twenty PATCHes that only reword a description
// each appended a full version, copied into every child, and the
// history grew without anything it accepts changing. A change only to
// annotations (title, description, examples, $comment, default,
// deprecated, readOnly, writeOnly, at schema positions) now registers
// nothing; a property named "description" is not an annotation.
func TestAnnotationOnlySchemaChangeRegistersNothing(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	ctx := context.Background()

	base := `{"type":"object","properties":{"id":{"type":"integer","description":"%s"}},"description":"%s"}`
	if _, err := manager.UpdateTopicSchema(ctx, testTopicName, []byte(fmt.Sprintf(base, "v0", "v0")), 0); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 20; i++ {
		next := fmt.Sprintf(base, fmt.Sprintf("reworded %d", i), fmt.Sprintf("doc %d", i))
		if _, err := manager.UpdateTopicSchema(ctx, testTopicName, []byte(next), 0); err != nil {
			t.Fatalf("annotation-only PATCH %d: %v", i, err)
		}
	}
	for _, next := range []string{
		`{"type":"object","properties":{"id":{"type":"integer","title":"ID","examples":[1,2],"default":1,"deprecated":true,"readOnly":true,"writeOnly":false,"$comment":"c"}}}`,
		`{"title":"T","$comment":"x","type":"object","properties":{"id":{"type":"integer"}}}`,
	} {
		if _, err := manager.UpdateTopicSchema(ctx, testTopicName, []byte(next), 0); err != nil {
			t.Fatalf("annotation-only PATCH %s: %v", next, err)
		}
	}
	if got := len(ms.schemas[testTopicName]); got != 1 {
		t.Fatalf("history has %d versions after annotation-only PATCHes, want 1", got)
	}
	if reg.compatCalls != 0 {
		t.Fatalf("CheckCompatible() called %d times for annotation-only changes", reg.compatCalls)
	}

	// A change to what the schema accepts is a new version, and so is
	// a new property named "description".
	for i, next := range []string{
		`{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}}}`,
		`{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"},"description":{"type":"string"}}}`,
	} {
		if _, err := manager.UpdateTopicSchema(ctx, testTopicName, []byte(next), 0); err != nil {
			t.Fatal(err)
		}
		if got := len(ms.schemas[testTopicName]); got != i+2 {
			t.Fatalf("history has %d versions after a real change, want %d", got, i+2)
		}
	}
}

// paddedSchema is a schema of about size bytes accepting more with each
// rev: maxProperties rises, a widening the compatibility check allows.
func paddedSchema(rev, size int) []byte {
	head := fmt.Sprintf(`{"type":"object","maxProperties":%d,"x-pad":"`, 1000+rev)
	return []byte(head + strings.Repeat("p", max(size-len(head)-2, 0)) + `"}`)
}

// TestSchemaHistoryStopsAtTheTopicBudget: each version was capped at
// 256 KiB and each history at 1000 versions, so one topic could store
// 256 MB that every node snapshots and restores. A history now stops at
// 4 MiB of stored versions with a 409 that names the budget; the
// unchanged latest is still accepted.
func TestSchemaHistoryStopsAtTheTopicBudget(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManager(t, ms, &fakeSchemaRegistry{})
	ctx := context.Background()

	const size = 200 << 10
	var refused error
	stored := 0
	for rev := 1; rev <= 40 && refused == nil; rev++ {
		_, err := manager.UpdateTopicSchema(ctx, testTopicName, paddedSchema(rev, size), 0)
		switch {
		case err == nil:
			stored += size
		case errors.Is(err, errs.ErrSchemaHistoryFull):
			refused = err
		default:
			t.Fatalf("version %d: %v", rev, err)
		}
	}
	if refused == nil {
		t.Fatalf("40 versions of 200 KiB (%d bytes) all accepted; the per-topic budget is 4 MiB", stored)
	}
	if stored > 4<<20 || stored+size <= 4<<20 {
		t.Fatalf("refused after %d stored bytes; want the refusal at the 4 MiB budget", stored)
	}
	if !strings.Contains(refused.Error(), "budget") {
		t.Errorf("refusal does not name the budget: %v", refused)
	}
	latest := ms.schemas[testTopicName][len(ms.schemas[testTopicName])]
	if _, err := manager.UpdateTopicSchema(ctx, testTopicName, latest, 0); err != nil {
		t.Fatalf("re-registering the latest on a full history: %v", err)
	}
}

// TestSchemaIsStoredCompacted (audit schemas:6): a pretty-printed body
// was stored byte for byte, indentation included, while every read path
// hands schemas back compacted. Schemas are now stored compacted, on
// create and on update.
func TestSchemaIsStoredCompacted(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, &fakeSchemaRegistry{})
	ctx := context.Background()
	pretty := "{\n  \"type\": \"object\",\n  \"properties\": {\n    \"id\": { \"type\": \"integer\" }\n  }\n}\n"
	if _, err := manager.CreateTopic(ctx, CreateOpts{Name: testTopicName, Partitions: 3, Schema: []byte(pretty)}); err != nil {
		t.Fatal(err)
	}
	if got, want := string(ms.schemas[testTopicName][1]), `{"type":"object","properties":{"id":{"type":"integer"}}}`; got != want {
		t.Fatalf("stored v1 = %q, want %q", got, want)
	}
	next := "{ \"type\": \"object\",\t\"properties\": { \"id\": {\"type\": \"integer\"}, \"n\": {\"type\": \"string\"} } }"
	if _, err := manager.UpdateTopicSchema(ctx, testTopicName, []byte(next), 0); err != nil {
		t.Fatal(err)
	}
	if got, want := string(ms.schemas[testTopicName][2]), `{"type":"object","properties":{"id":{"type":"integer"},"n":{"type":"string"}}}`; got != want {
		t.Fatalf("stored v2 = %q, want %q", got, want)
	}
}
