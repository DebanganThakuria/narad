package topics

import (
	"context"
	"errors"
	"fmt"
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
	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v1"}`), 1)
	if !errors.Is(err, errs.ErrSchemaVersionConflict) {
		t.Fatalf("base version on a schema-less topic error = %v, want %v", err, errs.ErrSchemaVersionConflict)
	}
	if ms.putSchemaCalls != 0 {
		t.Fatal("PutSchema() called despite a failed precondition")
	}
	// Negative is a malformed request, not a conflict.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v1"}`), -1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative base version error = %v, want %v", err, ErrInvalid)
	}

	// Unconditional first version, then a matching precondition.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v1"}`), 0); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v2"}`), 1); err != nil {
		t.Fatalf("v2 with base 1: %v", err)
	}
	if ms.lastSchemaVersion != 2 {
		t.Fatalf("PutSchema() version = %d, want 2", ms.lastSchemaVersion)
	}

	// Two clients that both read v2: the second one loses.
	if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v3-a"}`), 2); err != nil {
		t.Fatalf("v3 with base 2: %v", err)
	}
	putsBefore := ms.putSchemaCalls
	_, err = manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"v3-b"}`), 2)
	if !errors.Is(err, errs.ErrSchemaVersionConflict) {
		t.Fatalf("stale base version error = %v, want %v", err, errs.ErrSchemaVersionConflict)
	}
	if ms.putSchemaCalls != putsBefore {
		t.Fatal("PutSchema() called despite a stale base version")
	}
	if got := string(ms.schemas[testTopicName][3]); got != `{"title":"v3-a"}` {
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
		if err := ms.PutSchema(context.Background(), testTopicName, v, []byte(fmt.Sprintf(`{"title":"v%d"}`, v))); err != nil {
			t.Fatal(err)
		}
	}
	reg := &fakeSchemaRegistry{}
	manager := newTestManager(t, ms, reg)
	putsBefore := ms.putSchemaCalls

	_, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(`{"title":"one too many"}`), 0)
	if !errors.Is(err, errs.ErrSchemaHistoryFull) {
		t.Fatalf("UpdateTopicSchema() on a full history error = %v, want %v", err, errs.ErrSchemaHistoryFull)
	}
	if ms.putSchemaCalls != putsBefore || reg.compatCalls != 0 {
		t.Fatalf("PutSchema()/CheckCompatible() called (%d/%d) on a full history", ms.putSchemaCalls-putsBefore, reg.compatCalls)
	}
	latest := []byte(fmt.Sprintf(`{"title":"v%d"}`, metastore.MaxSchemaVersions))
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
		if _, err := manager.UpdateTopicSchema(context.Background(), testTopicName, []byte(fmt.Sprintf(`{"title":"v%d"}`, v)), 0); err != nil {
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
		if v.Version != i+1 || string(v.Schema) != fmt.Sprintf(`{"title":"v%d"}`, i+1) {
			t.Fatalf("versions[%d] = %d %s", i, v.Version, v.Schema)
		}
	}
	details, err = manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails(): %v", err)
	}
	if details.SchemaVersion != 3 || string(details.Schema) != `{"title":"v3"}` {
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
	if _, err := manager.UpdateTopicSchema(context.Background(), "parent", []byte(`{"title":"v1"}`), 0); err != nil {
		t.Fatal(err)
	}
	if err := ms.AttachChild(context.Background(), "parent", "child", 0); err != nil {
		t.Fatal(err)
	}
	_, err := manager.UpdateTopicSchema(context.Background(), "child", []byte(`{"title":"v2"}`), 1)
	if !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("child update error = %v, want %v", err, errs.ErrFanoutSchemaManaged)
	}
	// Detached, the child manages its own history again.
	if err := ms.DetachChild(context.Background(), "parent", "child"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.UpdateTopicSchema(context.Background(), "child", []byte(`{"title":"child-v1"}`), 0); err != nil {
		t.Fatalf("detached child update: %v", err)
	}
	if ms.lastSchemaTopic != "child" || ms.lastSchemaVersion != 1 {
		t.Fatalf("PutSchema() = %s v%d, want child v1", ms.lastSchemaTopic, ms.lastSchemaVersion)
	}
}
