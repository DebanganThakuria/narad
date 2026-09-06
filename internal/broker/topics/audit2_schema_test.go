package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// A schema update is compatibility-checked against the LOCAL in-memory
// registry, not against the metastore. A node whose registry has never
// loaded the topic's history (a leader elected after the topic got its
// schema on another node, and that never produced to it) treats the
// update as the first version: the compatibility check is skipped and
// the metastore's v1 is overwritten.
func TestAudit2SchemaUpdateOnColdRegistryBypassesCompatAndOverwritesV1(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	v1 := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	if err := ms.PutSchema(context.Background(), testTopicName, 1, v1); err != nil {
		t.Fatal(err)
	}

	// A fresh registry: this node never loaded the topic's schema history.
	m := newTestManager(t, ms, schema.NewJSONSchema())

	// Incompatible with v1: "id" removed, new required property added.
	incompatible := []byte(`{"type":"object","properties":{"name":{"type":"integer"}},"required":["name"]}`)
	_, err := m.UpdateTopicSchema(context.Background(), testTopicName, incompatible, 0)
	if err == nil {
		t.Errorf("incompatible schema was accepted (want %v)", errs.ErrSchemaIncompatible)
	} else if !errors.Is(err, errs.ErrSchemaIncompatible) {
		t.Errorf("err = %v, want %v", err, errs.ErrSchemaIncompatible)
	}
	if ms.lastSchemaVersion == 1 && string(ms.lastSchemaBytes) == string(incompatible) {
		t.Errorf("metastore schema v1 was overwritten by the incompatible schema: history lost")
	}
	if got := string(ms.schemas[testTopicName][1]); got != string(v1) {
		t.Errorf("schema v1 in metastore = %s, want the original v1", got)
	}
}
