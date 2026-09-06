package messaging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Once a node's registry holds ANY version of a topic's schema, Validate
// never returns ErrSchemaNotFound again, so the lazy metastore reload
// (loadPersistedSchemasCached, keyed by the metastore's schema version)
// is never consulted. A newer version registered through the leader is
// invisible to every other node until it restarts: a payload the
// current schema accepts is rejected on partitions owned by those nodes.
func TestAudit2SchemaVersionRegisteredElsewhereIsNeverPickedUp(t *testing.T) {
	store := newTestStore(t)
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(store, schema.NewJSONSchema(), fixedPartitionManager{}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")

	ctx := context.Background()
	const name = "orders"
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	v1 := []byte(`{"type":"object","properties":{"qty":{"type":"string"}}}`)
	if err := store.PutSchema(ctx, name, 1, v1); err != nil {
		t.Fatal(err)
	}

	// First produce on this node: lazy-loads v1 into the local registry.
	if err := e.validateProducePayload(ctx, name, []byte(`{"qty":"1"}`)); err != nil {
		t.Fatalf("v1-valid payload rejected: %v", err)
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{"qty":1}`)); err == nil {
		t.Fatal("integer qty accepted under v1; test premise broken")
	}

	// The leader (another node) registers v2, which widens qty to
	// string|integer. Replication lands it in this node's metastore
	// replica and bumps the schema version.
	v2 := []byte(`{"type":"object","properties":{"qty":{"type":["string","integer"]}}}`)
	if err := store.PutSchema(ctx, name, 2, v2); err != nil {
		t.Fatal(err)
	}
	if store.SchemaVersion(name) == 0 {
		t.Fatal("schema version did not advance")
	}

	// The metastore says v2 is current; this node still validates
	// against v1 and rejects a payload every other node accepts.
	if err := e.validateProducePayload(ctx, name, []byte(`{"qty":1}`)); err != nil {
		t.Errorf("payload valid under the current schema v2 rejected: %v", err)
	}
}
