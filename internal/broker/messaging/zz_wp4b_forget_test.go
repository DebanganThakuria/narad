package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// wp4bNewNode wires a topic manager and an engine over one metastore,
// registry, log map and in-flight book, the way broker.New does.
func wp4bNewNode(t *testing.T, store *metastore.Store, reg schema.Registry) (*topics.Manager, *Engine) {
	t.Helper()
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := topics.NewManager(dataDir, store, nil, reg, offsets, logs, topics.Config{
		DefaultPartitions:                3,
		MaxPartitions:                    12,
		DefaultRetentionMs:               3600000,
		DefaultVisibilityTimeoutMs:       30000,
		DefaultMaxInFlightPerPartition:   10,
		DefaultMaxAckedAheadPerPartition: 10,
	}, logger, "node-self")
	e := NewEngine(store, reg, fixedPartitionManager{}, offsets, logs, nil, nil, logger, "node-self")
	t.Cleanup(func() { e.dispatch.close() })
	mgr.SetWaiterReleaser(e)
	return mgr, e
}

// wp4bCachedNames lists the names prefix has entries for in the engine's
// per-topic caches.
func wp4bCachedNames(e *Engine, prefix string) []string {
	var out []string
	e.cacheMu.RLock()
	for name := range e.topicCache {
		if strings.HasPrefix(name, prefix) {
			out = append(out, "topicCache:"+name)
		}
	}
	for name := range e.assignmentCache {
		if strings.HasPrefix(name, prefix) {
			out = append(out, "assignmentCache:"+name)
		}
	}
	for name := range e.schemaLoadCache {
		if strings.HasPrefix(name, prefix) {
			out = append(out, "schemaLoadCache:"+name)
		}
	}
	e.cacheMu.RUnlock()
	e.consumeCursors.Range(func(k, _ any) bool {
		if name := k.(string); strings.HasPrefix(name, prefix) {
			out = append(out, "consumeCursors:"+name)
		}
		return true
	})
	return out
}

// TestWP4BDeletedTopicsLeaveNoEngineCaches churns uniquely named topics
// through create, use and delete. Every name used to leave an entry in
// the topic, assignment and schema caches and the consume cursors of
// every node that served it (about 1.7 KB at 3 partitions), forever; a
// straggling ownership check after the delete put back an empty
// assignment set even where the entry had been dropped.
func TestWP4BDeletedTopicsLeaveNoEngineCaches(t *testing.T) {
	store := wp4bNewStore(t)
	reg := schema.NewJSONSchema()
	mgr, e := wp4bNewNode(t, store, reg)
	ctx := context.Background()

	for i := range 40 {
		name := fmt.Sprintf("churn-%02d", i)
		if _, err := mgr.CreateTopic(ctx, topics.CreateOpts{Name: name, Partitions: 3, Schema: []byte(wp4bDriverSchema)}); err != nil {
			t.Fatal(err)
		}
		for p := 0; p < 3; p++ {
			if err := store.AssignPartition(ctx, name, p, "node-self"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := e.getTopic(ctx, name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.getAssignment(name, 1); err != nil {
			t.Fatal(err)
		}
		if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
			t.Fatal(err)
		}
		e.nextConsumeScanStart(name, 3)

		if err := mgr.DeleteTopic(ctx, name); err != nil {
			t.Fatal(err)
		}
		// Stragglers: ownership checks and lookups that reach the engine
		// after the delete.
		if _, err := e.getAssignment(name, 1); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("getAssignment of a deleted topic = %v, want not found", err)
		}
		if rows, err := e.listAssignments(name); err != nil || len(rows) != 0 {
			t.Fatalf("listAssignments of a deleted topic = %v, %v", rows, err)
		}
		if owned, err := e.localPartitions(name, 3); err != nil || owned != nil {
			t.Fatalf("localPartitions of a deleted topic = %v, %v", owned, err)
		}
		if _, err := e.getTopic(ctx, name); !errors.Is(err, ErrTopicNotFound) {
			t.Fatalf("getTopic of a deleted topic = %v", err)
		}
		if err := reg.Validate(ctx, name, wp4bDriverPayload); !errors.Is(err, schema.ErrSchemaNotFound) {
			t.Fatalf("registry still validates the deleted topic: %v", err)
		}
	}
	if left := wp4bCachedNames(e, "churn-"); len(left) != 0 {
		t.Fatalf("deleted topics left %d cache entries, e.g. %v", len(left), left[:min(len(left), 6)])
	}

	// A same-named topic created afterwards works from scratch.
	if _, err := mgr.CreateTopic(ctx, topics.CreateOpts{Name: "churn-00", Partitions: 3, Schema: []byte(`{"type":"object","required":["must"]}`)}); err != nil {
		t.Fatal(err)
	}
	if err := e.validateProducePayload(ctx, "churn-00", wp4bDriverPayload); err == nil {
		t.Fatal("recreated topic validated against its predecessor's schema")
	}
	if err := e.validateProducePayload(ctx, "churn-00", []byte(`{"must":1}`)); err != nil {
		t.Fatalf("payload valid under the recreated topic's schema rejected: %v", err)
	}
}

// TestWP4BSchemaDroppedUnderLiveTopicIsReloaded: the retired-incarnation
// hook drops the registry's schemas by name, and it also runs when an
// open quarantines a deleted incarnation's directory under a live
// same-named topic. The schema cache still said the live schema was
// loaded, so every later produce found no schema in the registry and
// went through unvalidated until the next schema change.
func TestWP4BSchemaDroppedUnderLiveTopicIsReloaded(t *testing.T) {
	store := wp4bNewStore(t)
	reg := schema.NewJSONSchema()
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSchema(ctx, name, 1, []byte(wp4bDriverSchema)); err != nil {
		t.Fatal(err)
	}
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatal(err)
	}

	// What the retired hook does to the registry.
	if err := reg.DropTopic(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := e.validateProducePayload(ctx, name, []byte(`{"id":1}`)); err == nil {
		t.Fatal("payload invalid under the live schema accepted after the registry dropped it")
	}
	if err := e.validateProducePayload(ctx, name, wp4bDriverPayload); err != nil {
		t.Fatalf("valid payload rejected after the reload: %v", err)
	}
}

// wp4bHookRegistry runs hook once, right after the first ReplaceTopic
// lands.
type wp4bHookRegistry struct {
	*schema.JSONSchema
	once sync.Once
	hook func()
}

func (r *wp4bHookRegistry) ReplaceTopic(ctx context.Context, topicName string, history []schema.Version) error {
	err := r.JSONSchema.ReplaceTopic(ctx, topicName, history)
	r.once.Do(r.hook)
	return err
}

// TestWP4BForgetDuringSchemaLoadReloads: a retire (registry drop, then
// ForgetTopic) that lands between a schema flight's registry write and
// its cache write must not leave the cache saying "loaded" over an
// empty registry.
func TestWP4BForgetDuringSchemaLoadReloads(t *testing.T) {
	store := wp4bNewStore(t)
	reg := &wp4bHookRegistry{JSONSchema: schema.NewJSONSchema()}
	e := wp4bNewEngine(t, store, reg)
	ctx := context.Background()
	const name = "orders"
	reg.hook = func() {
		_ = reg.DropTopic(ctx, name)
		e.ForgetTopic(name)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSchema(ctx, name, 1, []byte(wp4bDriverSchema)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.syncTopicSchemas(ctx, name); err != nil {
		t.Fatal(err)
	}
	// Ask the registry directly: the engine's own reload guard would mask
	// a wrong cache entry.
	if err := reg.Validate(ctx, name, []byte(`{"id":1}`)); err == nil || errors.Is(err, schema.ErrSchemaNotFound) {
		t.Fatalf("registry after the flight = %v, want the schema loaded and rejecting", err)
	}
	e.cacheMu.RLock()
	entry, ok := e.schemaLoadCache[name]
	e.cacheMu.RUnlock()
	if !ok || entry.version != store.SchemaVersion(name) || !entry.value {
		t.Fatalf("schema cache entry = %+v, %v; want loaded at version %d", entry, ok, store.SchemaVersion(name))
	}
}

// TestWP4BForgetDuringLookupDoesNotCache: a metadata load that
// overlapped a ForgetTopic of its key returns its value but does not
// store it.
func TestWP4BForgetDuringLookupDoesNotCache(t *testing.T) {
	var (
		mu    sync.RWMutex
		fence forgetFence
	)
	cache := map[string]cached[int]{}
	load := func() (int, error) {
		mu.Lock()
		fence.forget("t") // a ForgetTopic landing mid-load
		mu.Unlock()
		return 7, nil
	}
	v, err := lookupCached(&mu, cache, &fence, "t", 1, func() uint64 { return 1 }, load, nil)
	if err != nil || v != 7 {
		t.Fatalf("lookupCached = %d, %v; want 7, nil", v, err)
	}
	if _, ok := cache["t"]; ok {
		t.Fatal("a load that overlapped a forget was cached")
	}
	v, err = lookupCached(&mu, cache, &fence, "t", 1, func() uint64 { return 1 }, func() (int, error) { return 8, nil }, nil)
	if err != nil || v != 8 {
		t.Fatalf("lookupCached = %d, %v; want 8, nil", v, err)
	}
	if entry, ok := cache["t"]; !ok || entry.value != 8 {
		t.Fatalf("a clean load was not cached: %+v, %v", entry, ok)
	}
}

// TestWP4BEmptyAssignmentsAreNotCached: an empty assignment set (a
// deleted topic, or one whose partitions are not placed yet) is served
// but not cached; the first placed row is seen at once.
func TestWP4BEmptyAssignmentsAreNotCached(t *testing.T) {
	store := wp4bNewStore(t)
	e := wp4bNewEngine(t, store, schema.NewAlwaysValid())
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := e.getAssignment("orders", 0); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("getAssignment before placement = %v, want not found", err)
		}
	}
	e.cacheMu.RLock()
	_, cachedEmpty := e.assignmentCache["orders"]
	e.cacheMu.RUnlock()
	if cachedEmpty {
		t.Fatal("empty assignment set was cached")
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatal(err)
	}
	a, err := e.getAssignment("orders", 0)
	if err != nil || a.OwnerID != "node-self" {
		t.Fatalf("getAssignment after placement = %+v, %v", a, err)
	}
	e.cacheMu.RLock()
	_, cachedSet := e.assignmentCache["orders"]
	e.cacheMu.RUnlock()
	if !cachedSet {
		t.Fatal("non-empty assignment set was not cached")
	}
}
