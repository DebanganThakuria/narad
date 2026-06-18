package messaging

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestProduceAppendsAndReplicates(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	schemas := &fakeSchemas{}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 1}, replicator)

	offset, partitionIdx, err := engine.Produce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("Produce() offset = %d, want 0", offset)
	}
	if partitionIdx != 1 {
		t.Fatalf("Produce() partition = %d, want 1", partitionIdx)
	}
	if schemas.lastTopic != "orders" || string(schemas.lastPayload) != `{"id":1}` {
		t.Fatalf("schema Validate() args = topic=%q payload=%q", schemas.lastTopic, string(schemas.lastPayload))
	}
	if !replicator.called {
		t.Fatal("Replicate() was not called")
	}
	if replicator.lastTopic != "orders" || replicator.lastPartition != 1 || replicator.lastOffset != 0 {
		t.Fatalf("Replicate() args = topic=%q partition=%d offset=%d", replicator.lastTopic, replicator.lastPartition, replicator.lastOffset)
	}
	if string(replicator.lastPayload) != `{"id":1}` {
		t.Fatalf("Replicate() payload = %q, want %q", string(replicator.lastPayload), `{"id":1}`)
	}
	log, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := log.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
}

func TestProduceAllowsMissingSchema(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	schemas := &fakeSchemas{validateErr: errs.ErrSchemaNotFound}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 0}, replicator)

	offset, partitionIdx, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 || partitionIdx != 0 {
		t.Fatalf("Produce() = offset %d partition %d, want 0,0", offset, partitionIdx)
	}
	if !replicator.called {
		t.Fatal("Replicate() was not called for schema-not-found path")
	}
}

func TestProduceLoadsPersistedSchemaWhenLocalRegistryMisses(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	if err := ms.PutSchema(context.Background(), "orders", 1, []byte(`{"type":"object"}`)); err != nil {
		t.Fatalf("PutSchema() error = %v", err)
	}
	schemas := &fakeSchemas{validateErr: errs.ErrSchemaNotFound}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 0}, replicator)

	if _, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if len(schemas.loads) != 1 {
		t.Fatalf("schema loads = %d, want 1", len(schemas.loads))
	}
	if schemas.loads[0].topic != "orders" || schemas.loads[0].version != 1 {
		t.Fatalf("schema load = %+v, want orders v1", schemas.loads[0])
	}
	if !replicator.called {
		t.Fatal("Replicate() was not called after schema load")
	}
}

func TestProduceRejectsInvalidPayloadAfterLazySchemaLoad(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	rawSchema := []byte(`{
		"type":"object",
		"properties":{"id":{"type":"string"}},
		"required":["id"]
	}`)
	if err := ms.PutSchema(context.Background(), "orders", 1, rawSchema); err != nil {
		t.Fatalf("PutSchema() error = %v", err)
	}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schema.NewJSONSchema(), fixedPartitioner{picked: 0}, replicator)

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("Produce() error = %v, want schema validation error", err)
	}
	if replicator.called {
		t.Fatal("Replicate() was called after lazy schema validation failure")
	}
}

func TestProduceRejectsSchemaValidationError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	schemas := &fakeSchemas{validateErr: errors.New("invalid payload")}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 0}, replicator)

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "invalid payload") {
		t.Fatalf("Produce() error = %v, want invalid payload", err)
	}
	if replicator.called {
		t.Fatal("Replicate() was called after schema validation failure")
	}
}

func TestProduceReturnsReplicatorError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &fakeReplicator{err: errors.New("replication failed")}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || err.Error() != "messaging: replicate: replication failed" {
		t.Fatalf("Produce() error = %v, want wrapped replicate error", err)
	}
	log, getErr := engine.logs.Get("orders", 0)
	if getErr != nil {
		t.Fatalf("Get() error = %v", getErr)
	}
	if got := log.HighWatermark(); got != 0 {
		t.Fatalf("HighWatermark() = %d, want 0", got)
	}
}

func TestProduceSkipsDeadOwnerPartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-self", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(2) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	offset, partitionIdx, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
	if partitionIdx != 1 {
		t.Fatalf("partition = %d, want 1", partitionIdx)
	}
}

func TestProducePinnedPartitionUsesRequestedLocalPartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-self", ""); err != nil {
		t.Fatalf("AssignPartition(2) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	offset, partitionIdx, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`), 2)
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 || partitionIdx != 2 {
		t.Fatalf("Produce() = offset %d partition %d, want offset 0 partition 2", offset, partitionIdx)
	}
}

func TestProducePinnedPartitionRejectsOutOfRange(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`), 2)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrInvalid)
	}
}

func TestProducePinnedPartitionRejectsMissingAssignment(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`), 1)
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestProducePinnedPartitionRejectsRemoteOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`), 1)
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestProducePinnedPartitionRejectsDeadFollower(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", "node-dead"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`), 0)
	if err == nil || !strings.Contains(err.Error(), "no alive partition owner") {
		t.Fatalf("Produce() error = %v, want no alive partition owner", err)
	}
}

func TestProduceRejectsDeadFollowerPartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self", "node-dead"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-self", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(2) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 1})
	offset, partitionIdx, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
	if partitionIdx != 2 {
		t.Fatalf("partition = %d, want 2", partitionIdx)
	}
}

func TestProduceWithLocalReplicatorAdvancesHighWatermark(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-self", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(2) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	if _, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	committedLog, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := committedLog.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
}

func TestProduceFailsWhenNoPartitionHasAliveOwnerAndFollower(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-alive", Addr: "alive.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(alive) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-alive", "node-dead"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-dead", "node-alive"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`))
	if err == nil || !strings.Contains(err.Error(), "no alive partition owner") {
		t.Fatalf("Produce() error = %v, want no alive partition owner", err)
	}
}

func TestProduceFailsWhenHighWatermarkPersistFails(t *testing.T) {
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngineWithDir(t, dataDir, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, &fakeReplicator{})
	hwmPath := partitionHWMPath(dataDir, "orders", 0)
	if err := os.Remove(hwmPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Remove(hwm): %v", err)
	}
	if err := os.MkdirAll(hwmPath, 0o755); err != nil {
		t.Fatalf("Mkdir(hwm path): %v", err)
	}

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || !(strings.Contains(err.Error(), "commit boundary durability") || strings.Contains(err.Error(), "read hwm")) {
		t.Fatalf("Produce() error = %v, want commit boundary durability failure", err)
	}
}
