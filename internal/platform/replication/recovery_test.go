package replication

import (
	"context"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

func newRecoveryStore(t *testing.T, nodeID string) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   nodeID,
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return store
		}
		time.Sleep(25 * time.Millisecond)
	}
	store.Close()
	t.Fatal("metastore leader election timed out")
	return nil
}

func newRecoveryLogs(t *testing.T, store *metastore.Store) *runtime.Logs {
	t.Helper()
	return runtime.NewLogs(t.TempDir(), storage.Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1,
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  64 << 20,
		Retention:     storage.RetentionConfig{CheckInterval: time.Hour},
	}, store, nil)
}

func startRecoveryQUICServer(t *testing.T, logs *runtime.Logs) string {
	t.Helper()
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quic tls config: %v", err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsConf, quicConfig())
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		_ = serveQUICListener(ctx, listener, logs, nil)
	}()
	return listener.Addr().String()
}

func setupRecoveryAssignment(t *testing.T, ctx context.Context, store *metastore.Store, followerAddr string) {
	t.Helper()
	now := time.Now().Unix()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-a", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: now}); err != nil {
		t.Fatalf("register owner: %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-b", Addr: followerAddr, Status: metastore.MemberAlive, LastHeartbeat: now}); err != nil {
		t.Fatalf("register follower: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-a", "node-b"); err != nil {
		t.Fatalf("assign partition: %v", err)
	}
}

func appendRecords(t *testing.T, logs *runtime.Logs, records ...[]byte) {
	t.Helper()
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	for _, record := range records {
		if _, err := log.Append(record); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func advanceHWM(t *testing.T, logs *runtime.Logs, hwm int64) {
	t.Helper()
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	if err := log.AdvanceHighWatermark(hwm); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
}

func TestStoreRecoveryRepairOwnedPartitionsRepairsOwnerFromFollowerWithinCommittedBoundary(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	followerLogs := newRecoveryLogs(t, store)
	defer followerLogs.CloseAll()
	followerAddr := startRecoveryQUICServer(t, followerLogs)
	setupRecoveryAssignment(t, ctx, store, followerAddr)

	appendRecords(t, ownerLogs, []byte(`{"n":1}`))
	advanceHWM(t, ownerLogs, 2)
	if err := ownerLogs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	appendRecords(t, followerLogs, []byte(`{"n":1}`), []byte(`{"n":2}`))
	advanceHWM(t, followerLogs, 2)

	recovery := NewStoreRecovery("node-a", store, ownerLogs, nil)
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	log, err := ownerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get after recovery: %v", err)
	}
	if got := log.NextOffset(); got != 2 {
		t.Fatalf("NextOffset() = %d, want 2", got)
	}
	for i, want := range []string{`{"n":1}`, `{"n":2}`} {
		payload, err := log.Read(int64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if string(payload) != want {
			t.Fatalf("Read(%d) = %s, want %s", i, string(payload), want)
		}
	}
}

func TestStoreRecoveryRepairOwnedPartitionsRepairsFollowerFromOwner(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	followerLogs := newRecoveryLogs(t, store)
	defer followerLogs.CloseAll()
	followerAddr := startRecoveryQUICServer(t, followerLogs)
	setupRecoveryAssignment(t, ctx, store, followerAddr)

	appendRecords(t, ownerLogs, []byte(`{"n":1}`), []byte(`{"n":2}`))
	advanceHWM(t, ownerLogs, 2)
	appendRecords(t, followerLogs, []byte(`{"n":1}`))
	advanceHWM(t, followerLogs, 1)

	recovery := NewStoreRecovery("node-a", store, ownerLogs, nil)
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	log, err := followerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("follower logs.Get: %v", err)
	}
	payload, err := log.Read(1)
	if err != nil {
		t.Fatalf("follower Read(1): %v", err)
	}
	if string(payload) != `{"n":2}` {
		t.Fatalf("follower Read(1) = %s, want %s", payload, `{"n":2}`)
	}
}

func TestStoreRecoveryRepairOwnedPartitionsStopsAtCommittedBoundary(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	followerLogs := newRecoveryLogs(t, store)
	defer followerLogs.CloseAll()
	followerAddr := startRecoveryQUICServer(t, followerLogs)
	setupRecoveryAssignment(t, ctx, store, followerAddr)

	advanceHWM(t, ownerLogs, 1)
	if err := ownerLogs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	appendRecords(t, followerLogs, []byte(`{"n":1}`), []byte(`{"n":99}`))
	advanceHWM(t, followerLogs, 1)

	recovery := NewStoreRecovery("node-a", store, ownerLogs, nil)
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	log, err := ownerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get after recovery: %v", err)
	}
	if got := log.NextOffset(); got != 1 {
		t.Fatalf("NextOffset() = %d, want 1", got)
	}
	payload, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read(0): %v", err)
	}
	if string(payload) != `{"n":1}` {
		t.Fatalf("Read(0) = %s, want %s", payload, `{"n":1}`)
	}
}

func TestStoreRecoveryRepairOwnedPartitionsPropagatesReplicaReadErrors(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	setupRecoveryAssignment(t, ctx, store, "127.0.0.1:1")

	recovery := NewStoreRecovery("node-a", store, ownerLogs, nil)
	err := recovery.RepairOwnedPartitions(ctx)
	if err == nil {
		t.Fatal("RepairOwnedPartitions() error = nil, want replica read failure")
	}
}

func TestStoreRecoveryRepairOwnedPartitionsSkipsDeadFollower(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	logs := newRecoveryLogs(t, store)
	defer logs.CloseAll()

	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-a", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register owner: %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-b", Addr: "127.0.0.1:1", Status: metastore.MemberDead, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register follower: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-a", "node-b"); err != nil {
		t.Fatalf("assign partition: %v", err)
	}

	recovery := NewStoreRecovery("node-a", store, logs, nil)
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}
}

func TestStoreRecoveryFetchReplicaRecordOverQUIC(t *testing.T) {
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	logs := newRecoveryLogs(t, store)
	defer logs.CloseAll()
	addr := startRecoveryQUICServer(t, logs)
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	appendRecords(t, logs, []byte(`{"ok":true}`))
	advanceHWM(t, logs, 1)

	recovery := NewStoreRecovery("node-a", store, logs, nil)
	payload, found, err := recovery.fetchReplicaRecord(context.Background(), addr, "orders", 0, 0, true)
	if err != nil {
		t.Fatalf("fetchReplicaRecord(): %v", err)
	}
	if !found {
		t.Fatal("fetchReplicaRecord() found = false, want true")
	}
	if string(payload) != `{"ok":true}` {
		t.Fatalf("payload = %s, want %s", payload, `{"ok":true}`)
	}
}

func TestStoreRecoveryFetchReplicaRecordCommittedOnlyNotFound(t *testing.T) {
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	logs := newRecoveryLogs(t, store)
	defer logs.CloseAll()
	addr := startRecoveryQUICServer(t, logs)
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	appendRecords(t, logs, []byte(`{"ok":true}`))

	recovery := NewStoreRecovery("node-a", store, logs, nil)
	payload, found, err := recovery.fetchReplicaRecord(context.Background(), addr, "orders", 0, 0, true)
	if err != nil {
		t.Fatalf("fetchReplicaRecord(): %v", err)
	}
	if found {
		t.Fatal("fetchReplicaRecord() found = true, want false")
	}
	if payload != nil {
		t.Fatalf("payload = %v, want nil", payload)
	}
}
