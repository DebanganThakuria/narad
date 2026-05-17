package replication

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestStoreRecoveryRepairOwnedPartitionsRepairsFromFollower(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	logs := newRecoveryLogs(t, store)
	defer logs.CloseAll()

	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-a", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register owner: %v", err)
	}

	requests := make([]string, 0, 3)
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RawQuery)
		offset := r.URL.Query().Get("offset")
		switch offset {
		case "0":
			_, _ = w.Write([]byte(`{"payload":"eyJuIjoxfQ=="}`))
		case "1":
			_, _ = w.Write([]byte(`{"payload":"eyJuIjoyfQ=="}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer follower.Close()

	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-b", Addr: follower.URL, Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register follower: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-a", "node-b"); err != nil {
		t.Fatalf("assign partition: %v", err)
	}

	recovery := NewStoreRecovery("node-a", store, logs, follower.Client())
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
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
	if len(requests) != 3 {
		t.Fatalf("replica requests = %d, want 3", len(requests))
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

	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	if got := log.NextOffset(); got != 0 {
		t.Fatalf("NextOffset() = %d, want 0", got)
	}
}

func TestStoreRecoveryRepairOwnedPartitionsPropagatesReplicaReadErrors(t *testing.T) {
	ctx := context.Background()
	store := newRecoveryStore(t, "node-a")
	defer store.Close()
	logs := newRecoveryLogs(t, store)
	defer logs.CloseAll()

	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-a", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register owner: %v", err)
	}
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer follower.Close()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-b", Addr: follower.URL, Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("register follower: %v", err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, ReplicationFactor: 2}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-a", "node-b"); err != nil {
		t.Fatalf("assign partition: %v", err)
	}

	recovery := NewStoreRecovery("node-a", store, logs, follower.Client())
	err := recovery.RepairOwnedPartitions(ctx)
	if err == nil {
		t.Fatal("RepairOwnedPartitions() error = nil, want replica read failure")
	}
	want := "replica read failed with status 500: boom"
	if err.Error() != want {
		t.Fatalf("RepairOwnedPartitions() error = %q, want %q", err.Error(), want)
	}
}

func TestStoreRecoveryFetchReplicaRecordRejectsInvalidJSONPayload(t *testing.T) {
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"payload":"not-json"}`))
	}))
	defer follower.Close()

	recovery := NewStoreRecovery("node-a", nil, nil, follower.Client())
	_, found, err := recovery.fetchReplicaRecord(context.Background(), follower.URL, "orders", 0, 0)
	if err == nil {
		t.Fatal("fetchReplicaRecord() error = nil, want invalid payload error")
	}
	if found {
		t.Fatal("fetchReplicaRecord() found = true, want false on invalid payload")
	}
	if err.Error() != "decode replica read response: illegal base64 data at input byte 3" {
		t.Fatalf("fetchReplicaRecord() error = %q, want %q", err.Error(), "decode replica read response: illegal base64 data at input byte 3")
	}
}

func TestStoreRecoveryFetchReplicaRecordBuildsExpectedQuery(t *testing.T) {
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/replicate" {
			t.Fatalf("path = %s, want /internal/v1/replicate", r.URL.Path)
		}
		if got := r.URL.Query().Get("topic"); got != "orders" {
			t.Fatalf("topic query = %s, want orders", got)
		}
		if got := r.URL.Query().Get("partition"); got != "3" {
			t.Fatalf("partition query = %s, want 3", got)
		}
		if got := r.URL.Query().Get("offset"); got != "9" {
			t.Fatalf("offset query = %s, want 9", got)
		}
		_, _ = w.Write([]byte(`{"payload":"eyJvayI6dHJ1ZX0="}`))
	}))
	defer follower.Close()

	recovery := NewStoreRecovery("node-a", nil, nil, follower.Client())
	payload, found, err := recovery.fetchReplicaRecord(context.Background(), follower.URL, "orders", 3, 9)
	if err != nil {
		t.Fatalf("fetchReplicaRecord(): %v", err)
	}
	if !found {
		t.Fatal("fetchReplicaRecord() found = false, want true")
	}
	if string(payload) != `{"ok":true}` {
		t.Fatalf("payload = %s, want %s", string(payload), `{"ok":true}`)
	}
}

func TestStoreRecoveryFetchReplicaRecordNotFound(t *testing.T) {
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer follower.Close()

	recovery := NewStoreRecovery("node-a", nil, nil, follower.Client())
	payload, found, err := recovery.fetchReplicaRecord(context.Background(), follower.URL, "orders", 0, 0)
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

func TestStoreRecoveryFetchReplicaRecordWrapsRequestErrors(t *testing.T) {
	recovery := NewStoreRecovery("node-a", nil, nil, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial failed")
	})})

	_, found, err := recovery.fetchReplicaRecord(context.Background(), "127.0.0.1:1234", "orders", 0, 0)
	if err == nil {
		t.Fatal("fetchReplicaRecord() error = nil, want request error")
	}
	if found {
		t.Fatal("fetchReplicaRecord() found = true, want false on request error")
	}
	if err.Error() != "send replica read request: Get \"http://127.0.0.1:1234/internal/v1/replicate?offset=0&partition=0&topic=orders\": dial failed" {
		t.Fatalf("fetchReplicaRecord() error = %q", err.Error())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
