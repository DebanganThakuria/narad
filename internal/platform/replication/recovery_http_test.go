package replication_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	brokerruntime "github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	platformreplication "github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpreplication "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/replication"
)

func newHTTPRecoveryStore(t *testing.T, nodeID string) *metastore.Store {
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

func newHTTPRecoveryLogs(t *testing.T, store *metastore.Store) *brokerruntime.Logs {
	t.Helper()
	return brokerruntime.NewLogs(t.TempDir(), storage.Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1,
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  64 << 20,
		Retention:     storage.RetentionConfig{CheckInterval: time.Hour},
	}, store, nil)
}

func newHTTPRecoveryReplicaServer(t *testing.T, logs *brokerruntime.Logs) *httptest.Server {
	t.Helper()
	set := &handlers.Set{Deps: handlers.Deps{
		Logs:        logs,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		ShutdownCtx: context.Background(),
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/v1/replicate", httpreplication.Replicate(set))
	mux.HandleFunc("GET /internal/v1/replicate", httpreplication.ReadReplica(set))
	return httptest.NewServer(mux)
}

func TestStoreRecoveryRepairOwnedPartitionsRecoversOwnerThroughReplicaHTTP(t *testing.T) {
	ctx := context.Background()
	store := newHTTPRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newHTTPRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	followerLogs := newHTTPRecoveryLogs(t, store)
	defer followerLogs.CloseAll()
	follower := newHTTPRecoveryReplicaServer(t, followerLogs)
	defer follower.Close()

	registerHTTPRecoveryTopology(t, ctx, store, follower.URL)

	postHTTPReplicaRecord(t, follower, "orders", 0, 0, []byte(`{"n":1}`))
	postHTTPReplicaRecord(t, follower, "orders", 0, 1, []byte(`{"n":2}`))

	ownerLog, err := ownerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("owner logs.Get: %v", err)
	}
	if _, err := ownerLog.Append([]byte(`{"n":1}`)); err != nil {
		t.Fatalf("append owner: %v", err)
	}
	if err := ownerLog.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("advance owner high watermark: %v", err)
	}
	if err := ownerLogs.CloseTopic("orders"); err != nil {
		t.Fatalf("close owner topic: %v", err)
	}

	recovery := platformreplication.NewStoreRecovery("node-a", store, ownerLogs, follower.Client())
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	ownerLog, err = ownerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("owner logs.Get after recovery: %v", err)
	}
	if got := ownerLog.NextOffset(); got != 2 {
		t.Fatalf("owner NextOffset() = %d, want 2", got)
	}
	payload, err := ownerLog.Read(1)
	if err != nil {
		t.Fatalf("owner Read(1): %v", err)
	}
	if string(payload) != `{"n":2}` {
		t.Fatalf("owner Read(1) = %s, want %s", string(payload), `{"n":2}`)
	}
}

func TestStoreRecoveryRepairOwnedPartitionsRepairsFollowerThroughReplicaHTTP(t *testing.T) {
	ctx := context.Background()
	store := newHTTPRecoveryStore(t, "node-a")
	defer store.Close()
	ownerLogs := newHTTPRecoveryLogs(t, store)
	defer ownerLogs.CloseAll()
	followerLogs := newHTTPRecoveryLogs(t, store)
	defer followerLogs.CloseAll()
	follower := newHTTPRecoveryReplicaServer(t, followerLogs)
	defer follower.Close()

	registerHTTPRecoveryTopology(t, ctx, store, follower.URL)

	ownerLog, err := ownerLogs.Get("orders", 0)
	if err != nil {
		t.Fatalf("owner logs.Get: %v", err)
	}
	for _, payload := range [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)} {
		if _, err := ownerLog.Append(payload); err != nil {
			t.Fatalf("append owner: %v", err)
		}
	}
	if err := ownerLog.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("advance owner high watermark: %v", err)
	}

	postHTTPReplicaRecord(t, follower, "orders", 0, 0, []byte(`{"n":1}`))

	recovery := platformreplication.NewStoreRecovery("node-a", store, ownerLogs, follower.Client())
	if err := recovery.RepairOwnedPartitions(ctx); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	payload := getHTTPReplicaRecord(t, follower, "orders", 0, 1, true)
	if string(payload) != `{"n":2}` {
		t.Fatalf("follower Read(1) = %s, want %s", string(payload), `{"n":2}`)
	}
}

func registerHTTPRecoveryTopology(t *testing.T, ctx context.Context, store *metastore.Store, followerAddr string) {
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

func postHTTPReplicaRecord(t *testing.T, server *httptest.Server, topicName string, partition int, offset int64, payload []byte) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"topic":     topicName,
		"partition": partition,
		"offset":    offset,
		"payload":   payload,
		"leader_id": "node-a",
	})
	if err != nil {
		t.Fatalf("marshal replica record: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, server.URL+"/internal/v1/replicate", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new replicate request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("post replicate record: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("post replicate status = %d, want 204: %s", resp.StatusCode, string(body))
	}
}

func getHTTPReplicaRecord(t *testing.T, server *httptest.Server, topicName string, partition int, offset int64, committed bool) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/internal/v1/replicate", nil)
	if err != nil {
		t.Fatalf("new replica read request: %v", err)
	}
	q := req.URL.Query()
	q.Set("topic", topicName)
	q.Set("partition", fmt.Sprintf("%d", partition))
	q.Set("offset", fmt.Sprintf("%d", offset))
	if committed {
		q.Set("committed", "true")
	}
	req.URL.RawQuery = q.Encode()

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("get replica record: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("get replica status = %d, want 200: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Payload []byte `json:"payload"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode replica read: %v", err)
	}
	return out.Payload
}
