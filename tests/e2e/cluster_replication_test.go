package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	platformreplication "github.com/debanganthakuria/narad/internal/platform/replication"
)

func TestProduce_ClusterReplicatorPostsToFollower(t *testing.T) {
	var captured struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Payload   []byte `json:"payload"`
		LeaderID  string `json:"leader_id"`
	}

	requests := make(chan struct{}, 1)
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: got %s want POST", r.Method)
		}
		if r.URL.Path != "/internal/v1/replicate" {
			t.Fatalf("path: got %s want /internal/v1/replicate", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode replicate request: %v", err)
		}
		select {
		case requests <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer follower.Close()

	env := newTestEnv(t, withReplicatorFactory(func(store *metastore.Store, _ *http.Client) replication.Replicator {
		return platformreplication.NewCluster("test-0", store, follower.Client())
	}))
	store, ok := env.ms.(*metastore.Store)
	if !ok {
		t.Fatalf("unexpected metastore type %T", env.ms)
	}
	member, err := store.GetMember("test-1")
	if err != nil {
		t.Fatalf("get member test-1: %v", err)
	}
	member.Addr = follower.URL
	if err := store.RegisterMember(context.Background(), member); err != nil {
		t.Fatalf("register follower addr: %v", err)
	}

	mustCreateTopic(t, env, createTopicReq{Name: "cluster-repl", Partitions: 3, ReplicationFactor: 2})
	result := mustProduce(t, env, "cluster-repl", "stable", map[string]string{"hello": "cluster"})

	select {
	case <-requests:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replication request")
	}

	if captured.Topic != "cluster-repl" {
		t.Fatalf("topic: got %s want cluster-repl", captured.Topic)
	}
	if captured.Partition != result.Partition {
		t.Fatalf("partition: got %d want %d", captured.Partition, result.Partition)
	}
	if captured.Offset != result.Offset {
		t.Fatalf("offset: got %d want %d", captured.Offset, result.Offset)
	}
	if captured.LeaderID != "test-0" {
		t.Fatalf("leader_id: got %s want test-0", captured.LeaderID)
	}
	if string(captured.Payload) != `{"hello":"cluster"}` {
		t.Fatalf("payload: got %s want %s", string(captured.Payload), `{"hello":"cluster"}`)
	}
}
