package e2e

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	platformreplication "github.com/debanganthakuria/narad/internal/platform/replication"
)

func TestRecovery_RepairsOwnerFromFollowerViaInternalHTTP(t *testing.T) {
	owner := newTestEnv(t)
	defer owner.close()
	follower := newTestEnv(t)
	defer follower.close()
	store := recoveryStore(t, owner)
	registerRecoveryFollower(t, store, follower.Server.URL)
	mustCreateTopic(t, owner, createTopicReq{Name: "recover-owner", Partitions: 3, ReplicationFactor: 2})
	assignRecoveryPartition(t, store, "recover-owner")

	postReplicaRecord(t, follower, "recover-owner", 0, 0, []byte(`{"n":1}`))
	postReplicaRecord(t, follower, "recover-owner", 0, 1, []byte(`{"n":2}`))

	ownerLog, err := owner.logs.Get("recover-owner", 0)
	if err != nil {
		t.Fatalf("owner logs.Get: %v", err)
	}
	if _, err := ownerLog.Append([]byte(`{"n":1}`)); err != nil {
		t.Fatalf("append owner: %v", err)
	}
	if err := ownerLog.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("advance owner high watermark: %v", err)
	}
	if err := owner.logs.CloseTopic("recover-owner"); err != nil {
		t.Fatalf("close owner topic: %v", err)
	}

	recovery := platformreplication.NewStoreRecovery("test-0", store, owner.logs, follower.client)
	if err := recovery.RepairOwnedPartitions(context.Background()); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	ownerLog, err = owner.logs.Get("recover-owner", 0)
	if err != nil {
		t.Fatalf("owner logs.Get after recovery: %v", err)
	}
	payload, err := ownerLog.Read(1)
	if err != nil {
		t.Fatalf("owner Read(1): %v", err)
	}
	if string(payload) != `{"n":2}` {
		t.Fatalf("owner Read(1) = %s, want %s", string(payload), `{"n":2}`)
	}
	if got := ownerLog.HighWatermark(); got != 2 {
		t.Fatalf("owner HighWatermark() = %d, want 2", got)
	}
}

func TestRecovery_RepairsFollowerFromOwnerViaInternalHTTP(t *testing.T) {
	owner := newTestEnv(t)
	defer owner.close()
	follower := newTestEnv(t)
	defer follower.close()
	store := recoveryStore(t, owner)
	registerRecoveryFollower(t, store, follower.Server.URL)
	mustCreateTopic(t, owner, createTopicReq{Name: "recover-follower", Partitions: 3, ReplicationFactor: 2})
	assignRecoveryPartition(t, store, "recover-follower")

	ownerLog, err := owner.logs.Get("recover-follower", 0)
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
	postReplicaRecord(t, follower, "recover-follower", 0, 0, []byte(`{"n":1}`))

	recovery := platformreplication.NewStoreRecovery("test-0", store, owner.logs, follower.client)
	if err := recovery.RepairOwnedPartitions(context.Background()); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	payload := getCommittedReplicaRecord(t, follower, "recover-follower", 0, 1)
	if string(payload) != `{"n":2}` {
		t.Fatalf("follower committed payload = %s, want %s", string(payload), `{"n":2}`)
	}
}

func recoveryStore(t *testing.T, e *env) *metastore.Store {
	t.Helper()
	store, ok := e.ms.(*metastore.Store)
	if !ok {
		t.Fatalf("unexpected metastore type %T", e.ms)
	}
	return store
}

func registerRecoveryFollower(t *testing.T, store *metastore.Store, followerURL string) {
	t.Helper()
	member, err := store.GetMember("test-1")
	if err != nil {
		t.Fatalf("get member test-1: %v", err)
	}
	member.Addr = followerURL
	member.Status = metastore.MemberAlive
	member.LastHeartbeat = time.Now().Unix()
	if err := store.RegisterMember(context.Background(), member); err != nil {
		t.Fatalf("register follower addr: %v", err)
	}
}

func assignRecoveryPartition(t *testing.T, store *metastore.Store, topicName string) {
	t.Helper()
	if err := store.AssignPartition(context.Background(), topicName, 0, "test-0", "test-1"); err != nil {
		t.Fatalf("assign recovery partition: %v", err)
	}
}

func postReplicaRecord(t *testing.T, e *env, topicName string, partition int, offset int64, payload []byte) {
	t.Helper()
	resp := e.post("/internal/v1/replicate", map[string]any{
		"topic":     topicName,
		"partition": partition,
		"offset":    offset,
		"payload":   payload,
		"leader_id": "test-0",
	})
	expectStatus(t, resp, http.StatusNoContent)
}

func getCommittedReplicaRecord(t *testing.T, e *env, topicName string, partition int, offset int64) []byte {
	t.Helper()
	path := "/internal/v1/replicate?topic=" + url.QueryEscape(topicName) +
		"&partition=" + strconv.Itoa(partition) +
		"&offset=" + strconv.FormatInt(offset, 10) +
		"&committed=true"
	resp := e.get(path)
	expectStatus(t, resp, http.StatusOK)
	var out struct {
		Payload []byte `json:"payload"`
	}
	decodeJSON(t, resp, &out)
	return out.Payload
}
