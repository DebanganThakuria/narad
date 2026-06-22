package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	platformreplication "github.com/debanganthakuria/narad/internal/platform/replication"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

func TestRecovery_RepairsOwnerFromFollowerViaQUIC(t *testing.T) {
	owner := newTestEnv(t)
	defer owner.close()
	follower := newTestEnv(t)
	defer follower.close()
	startRecoveryQUIC(t, follower)
	store := recoveryStore(t, owner)
	registerRecoveryFollower(t, store, quicAddrForEnv(follower))
	mustCreateTopic(t, owner, createTopicReq{Name: "recover-owner", Partitions: 3, ReplicationFactor: 2})
	assignRecoveryPartition(t, store, "recover-owner")

	appendReplicaRecord(t, follower, "recover-owner", 0, []byte(`{"n":1}`), []byte(`{"n":2}`))

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

func TestRecovery_RepairsFollowerFromOwnerViaQUIC(t *testing.T) {
	owner := newTestEnv(t)
	defer owner.close()
	follower := newTestEnv(t)
	defer follower.close()
	startRecoveryQUIC(t, follower)
	store := recoveryStore(t, owner)
	registerRecoveryFollower(t, store, quicAddrForEnv(follower))
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
	if err := owner.logs.CloseTopic("recover-follower"); err != nil {
		t.Fatalf("close owner topic: %v", err)
	}
	appendReplicaRecord(t, follower, "recover-follower", 0, []byte(`{"n":1}`))

	recovery := platformreplication.NewStoreRecovery("test-0", store, owner.logs, follower.client)
	if err := recovery.RepairOwnedPartitions(context.Background()); err != nil {
		t.Fatalf("RepairOwnedPartitions: %v", err)
	}

	payload := readReplicaRecord(t, follower, "recover-follower", 0, 1)
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

func registerRecoveryFollower(t *testing.T, store *metastore.Store, followerAddr string) {
	t.Helper()
	member, err := store.GetMember("test-1")
	if err != nil {
		t.Fatalf("get member test-1: %v", err)
	}
	member.Addr = followerAddr
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

func startRecoveryQUIC(t *testing.T, e *env) {
	t.Helper()
	addr := quicAddrForEnv(e)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	t.Cleanup(cancel)
	go func() {
		if err := platformreplication.ServeQUIC(ctx, addr, e.logs, nil); err != nil {
			errCh <- err
		}
	}()
	waitForRecoveryQUIC(t, addr, errCh)
}

func waitForRecoveryQUIC(t *testing.T, addr string, errCh <-chan error) {
	t.Helper()
	client := platformreplication.NewQUICFrameClient(250 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("start recovery QUIC: %v", err)
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		frame, err := client.Request(ctx, addr, replicationwire.StreamFramePing, nil)
		cancel()
		if err == nil && frame.Type == replicationwire.StreamFramePong {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("recovery QUIC %s did not become ready", addr)
}

func quicAddrForEnv(e *env) string {
	return strings.TrimPrefix(e.Server.URL, "http://")
}

func appendReplicaRecord(t *testing.T, e *env, topicName string, partition int, payloads ...[]byte) {
	t.Helper()
	log, err := e.logs.Get(topicName, partition)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	for _, payload := range payloads {
		if _, err := log.Append(payload); err != nil {
			t.Fatalf("append replica: %v", err)
		}
	}
	if err := log.AdvanceHighWatermark(log.NextOffset()); err != nil {
		t.Fatalf("advance replica hwm: %v", err)
	}
}

func readReplicaRecord(t *testing.T, e *env, topicName string, partition int, offset int64) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		log, err := e.logs.Get(topicName, partition)
		if err != nil {
			t.Fatalf("logs.Get: %v", err)
		}
		payload, err := log.Read(offset)
		if err == nil {
			return payload
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replica offset %d", offset)
	return nil
}
