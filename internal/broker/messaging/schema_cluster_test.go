package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// clusterNode is one broker node of the test cluster: a Raft voter plus
// a messaging engine with a real JSON Schema registry over it, the way
// serve wires them.
type clusterNode struct {
	id     string
	addr   string
	store  *metastore.Store
	engine *Engine
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// newThreeNodeCluster boots three voters on free ports, each with its
// own engine, and registers them as alive members.
func newThreeNodeCluster(t *testing.T) []*clusterNode {
	t.Helper()
	base := t.TempDir()
	ids := []string{"sc-1", "sc-2", "sc-3"}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	nodes := make([]*clusterNode, 0, 3)
	for i := range ids {
		var peers []metastore.Peer
		for j := range ids {
			if i != j {
				peers = append(peers, metastore.Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		store, err := metastore.New(metastore.Config{
			NodeID:        ids[i],
			DataDir:       filepath.Join(base, ids[i], "meta"),
			BindAddr:      addrs[i],
			AdvertiseAddr: addrs[i],
			Peers:         peers,
		})
		if err != nil {
			t.Fatalf("metastore.New(%s): %v", ids[i], err)
		}
		logs := runtime.NewLogs(filepath.Join(base, ids[i], "data"), storage.Options{FlushInterval: time.Millisecond}, store, nil)
		offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
		}, nil)
		engine := NewEngine(store, schema.NewJSONSchema(), fixedPartitionManager{picked: i}, offsets, logs, nil, nil,
			slog.New(slog.NewTextHandler(io.Discard, nil)), ids[i])
		nodes = append(nodes, &clusterNode{id: ids[i], addr: addrs[i], store: store, engine: engine})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.engine.Close()
			_ = n.store.Close()
		}
	})
	leader := waitForLeader(t, nodes)
	ctx := context.Background()
	for i, n := range nodes {
		if err := leader.store.RegisterMember(ctx, metastore.Member{ID: n.id, Addr: addrs[i], Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", n.id, err)
		}
	}
	return nodes
}

func waitForLeader(t *testing.T, nodes []*clusterNode) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.store.IsLeader() {
				return n
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

// waitForSchemaVersion blocks until every node's replica holds exactly
// `versions` schema versions for the topic: the moment the produce
// path on that node enforces the latest one.
func waitForSchemaVersion(t *testing.T, nodes []*clusterNode, topicName string, versions int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for _, n := range nodes {
		for {
			history, err := schema.PersistedHistory(ctx, n.store, topicName)
			if err == nil && len(history) == versions {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: schema history has %d versions (err %v), want %d", n.id, len(history), err, versions)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// produceOn produces to the partition the node owns and reports whether
// the schema rejected the payload.
func produceOn(t *testing.T, n *clusterNode, partition int, payload string) error {
	t.Helper()
	_, _, err := n.engine.Produce(context.Background(), "orders", "k", []byte(payload), partition)
	return err
}

// TestSchemaEnforcedOnEveryNode: a schema registered through the leader
// is enforced by the produce path of every node, including followers
// whose registry had already loaded an older version, and a widening
// registered later is picked up by every node without a restart.
func TestSchemaEnforcedOnEveryNode(t *testing.T) {
	nodes := newThreeNodeCluster(t)
	leader := waitForLeader(t, nodes)
	ctx := context.Background()
	if err := leader.store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for i, n := range nodes {
		if err := leader.store.AssignPartition(ctx, "orders", i, n.id); err != nil {
			t.Fatalf("AssignPartition(%d): %v", i, err)
		}
	}
	waitForSchemaVersion(t, nodes, "orders", 0)
	// A follower serves ownership only once its replica has caught up
	// with the leader since start and applied the assignment; under CI
	// load that lags the AssignPartition futures by a few hundred ms.
	for i, n := range nodes {
		deadline := time.Now().Add(15 * time.Second)
		for {
			a, err := n.store.GetAssignment("orders", i)
			if err == nil && a.OwnerID == n.id {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never saw itself as owner of partition %d: %+v, %v", n.id, i, a, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Before any schema: every node takes anything, and their registries
	// now hold "no schema" for the topic.
	for i, n := range nodes {
		if err := produceOn(t, n, i, `not even json`); err != nil {
			t.Fatalf("%s before schema: %v", n.id, err)
		}
	}

	v1 := []byte(`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`)
	if err := leader.store.PutSchema(ctx, "orders", 1, v1); err != nil {
		t.Fatalf("PutSchema v1: %v", err)
	}
	waitForSchemaVersion(t, nodes, "orders", 1)
	for i, n := range nodes {
		err := produceOn(t, n, i, `{"id":"not-an-integer"}`)
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "schema") {
			t.Fatalf("%s after v1: invalid payload error = %v, want a schema rejection", n.id, err)
		}
		if err := produceOn(t, n, i, `{"id":1}`); err != nil {
			t.Fatalf("%s after v1: valid payload rejected: %v", n.id, err)
		}
	}

	// Widen on the leader; every node (each of which has v1 loaded and
	// cached) must validate against v2 on its next produce.
	v2 := []byte(`{"type":"object","properties":{"id":{"type":["integer","string"]}},"required":["id"]}`)
	if err := leader.store.PutSchema(ctx, "orders", 2, v2); err != nil {
		t.Fatalf("PutSchema v2: %v", err)
	}
	waitForSchemaVersion(t, nodes, "orders", 2)
	for i, n := range nodes {
		if err := produceOn(t, n, i, `{"id":"now-a-string"}`); err != nil {
			t.Fatalf("%s after v2: %v", n.id, err)
		}
		if err := produceOn(t, n, i, `{"id":true}`); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s after v2: boolean id error = %v, want rejection", n.id, err)
		}
	}

	// A stale proposer (a node that believes the latest is v1) cannot
	// overwrite v2 on any replica.
	err := leader.store.PutSchema(ctx, "orders", 2, v1)
	if !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("overwrite of v2 error = %v, want %v", err, errs.ErrAlreadyExists)
	}
	for _, n := range nodes {
		raw, err := n.store.GetSchema(ctx, "orders", 2)
		if err != nil || string(raw) != string(v2) {
			t.Fatalf("%s: v2 = %s (%v) after refused overwrite", n.id, raw, err)
		}
	}
}

// TestSchemaSurvivesLeaderKilledMidUpdate: the leader is torn down
// while schema updates are in flight. The survivors must agree on the
// history (a version is either on both or on neither), the new leader
// must accept exactly latest+1, and each survivor's produce path must
// enforce the agreed latest.
func TestSchemaSurvivesLeaderKilledMidUpdate(t *testing.T) {
	nodes := newThreeNodeCluster(t)
	leader := waitForLeader(t, nodes)
	ctx := context.Background()
	if err := leader.store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for i, n := range nodes {
		if err := leader.store.AssignPartition(ctx, "orders", i, n.id); err != nil {
			t.Fatalf("AssignPartition(%d): %v", i, err)
		}
	}
	if err := leader.store.PutSchema(ctx, "orders", 1, []byte(`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`)); err != nil {
		t.Fatalf("PutSchema v1: %v", err)
	}
	waitForSchemaVersion(t, nodes, "orders", 1)

	// Fire a burst of updates and kill the leader in the middle of it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 2; v <= 40; v++ {
			raw := []byte(fmt.Sprintf(`{"type":"object","properties":{"id":{"type":"integer"},"f%d":{"type":"string"}},"required":["id"]}`, v))
			if err := leader.store.PutSchema(ctx, "orders", v, raw); err != nil {
				return
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	_ = leader.engine.Close()
	_ = leader.store.Close()
	wg.Wait()

	var survivors []*clusterNode
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeader(t, survivors)

	// A write through the new leader lands only after everything the
	// old leader committed has been applied on it; that is the barrier
	// behind which the agreed history is read. The other survivor then
	// has to catch up to the same length.
	barrier := func() {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			err := newLeader.store.RegisterMember(ctx, metastore.Member{ID: newLeader.id, Addr: newLeader.addr, Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()})
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("barrier write through the new leader: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	barrier()
	leaderHistory, err := schema.PersistedHistory(ctx, newLeader.store, "orders")
	if err != nil {
		t.Fatalf("new leader history: %v", err)
	}
	waitForSchemaVersion(t, survivors, "orders", len(leaderHistory))
	var histories [2][]schema.Version
	for i, n := range survivors {
		h, err := schema.PersistedHistory(ctx, n.store, "orders")
		if err != nil {
			t.Fatalf("%s: history: %v", n.id, err)
		}
		histories[i] = h
	}
	for i := range histories[0] {
		if histories[0][i].Number != i+1 || histories[1][i].Number != i+1 || string(histories[0][i].Raw) != string(histories[1][i].Raw) {
			t.Fatalf("survivors disagree at v%d: %s vs %s", i+1, histories[0][i].Raw, histories[1][i].Raw)
		}
	}
	latest := len(histories[0])

	// The new leader continues the history from exactly the agreed latest.
	if err := newLeader.store.PutSchema(ctx, "orders", latest, []byte(`{"type":"object"}`)); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Fatalf("overwrite of v%d on the new leader error = %v, want %v", latest, err, errs.ErrAlreadyExists)
	}
	if err := newLeader.store.PutSchema(ctx, "orders", latest+2, []byte(`{"type":"object"}`)); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("gap on the new leader error = %v, want %v", err, errs.ErrInvalidArgument)
	}
	final := []byte(fmt.Sprintf(`{"type":"object","properties":{"id":{"type":["integer","string"]},"f%d":{"type":"string"}},"required":["id"]}`, latest+1))
	if err := newLeader.store.PutSchema(ctx, "orders", latest+1, final); err != nil {
		t.Fatalf("PutSchema v%d on the new leader: %v", latest+1, err)
	}
	waitForSchemaVersion(t, survivors, "orders", latest+1)

	// Each survivor enforces the final schema on the partition it owns.
	for i, n := range nodes {
		if n == leader {
			continue
		}
		if err := produceOn(t, n, i, `{"id":"string-ok-now"}`); err != nil {
			t.Fatalf("%s: valid payload under the final schema rejected: %v", n.id, err)
		}
		if err := produceOn(t, n, i, `{"no":"id"}`); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: invalid payload error = %v, want rejection", n.id, err)
		}
	}
}
