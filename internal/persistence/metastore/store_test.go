package metastore_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func newTestStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{
		NodeID:        "test-0",
		DataDir:       t.TempDir(),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	// Wait for leader election.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			s.DeleteTopic(context.Background(), "__probe__")
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

func TestTopicCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	ord := topic.Topic{Name: "orders", Partitions: 4, RetentionMs: 3600_000}

	if err := s.CreateTopic(ctx, ord); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Duplicate create must fail.
	if err := s.CreateTopic(ctx, ord); err != metastore.ErrAlreadyExists {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.Partitions != 4 || got.RetentionMs != 3600_000 {
		t.Fatalf("unexpected topic: %+v", got)
	}

	ord.RetentionMs = 86400_000
	if err := s.UpdateTopic(ctx, ord); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	got, _ = s.GetTopic(ctx, "orders")
	if got.RetentionMs != 86400_000 {
		t.Fatalf("update not reflected: %+v", got)
	}

	if err := s.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := s.GetTopic(ctx, "orders"); err != metastore.ErrNotFound {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
}

func TestListTopicsPaginated(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for _, name := range []string{"aaa", "bbb", "ccc", "ddd", "eee"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 1}); err != nil {
			t.Fatalf("CreateTopic %s: %v", name, err)
		}
	}

	// Page 1.
	page1, tok, err := s.ListTopics(ctx, metastore.ListOptions{Limit: 2})
	if err != nil || len(page1) != 2 || tok == "" {
		t.Fatalf("page1: topics=%v tok=%q err=%v", page1, tok, err)
	}

	// Page 2.
	page2, tok2, err := s.ListTopics(ctx, metastore.ListOptions{Limit: 2, PageToken: tok})
	if err != nil || len(page2) != 2 || tok2 == "" {
		t.Fatalf("page2: topics=%v tok=%q err=%v", page2, tok2, err)
	}

	// Page 3 (last).
	page3, tok3, err := s.ListTopics(ctx, metastore.ListOptions{Limit: 2, PageToken: tok2})
	if err != nil || len(page3) != 1 || tok3 != "" {
		t.Fatalf("page3: topics=%v tok=%q err=%v", page3, tok3, err)
	}
}

func TestSchemas(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	schema := []byte(`{"type":"object"}`)
	if err := s.PutSchema(ctx, "orders", 1, schema); err != nil {
		t.Fatalf("PutSchema: %v", err)
	}

	got, err := s.GetSchema(ctx, "orders", 1)
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if string(got) != string(schema) {
		t.Fatalf("schema mismatch: %s", got)
	}

	if _, err := s.GetSchema(ctx, "orders", 2); err != metastore.ErrNotFound {
		t.Fatalf("want ErrNotFound for missing version, got %v", err)
	}
}

func TestDeleteTopicCleansSchemas(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1})
	s.PutSchema(ctx, "orders", 1, []byte(`{"type":"object"}`))
	s.DeleteTopic(ctx, "orders")

	if _, err := s.GetSchema(ctx, "orders", 1); err != metastore.ErrNotFound {
		t.Fatalf("schema should be deleted with topic, got %v", err)
	}
}

func TestMembers(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	m := metastore.Member{ID: "narad-0", Addr: "10.0.0.1:7943", Status: metastore.MemberAlive, LastHeartbeat: 1000}
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}

	got, err := s.GetMember("narad-0")
	if err != nil {
		t.Fatalf("GetMember: %v", err)
	}
	if got.Addr != "10.0.0.1:7943" || got.Status != metastore.MemberAlive {
		t.Fatalf("unexpected member: %+v", got)
	}

	if err := s.Heartbeat(ctx, "narad-0", 2000); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	got, _ = s.GetMember("narad-0")
	if got.LastHeartbeat != 2000 {
		t.Fatalf("heartbeat not updated: %+v", got)
	}

	if err := s.MarkMemberDead(ctx, "narad-0"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	got, _ = s.GetMember("narad-0")
	if got.Status != metastore.MemberDead {
		t.Fatalf("member should be dead: %+v", got)
	}

	members, err := s.ListMembers()
	if err != nil || len(members) != 1 {
		t.Fatalf("ListMembers: got %v, err %v", members, err)
	}
}

func TestAssignments(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for _, p := range []int{0, 1, 2, 3} {
		if err := s.AssignPartition(ctx, "orders", p, "narad-0", "narad-1"); err != nil {
			t.Fatalf("AssignPartition %d: %v", p, err)
		}
	}
	s.AssignPartition(ctx, "payments", 0, "narad-1", "narad-2")

	a, err := s.GetAssignment("orders", 2)
	if err != nil || a.OwnerID != "narad-0" {
		t.Fatalf("GetAssignment: %+v, err %v", a, err)
	}

	list, err := s.ListAssignments("orders")
	if err != nil || len(list) != 4 {
		t.Fatalf("ListAssignments: got %v, err %v", list, err)
	}

	// Deleting the topic must also remove its assignments.
	s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 4})
	s.DeleteTopic(ctx, "orders")
	list, _ = s.ListAssignments("orders")
	if len(list) != 0 {
		t.Fatalf("assignments should be gone after DeleteTopic, got %v", list)
	}

	// payments assignment must be untouched.
	if _, err := s.GetAssignment("payments", 0); err != nil {
		t.Fatalf("unrelated assignment deleted: %v", err)
	}
}

func TestAssignNewPartitionsBalancesOwnersAndFollowers(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for _, id := range []string{"narad-1", "narad-2"} {
		if err := s.RegisterMember(ctx, metastore.Member{
			ID:            id,
			Addr:          id + ":7943",
			Status:        metastore.MemberAlive,
			LastHeartbeat: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("RegisterMember %s: %v", id, err)
		}
	}

	if err := s.AssignNewPartitions(ctx, "orders", 0, 6, 2); err != nil {
		t.Fatalf("AssignNewPartitions: %v", err)
	}
	assignments, err := s.ListAssignments("orders")
	if err != nil {
		t.Fatalf("ListAssignments: %v", err)
	}
	if len(assignments) != 6 {
		t.Fatalf("assignments = %d, want 6", len(assignments))
	}

	owners := map[string]int{}
	followers := map[string]int{}
	for _, assignment := range assignments {
		if assignment.OwnerID == assignment.FollowerID {
			t.Fatalf("partition %d assigned owner and follower to %q", assignment.Partition, assignment.OwnerID)
		}
		wantOwner := "narad-1"
		wantFollower := "narad-2"
		if assignment.Partition%2 == 1 {
			wantOwner = "narad-2"
			wantFollower = "narad-1"
		}
		if assignment.OwnerID != wantOwner || assignment.FollowerID != wantFollower {
			t.Fatalf("partition %d = owner %s follower %s, want owner %s follower %s",
				assignment.Partition, assignment.OwnerID, assignment.FollowerID, wantOwner, wantFollower)
		}
		owners[assignment.OwnerID]++
		followers[assignment.FollowerID]++
	}
	for _, id := range []string{"narad-1", "narad-2"} {
		if owners[id] != 3 {
			t.Fatalf("%s owns %d partitions, want 3; all owners=%v", id, owners[id], owners)
		}
		if followers[id] != 3 {
			t.Fatalf("%s follows %d partitions, want 3; all followers=%v", id, followers[id], followers)
		}
	}
}

func TestBootstrapThreeNodeCluster(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	addrs := []string{"127.0.0.1:19101", "127.0.0.1:19102", "127.0.0.1:19103"}
	ids := []string{"node-1", "node-2", "node-3"}
	stores := make([]*metastore.Store, 0, 3)

	for i := range ids {
		peers := make([]metastore.Peer, 0, len(ids)-1)
		for j := range ids {
			if i == j {
				continue
			}
			peers = append(peers, metastore.Peer{ID: ids[j], Addr: addrs[j]})
		}
		store, err := metastore.New(metastore.Config{
			NodeID:        ids[i],
			DataDir:       filepath.Join(baseDir, fmt.Sprintf("metastore-%s", ids[i])),
			BindAddr:      addrs[i],
			AdvertiseAddr: addrs[i],
			Peers:         peers,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores = append(stores, store)
	}
	for i := range stores {
		store := stores[i]
		t.Cleanup(func() { _ = store.Close() })
	}

	leader := waitForLeaderStore(t, stores)
	probe := topic.Topic{Name: "cluster-probe", Partitions: 3}
	if err := leader.CreateTopic(ctx, probe); err != nil {
		t.Fatalf("leader CreateTopic: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for _, store := range stores {
		for time.Now().Before(deadline) {
			got, err := store.GetTopic(ctx, "cluster-probe")
			if err == nil && got.Partitions == 3 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		got, err := store.GetTopic(ctx, "cluster-probe")
		if err != nil {
			t.Fatalf("GetTopic(cluster-probe): %v", err)
		}
		if got.Partitions != 3 {
			t.Fatalf("GetTopic(cluster-probe) = %+v", got)
		}
	}
}

func waitForLeaderStore(t *testing.T, stores []*metastore.Store) *metastore.Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, store := range stores {
			if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__leader_probe__", Partitions: 1}); err == nil {
				_ = store.DeleteTopic(context.Background(), "__leader_probe__")
				return store
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for cluster leader")
	return nil
}
