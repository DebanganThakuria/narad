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

func TestMetadataVersionAdvancesOnSuccessfulMutation(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	before := s.MetadataVersion()
	ord := topic.Topic{Name: "orders", Partitions: 1}
	if err := s.CreateTopic(ctx, ord); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	afterCreate := s.MetadataVersion()
	if afterCreate <= before {
		t.Fatalf("MetadataVersion after create = %d, want > %d", afterCreate, before)
	}

	if err := s.CreateTopic(ctx, ord); err != metastore.ErrAlreadyExists {
		t.Fatalf("duplicate CreateTopic: got %v, want ErrAlreadyExists", err)
	}
	afterFailedCreate := s.MetadataVersion()
	if afterFailedCreate != afterCreate {
		t.Fatalf("MetadataVersion after failed create = %d, want %d", afterFailedCreate, afterCreate)
	}
}

func TestDomainVersionsAdvanceOnlyForAffectedMetadata(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	initialTopic := s.TopicVersion("orders")
	initialAssignment := s.AssignmentVersion("orders")
	initialSchema := s.SchemaVersion("orders")
	initialRoutingMembers := s.RoutingMembersVersion()

	orders := topic.Topic{Name: "orders", Partitions: 1}
	if err := s.CreateTopic(ctx, orders); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	afterCreateTopic := s.TopicVersion("orders")
	if afterCreateTopic <= initialTopic {
		t.Fatalf("TopicVersion after create = %d, want > %d", afterCreateTopic, initialTopic)
	}
	if got := s.AssignmentVersion("orders"); got != initialAssignment {
		t.Fatalf("AssignmentVersion after create = %d, want %d", got, initialAssignment)
	}
	if got := s.SchemaVersion("orders"); got != initialSchema {
		t.Fatalf("SchemaVersion after create = %d, want %d", got, initialSchema)
	}
	if got := s.RoutingMembersVersion(); got != initialRoutingMembers {
		t.Fatalf("RoutingMembersVersion after create = %d, want %d", got, initialRoutingMembers)
	}

	if err := s.CreateTopic(ctx, topic.Topic{Name: "payments", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic payments: %v", err)
	}
	if got := s.TopicVersion("orders"); got != afterCreateTopic {
		t.Fatalf("TopicVersion(orders) after unrelated topic create = %d, want %d", got, afterCreateTopic)
	}

	orders.RetentionMs = 10
	if err := s.UpdateTopic(ctx, orders); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	afterUpdateTopic := s.TopicVersion("orders")
	if afterUpdateTopic <= afterCreateTopic {
		t.Fatalf("TopicVersion after update = %d, want > %d", afterUpdateTopic, afterCreateTopic)
	}

	if err := s.AssignPartition(ctx, "orders", 0, "narad-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	afterAssign := s.AssignmentVersion("orders")
	if afterAssign <= initialAssignment {
		t.Fatalf("AssignmentVersion after assign = %d, want > %d", afterAssign, initialAssignment)
	}
	if got := s.TopicVersion("orders"); got != afterUpdateTopic {
		t.Fatalf("TopicVersion after assign = %d, want %d", got, afterUpdateTopic)
	}

	if err := s.PutSchema(ctx, "orders", 1, []byte(`{"type":"object"}`)); err != nil {
		t.Fatalf("PutSchema: %v", err)
	}
	afterSchema := s.SchemaVersion("orders")
	if afterSchema <= initialSchema {
		t.Fatalf("SchemaVersion after put = %d, want > %d", afterSchema, initialSchema)
	}
	if got := s.AssignmentVersion("orders"); got != afterAssign {
		t.Fatalf("AssignmentVersion after schema put = %d, want %d", got, afterAssign)
	}

	member := metastore.Member{ID: "narad-0", Addr: "10.0.0.1:7943", ClusterAddr: "10.0.0.1:7942", Status: metastore.MemberAlive, LastHeartbeat: 1000}
	if err := s.RegisterMember(ctx, member); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	afterMemberJoin := s.RoutingMembersVersion()
	if afterMemberJoin <= initialRoutingMembers {
		t.Fatalf("RoutingMembersVersion after member join = %d, want > %d", afterMemberJoin, initialRoutingMembers)
	}

	member.LastHeartbeat = 2000
	if err := s.RegisterMember(ctx, member); err != nil {
		t.Fatalf("RegisterMember heartbeat-only update: %v", err)
	}
	if got := s.RoutingMembersVersion(); got != afterMemberJoin {
		t.Fatalf("RoutingMembersVersion after heartbeat-only register = %d, want %d", got, afterMemberJoin)
	}
	if err := s.Heartbeat(ctx, "narad-0", 3000); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if got := s.RoutingMembersVersion(); got != afterMemberJoin {
		t.Fatalf("RoutingMembersVersion after Heartbeat = %d, want %d", got, afterMemberJoin)
	}

	member.Addr = "10.0.0.2:7943"
	member.LastHeartbeat = 4000
	if err := s.RegisterMember(ctx, member); err != nil {
		t.Fatalf("RegisterMember addr update: %v", err)
	}
	afterMemberAddr := s.RoutingMembersVersion()
	if afterMemberAddr <= afterMemberJoin {
		t.Fatalf("RoutingMembersVersion after addr update = %d, want > %d", afterMemberAddr, afterMemberJoin)
	}

	if err := s.MarkMemberDead(ctx, "narad-0"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	afterMemberDead := s.RoutingMembersVersion()
	if afterMemberDead <= afterMemberAddr {
		t.Fatalf("RoutingMembersVersion after member dead = %d, want > %d", afterMemberDead, afterMemberAddr)
	}

	if err := s.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := s.TopicVersion("orders"); got <= afterUpdateTopic {
		t.Fatalf("TopicVersion after delete = %d, want > %d", got, afterUpdateTopic)
	}
	if got := s.AssignmentVersion("orders"); got <= afterAssign {
		t.Fatalf("AssignmentVersion after delete = %d, want > %d", got, afterAssign)
	}
	if got := s.SchemaVersion("orders"); got <= afterSchema {
		t.Fatalf("SchemaVersion after delete = %d, want > %d", got, afterSchema)
	}
	if got := s.RoutingMembersVersion(); got != afterMemberDead {
		t.Fatalf("RoutingMembersVersion after topic delete = %d, want %d", got, afterMemberDead)
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

func TestListTopicsPaginationAfterTokenTopicDeleted(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for _, name := range []string{"aaa", "bbb", "ccc", "ddd"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 1}); err != nil {
			t.Fatalf("CreateTopic %s: %v", name, err)
		}
	}

	page1, tok, err := s.ListTopics(ctx, metastore.ListOptions{Limit: 2})
	if err != nil || len(page1) != 2 || tok != "bbb" {
		t.Fatalf("page1: topics=%v tok=%q err=%v", page1, tok, err)
	}

	// Deleting the page-token topic between pages must not skip the topic
	// that Seek lands on next.
	if err := s.DeleteTopic(ctx, tok); err != nil {
		t.Fatalf("DeleteTopic(%s): %v", tok, err)
	}

	page2, _, err := s.ListTopics(ctx, metastore.ListOptions{Limit: 2, PageToken: tok})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 2 || page2[0].Name != "ccc" || page2[1].Name != "ddd" {
		t.Fatalf("page2 = %+v, want ccc,ddd", page2)
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
		if err := s.AssignPartition(ctx, "orders", p, "narad-0"); err != nil {
			t.Fatalf("AssignPartition %d: %v", p, err)
		}
	}
	s.AssignPartition(ctx, "payments", 0, "narad-1")

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

func TestStoreAppliedCaughtUp(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// An elected single-node leader that has applied its probe writes is
	// caught up.
	if !s.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp() = false, want true for an elected single-node leader")
	}

	// Remains caught up after further committed+applied writes.
	if err := s.CreateTopic(ctx, topic.Topic{Name: "caughtup-topic", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !s.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp() = false after an applied write, want true")
	}
}
