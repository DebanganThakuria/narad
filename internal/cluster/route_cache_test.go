package cluster

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

func TestOwnerAddrReturnsRemoteMemberAddress(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	got := router.ownerAddr("orders", 1)
	if got != "remote.example:7942" {
		t.Fatalf("ownerAddr() = %q, want %q", got, "remote.example:7942")
	}
}

func TestOwnerAddrReturnsEmptyForLocalOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 0); got != "" {
		t.Fatalf("ownerAddr() = %q, want empty", got)
	}
}

func TestOwnerAddrReturnsEmptyForDeadMember(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 2); got != "" {
		t.Fatalf("ownerAddr() = %q, want empty", got)
	}
}

func TestRouteCacheInvalidatesWhenRoutingMemberVersionChanges(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 0); got != "remote.example:7942" {
		t.Fatalf("ownerAddr() before member death = %q, want remote.example:7942", got)
	}
	if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}
	if got := router.ownerAddr("orders", 0); got != "" {
		t.Fatalf("ownerAddr() after member death = %q, want empty", got)
	}
}

func TestRouteCacheInvalidatesWhenAssignmentVersionChanges(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(remote) error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 0); got != "remote.example:7942" {
		t.Fatalf("ownerAddr() before reassignment = %q, want remote.example:7942", got)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition(self) error = %v", err)
	}
	if got := router.ownerAddr("orders", 0); got != "" {
		t.Fatalf("ownerAddr() after reassignment = %q, want empty local owner", got)
	}
}

func TestRouteCacheKeepsEntryOnHeartbeatOnlyMemberUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 0); got != "remote.example:7942" {
		t.Fatalf("ownerAddr() initial = %q, want remote.example:7942", got)
	}
	router.routeMu.RLock()
	before := router.routes["orders"]
	router.routeMu.RUnlock()

	if err := store.RegisterMember(ctx, metastore.Member{
		ID:            "node-remote",
		Addr:          "remote.example:7942",
		Status:        metastore.MemberAlive,
		LastHeartbeat: 1234,
	}); err != nil {
		t.Fatalf("RegisterMember() heartbeat-only update error = %v", err)
	}

	if got := router.ownerAddr("orders", 0); got != "remote.example:7942" {
		t.Fatalf("ownerAddr() after heartbeat-only update = %q, want remote.example:7942", got)
	}
	router.routeMu.RLock()
	after := router.routes["orders"]
	router.routeMu.RUnlock()
	if after.assignmentVersion != before.assignmentVersion || after.routingMembersVersion != before.routingMembersVersion {
		t.Fatalf("route versions after heartbeat-only update = (%d,%d), want (%d,%d)",
			after.assignmentVersion, after.routingMembersVersion, before.assignmentVersion, before.routingMembersVersion)
	}
}

func TestRouteCacheClearsConsumeCursorsOnAssignmentChange(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition(self) error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	if _, ok := router.routesForTopic("orders"); !ok {
		t.Fatal("routesForTopic() initial = false, want true")
	}

	router.consumeMu.Lock()
	router.consumeCursor["orders"] = 1
	router.consumeCursor["orders:local"] = 2
	router.consumeCursor["orders:remote"] = 3
	router.consumeMu.Unlock()

	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(remote) error = %v", err)
	}
	if _, ok := router.routesForTopic("orders"); !ok {
		t.Fatal("routesForTopic() after reassignment = false, want true")
	}

	router.consumeMu.Lock()
	_, base := router.consumeCursor["orders"]
	_, local := router.consumeCursor["orders:local"]
	_, remote := router.consumeCursor["orders:remote"]
	router.consumeMu.Unlock()
	if base || local || remote {
		t.Fatalf("consume cursors after assignment change: base=%v local=%v remote=%v, want all false", base, local, remote)
	}
}
