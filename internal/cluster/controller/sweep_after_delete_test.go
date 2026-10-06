package controller_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Assignment rows left behind for a deleted topic (a placement pass of
// an earlier release wrote them after the delete) are pruned by the
// leader, so a topic recreated under the name gets owners of its own
// instead of inheriting them.
func TestSweepAfterDeleteLeavesNoRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestStore(t)
	for _, id := range []string{"a", "b", "c"} {
		if err := s.RegisterMember(ctx, metastore.Member{
			ID: id, Addr: id + ":7943", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix(),
			Build: "narad test", EntryTypes: metastore.MaxEntryType,
		}); err != nil {
			t.Fatal(err)
		}
	}
	dataDir := t.TempDir()
	m := topics.NewManager(dataDir, s, s, schema.NewJSONSchema(),
		consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 2, MaxAckedAhead: 2}, nil
		}, nil),
		runtime.NewLogs(dataDir, storage.Options{}, s, nil),
		topics.Config{DefaultPartitions: 3, MaxPartitions: 64, DefaultRetentionMs: 3_600_000, DefaultVisibilityTimeoutMs: 30_000},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 12}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	// What a placement pass that listed the topic before the delete
	// wrote after it.
	for p := range 12 {
		if err := s.AssignPartition(ctx, "orders", p, "a"); err != nil {
			t.Fatal(err)
		}
	}

	c := controller.New(s, controller.Config{ReconcileInterval: 20 * time.Millisecond, DeadTimeout: time.Hour})
	go c.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, err := s.ListAssignments("orders")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d assignment rows of the deleted topic remain, want 0", len(rows))
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListAssignments("orders")
	if err != nil {
		t.Fatal(err)
	}
	owners := map[string]int{}
	for _, a := range rows {
		owners[a.OwnerID]++
	}
	if len(rows) != 3 || len(owners) != 3 {
		t.Fatalf("recreated topic's owners = %v over %d rows, want 3 rows on 3 members", owners, len(rows))
	}
}
