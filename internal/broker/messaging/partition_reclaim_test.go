package messaging

// Reclaim deletes partition data, so its guards get their own tests: it
// must refuse for anything locally owned (or becoming so) and only delete
// a copy whose partition affirmatively lives elsewhere.

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestReclaimMovedPartitionGuards(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	// p0 owned by us; p1 moved away; p2 owned elsewhere but moving BACK to us.
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-other"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-other"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := store.SetAssignmentTarget(ctx, "orders", 2, "node-self"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	dataDir := engine.logs.DataDir()
	mkPartition := func(p int) string {
		dir := storage.TopicPartitionDir(dataDir, "orders", p)
		log, err := storage.NewLog(dir, storage.Options{})
		if err != nil {
			t.Fatalf("NewLog(p%d): %v", p, err)
		}
		if _, err := log.Append(storage.EncodeKeyedRecord("k", 0, []byte("x"))); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	dir0, dir1, dir2 := mkPartition(0), mkPartition(1), mkPartition(2)

	// Locally owned: refused, data untouched.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 0); err == nil {
		t.Fatal("reclaim of a locally-owned partition must refuse")
	}
	if _, err := os.Stat(dir0); err != nil {
		t.Fatalf("owned partition dir touched: %v", err)
	}

	// Moving back to us: refused, data untouched.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 2); err == nil {
		t.Fatal("reclaim of a partition targeted at this node must refuse")
	}
	if _, err := os.Stat(dir2); err != nil {
		t.Fatalf("inbound-move partition dir touched: %v", err)
	}

	// Moved away: reclaimed, dir gone.
	if err := engine.ReclaimMovedPartition(ctx, "orders", 1); err != nil {
		t.Fatalf("reclaim of a moved-away partition: %v", err)
	}
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Fatalf("moved-away partition dir still present (err=%v)", err)
	}
}

// A source that lost contact (not crashed) kept committing while the
// destination force-promoted its copy at the source's last-seen HWM. When
// the source returns, its copy is AHEAD of the promoted position and holds
// records that exist nowhere else: the reclaim must quarantine it, never
// delete it. A copy at or behind the promoted HWM is reclaimed as before,
// and a reclaim that does not know the promoted position deletes as it
// always did.
func TestReclaimQuarantinesCopyAheadOfPromotedHWM(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	for p := range 3 {
		if err := store.AssignPartition(ctx, "orders", p, "node-other"); err != nil {
			t.Fatalf("AssignPartition: %v", err)
		}
	}
	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	dataDir := engine.logs.DataDir()
	mkPartition := func(p, n int) string {
		dir := storage.TopicPartitionDir(dataDir, "orders", p)
		log, err := storage.NewLog(dir, storage.Options{})
		if err != nil {
			t.Fatalf("NewLog(p%d): %v", p, err)
		}
		for i := range n {
			if _, err := log.Append(storage.EncodeKeyedRecord("k", int64(i), []byte("x"))); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := log.AdvanceHighWatermark(int64(n)); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
		if err := log.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return dir
	}
	dir0, dir1, dir2 := mkPartition(0, 10), mkPartition(1, 10), mkPartition(2, 10)

	// p0: promoted elsewhere at 7, local copy has 10: AHEAD, quarantine.
	err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 0, ReclaimGuard{PromotedHWM: 7, Known: true})
	if !errors.Is(err, ErrPartitionQuarantined) {
		t.Fatalf("reclaim of a copy ahead of the promoted hwm = %v, want ErrPartitionQuarantined", err)
	}
	if _, err := os.Stat(dir0); !os.IsNotExist(err) {
		t.Fatalf("quarantined partition dir still at its original path (err=%v)", err)
	}
	qdir := dir0 + QuarantineSuffix
	log, err := storage.NewLog(qdir, storage.Options{})
	if err != nil {
		t.Fatalf("quarantined copy does not recover: %v", err)
	}
	if log.NextOffset() != 10 {
		t.Fatalf("quarantined copy next offset = %d, want 10 (records intact)", log.NextOffset())
	}
	_ = log.Close()

	// p1: promoted at 10 (or later): the copy holds nothing unique, reclaim.
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 1, ReclaimGuard{PromotedHWM: 10, Known: true}); err != nil {
		t.Fatalf("reclaim of a copy at the promoted hwm: %v", err)
	}
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Fatalf("copy at the promoted hwm not reclaimed (err=%v)", err)
	}
	// p2: promoted position unknown (older owner): delete as before.
	if err := engine.ReclaimMovedPartitionGuarded(ctx, "orders", 2, ReclaimGuard{}); err != nil {
		t.Fatalf("unguarded reclaim: %v", err)
	}
	if _, err := os.Stat(dir2); !os.IsNotExist(err) {
		t.Fatalf("unguarded copy not reclaimed (err=%v)", err)
	}
}
