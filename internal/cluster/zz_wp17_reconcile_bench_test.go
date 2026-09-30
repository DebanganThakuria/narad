package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

// zzWP17Partitions is the partition count every seeded topic gets.
const zzWP17Partitions = 12

// zzWP17SeededStore opens a single-node metastore whose replica already
// holds n standalone topics of zzWP17Partitions partitions each, owned
// round-robin by this node and two others. The records go straight into
// fsm.db before the store opens: creating thousands of topics through
// Raft costs an fsync per apply. The bucket names and key layout are the
// metastore's; the sanity check at the end fails the caller if they
// drift.
func zzWP17SeededStore(tb testing.TB, n int) *metastore.Store {
	tb.Helper()
	dir := tb.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "fsm.db"), 0o600, nil)
	if err != nil {
		tb.Fatalf("open fsm.db: %v", err)
	}
	owners := []string{"node-self", "node-b", "node-c"}
	err = db.Update(func(tx *bolt.Tx) error {
		topics, err := tx.CreateBucketIfNotExists([]byte("topics"))
		if err != nil {
			return err
		}
		assignments, err := tx.CreateBucketIfNotExists([]byte("assignments"))
		if err != nil {
			return err
		}
		for i := range n {
			name := fmt.Sprintf("topic-%05d", i)
			v, err := json.Marshal(topic.Topic{
				Name: name, ID: fmt.Sprintf("id-%05d", i), Partitions: zzWP17Partitions,
				RetentionMs: 7_200_000, VisibilityTimeoutMs: 30_000,
				MaxInFlightPerPartition: 1024, MaxAckedAheadPerPartition: 1024,
				CreatedAt: 1_700_000_000_000, Owner: "svc-orders",
			})
			if err != nil {
				return err
			}
			if err := topics.Put([]byte(name), v); err != nil {
				return err
			}
			for p := range zzWP17Partitions {
				av, err := json.Marshal(metastore.Assignment{Topic: name, Partition: p, OwnerID: owners[(i+p)%len(owners)]})
				if err != nil {
					return err
				}
				if err := assignments.Put(fmt.Appendf(nil, "%s:%d", name, p), av); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		tb.Fatalf("seed fsm.db: %v", err)
	}

	store, err := metastore.New(metastore.Config{NodeID: "node-self", DataDir: dir, BindAddr: "127.0.0.1:0"})
	if err != nil {
		tb.Fatalf("metastore.New: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			break
		}
		if time.Now().After(deadline) {
			tb.Fatal("timed out waiting for leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	for !store.AppliedCaughtUp() || !store.OwnershipViewReady() {
		if time.Now().After(deadline) {
			tb.Fatal("timed out waiting for the replica to catch up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _, err := store.ListTopics(context.Background(), metastore.ListOptions{})
	if err != nil || len(got) != n {
		tb.Fatalf("seeded store lists %d topics (err %v), want %d", len(got), err, n)
	}
	if as, err := store.ListAssignments("topic-00000"); n > 0 && (err != nil || len(as) != zzWP17Partitions) {
		tb.Fatalf("seeded store lists %d assignments (err %v), want %d", len(as), err, zzWP17Partitions)
	}
	return store
}

// zzWP17Runners wires the two reconcilers a serving node runs against
// store, the way serve does, minus the broker (no link or move exists,
// so neither ever spawns a worker).
func zzWP17Runners(tb testing.TB, store *metastore.Store) (*FanoutRunner, *MoveRunner) {
	tb.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := tb.TempDir()
	fanout := NewFanoutRunner(store, "node-self", dataDir, nil, nil, partition.NewHashRoundRobin(), nil, logger, FanoutConfig{})
	move := NewMoveRunner(store, "node-self", dataDir, nil, nil, nil, logger, MoveConfig{})
	return fanout, move
}

// BenchmarkZZWP17ReconcileTick is one reconcile tick of both runners (the
// fan-out and the move reconciler each run one per second on every node)
// against a replica with n twelve-partition topics and no fan-out links
// or moves: the steady state of a large cluster. The fan-out runner's
// orphan-cursor sweep runs every 30th tick, as in production, so its cost
// is amortised into the per-tick figure.
func BenchmarkZZWP17ReconcileTick(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("topics=%d", n), func(b *testing.B) {
			store := zzWP17SeededStore(b, n)
			fanout, move := zzWP17Runners(b, store)
			ctx := context.Background()
			fanout.Reconcile(ctx)
			move.Reconcile(ctx)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				fanout.Reconcile(ctx)
				move.Reconcile(ctx)
			}
		})
	}
}
