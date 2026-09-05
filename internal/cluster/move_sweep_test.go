package cluster

// The stale-copy sweep may only fire when the local view AND the leader
// agree the partition lives elsewhere. These tests pin the reclaim call
// and every refusal gate; the engine-side guards have their own tests in
// the messaging package.

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

type fakeReclaimer struct {
	mu     sync.Mutex
	calls  []string
	guards []messaging.ReclaimGuard
	err    error
}

func (f *fakeReclaimer) ReclaimMovedPartition(_ context.Context, topicName string, partition int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, topicName)
	return f.err
}

func (f *fakeReclaimer) ReclaimMovedPartitionGuarded(ctx context.Context, topicName string, partition int, guard messaging.ReclaimGuard) error {
	f.mu.Lock()
	f.guards = append(f.guards, guard)
	f.mu.Unlock()
	return f.ReclaimMovedPartition(ctx, topicName, partition)
}

func (f *fakeReclaimer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func sweepTestRunner(t *testing.T, store *fakeMoveStore, rec *fakeReclaimer) (*MoveRunner, string) {
	t.Helper()
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, rec, nil, nil, MoveConfig{})
	return r, dataDir
}

func mkLocalPartitionDir(t *testing.T, dataDir string) string {
	t.Helper()
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return dir
}

// A local dir for a partition owned elsewhere, with the (self-)leader
// confirming, is reclaimed on the first sweep pass.
func TestMoveSweepReclaimsMovedAwayCopy(t *testing.T) {
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	rec := &fakeReclaimer{}
	r, dataDir := sweepTestRunner(t, store, rec)
	mkLocalPartitionDir(t, dataDir)

	r.Reconcile(context.Background()) // pass 1 → sweep runs
	r.wg.Wait()

	if rec.count() != 1 {
		t.Fatalf("reclaim calls = %d, want 1", rec.count())
	}
}

// Every refusal gate: locally owned, moving to us, no local dir, and a
// leader that cannot confirm (follower whose leader RPC fails). None may
// reclaim.
func TestMoveSweepRefusalGates(t *testing.T) {
	cases := []struct {
		name    string
		store   *fakeMoveStore
		makeDir bool
	}{
		{
			name: "locally owned",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-dst"},
			},
			makeDir: true,
		},
		{
			name: "no local dir",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
			},
			makeDir: false,
		},
		{
			name: "leader unreachable for confirmation",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src"},
				leaderID:   "narad-ldr", // not self → follower path → movePeerFake.GetAssignment errors
				member:     metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive},
			},
			makeDir: true,
		},
		{
			name: "unassigned partition",
			store: &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: ""},
			},
			makeDir: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeReclaimer{}
			r, dataDir := sweepTestRunner(t, tc.store, rec)
			if tc.makeDir {
				mkLocalPartitionDir(t, dataDir)
			}
			r.sweepStaleCopies(context.Background())
			if rec.count() != 0 {
				t.Fatalf("%s: reclaim was called (%d times) — must refuse", tc.name, rec.count())
			}
		})
	}
}

// Before reclaiming, the sweep asks the new owner for its move marker and
// hands the promoted HWM to the reclaim, which quarantines a local copy
// that is ahead of it. An owner that cannot be asked defers the sweep;
// an owner without a marker (older release) reclaims unguarded.
func TestMoveSweepPassesPromotedHWMToReclaim(t *testing.T) {
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-new"},
		member:     metastore.Member{ID: "narad-new", Addr: "newaddr", Status: metastore.MemberAlive},
	}
	rec := &fakeReclaimer{}
	dataDir := t.TempDir()
	peer := movePeerFake{marker: &messaging.MoveMarker{Source: "narad-dst", HighWatermark: 42, ForcePromoted: true}}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{})
	mkLocalPartitionDir(t, dataDir)

	r.sweepStaleCopies(context.Background())
	if rec.count() != 1 || len(rec.guards) != 1 {
		t.Fatalf("reclaim calls = %d (guarded %d), want 1 guarded call", rec.count(), len(rec.guards))
	}
	if g := rec.guards[0]; !g.Known || g.PromotedHWM != 42 {
		t.Fatalf("reclaim guard = %+v, want the owner's promoted hwm 42", g)
	}

	// No marker on the owner: unguarded reclaim, as before.
	rec = &fakeReclaimer{}
	r = NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, rec, nil, nil, MoveConfig{})
	r.sweepStaleCopies(context.Background())
	if len(rec.guards) != 1 || rec.guards[0].Known {
		t.Fatalf("guards = %+v, want one unguarded reclaim", rec.guards)
	}

	// Owner has no address: the sweep defers rather than deleting blind.
	rec = &fakeReclaimer{}
	store.member.Addr = ""
	r = NewMoveRunner(store, "narad-dst", dataDir, peer, rec, nil, nil, MoveConfig{})
	r.sweepStaleCopies(context.Background())
	if rec.count() != 0 {
		t.Fatalf("reclaim called %d times with the owner unreachable; must defer", rec.count())
	}
}
