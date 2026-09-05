package cluster

// Moves and the periodic sweep are the two paths that touch topic
// directories without an open, so they carry their own incarnation
// checks: a copy of another incarnation is never installed, a local
// directory of a deleted incarnation is set aside (after the leader
// confirms) rather than mistaken for a stale copy, and a quarantined
// directory is reclaimed only once the leader confirms its incarnation
// is gone.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// keepingReclaimer is a fakeReclaimer that also prepares topic
// directories through a real runtime.Logs, like the engine does.
type keepingReclaimer struct {
	fakeReclaimer
	logs *runtime.Logs
}

func (k *keepingReclaimer) EnsureTopicIncarnation(topicName, id string) error {
	return k.logs.EnsureTopicIncarnation(topicName, id)
}

func newKeepingReclaimer(t *testing.T, dataDir string) *keepingReclaimer {
	t.Helper()
	logs := runtime.NewLogs(dataDir, storage.Options{}, nil, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	return &keepingReclaimer{logs: logs}
}

// A source that reports its copy as belonging to another incarnation of
// the topic (the source never purged the deleted topic's partition, or
// this replica is behind a recreate) is refused: no install, no flip.
func TestMoveRunnerRefusesCopyOfAnotherIncarnation(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 8)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:     []topic.Topic{{Name: "orders", ID: "1111111111111111", Partitions: 1}},
	}
	peer := movePeerFake{
		dirFetcher:  dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		incarnation: "0000000000000000",
	}
	dataDir := t.TempDir()
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	r.Reconcile(ctx)
	r.wg.Wait()

	if len(store.completeArgs) != 0 {
		t.Fatalf("flip proposed for a copy of another incarnation: %v", store.completeArgs)
	}
	if _, err := os.Stat(storage.TopicPartitionDir(dataDir, "orders", 0)); !os.IsNotExist(err) {
		t.Fatalf("partition of another incarnation was installed (stat err %v)", err)
	}
}

// A matching incarnation installs, and the install stamps the topic
// directory with it (a copy landing on a node that never opened the
// topic must leave a marked directory behind).
func TestMoveRunnerInstallStampsIncarnation(t *testing.T) {
	src := t.TempDir()
	wantHWM, _ := buildSourcePartition(t, src, 8)
	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:     []topic.Topic{{Name: "orders", ID: "1111111111111111", Partitions: 1}},
	}
	peer := movePeerFake{
		dirFetcher:  dirFetcher{dir: src, hwm: wantHWM, committed: 5, hasCommitted: true},
		incarnation: "1111111111111111",
	}
	dataDir := t.TempDir()
	// A deleted incarnation's directory is already there: the install
	// must set it aside, not drop the copy into it.
	if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "orders"), "0000000000000000"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{RetryBackoff: 5 * time.Millisecond})

	r.Reconcile(context.Background())
	r.wg.Wait()

	if len(store.completeArgs) != 3 {
		t.Fatalf("flip not proposed: %v", store.completeArgs)
	}
	id, ok, err := storage.ReadTopicIncarnation(storage.TopicDir(dataDir, "orders"))
	if err != nil || !ok || id != "1111111111111111" {
		t.Fatalf("marker after install = (%q, %v, %v), want 1111111111111111", id, ok, err)
	}
	// The old incarnation's directory was set aside (and, this node
	// being its own leader, its quarantine reclaimed in the same pass).
	log, err := storage.NewLog(storage.TopicPartitionDir(dataDir, "orders", 0), storage.Options{})
	if err != nil {
		t.Fatalf("recover installed partition: %v", err)
	}
	defer log.Close()
	if log.NextOffset() != wantHWM {
		t.Fatalf("installed NextOffset = %d, want %d", log.NextOffset(), wantHWM)
	}
}

// The sweep finds topics/orders carrying a marker for an incarnation
// other than the live topic's. It sets the directory aside only after
// the leader confirms the live incarnation, and never treats its
// partitions as stale copies to reclaim.
func TestMoveSweepSetsAsideDirOfDeletedIncarnation(t *testing.T) {
	live := topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}
	newStore := func(leaderID string) *fakeMoveStore {
		return &fakeMoveStore{
			assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-other"},
			member:     metastore.Member{ID: "narad-other", Addr: "otheraddr", Status: metastore.MemberAlive},
			topics:     []topic.Topic{live},
			leaderID:   leaderID,
		}
	}
	prepare := func(t *testing.T) string {
		dataDir := t.TempDir()
		mkLocalPartitionDir(t, dataDir)
		if err := storage.WriteTopicIncarnation(storage.TopicDir(dataDir, "orders"), "1111111111111111"); err != nil {
			t.Fatalf("WriteTopicIncarnation: %v", err)
		}
		return dataDir
	}

	t.Run("leader unreachable keeps the dir in place", func(t *testing.T) {
		dataDir := prepare(t)
		rec := newKeepingReclaimer(t, dataDir)
		store := newStore("narad-ldr")
		store.member = metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}
		r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{}, rec, nil, nil, MoveConfig{})
		r.sweepStaleCopies(context.Background())
		if rec.count() != 0 {
			t.Fatalf("reclaim called %d times for a directory of another incarnation", rec.count())
		}
		if _, err := os.Stat(storage.TopicPartitionDir(dataDir, "orders", 0)); err != nil {
			t.Fatalf("directory moved without leader confirmation: %v", err)
		}
	})

	t.Run("leader confirms the live incarnation: dir set aside, no partition reclaim", func(t *testing.T) {
		dataDir := prepare(t)
		rec := newKeepingReclaimer(t, dataDir)
		store := newStore("narad-ldr")
		store.member = metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive}
		r := NewMoveRunner(store, "narad-dst", dataDir, movePeerFake{leaderTopic: &live}, rec, nil, nil, MoveConfig{})
		r.sweepStaleCopies(context.Background())
		if rec.count() != 0 {
			t.Fatalf("reclaim called %d times for a directory of another incarnation", rec.count())
		}
		// The old partition is no longer under the live topic's directory
		// (quarantined; the same pass then reclaimed the quarantine, the
		// leader having confirmed the incarnation is gone).
		if _, err := os.Stat(filepath.Join(storage.TopicDir(dataDir, "orders"), "p00000")); !os.IsNotExist(err) {
			t.Fatalf("deleted incarnation's partition still under the live topic dir (stat err %v)", err)
		}
		if _, err := os.Stat(storage.StaleTopicDir(dataDir, "orders", "1111111111111111")); !os.IsNotExist(err) {
			t.Fatalf("quarantine not reclaimed after leader confirmation (stat err %v)", err)
		}
		id, ok, _ := storage.ReadTopicIncarnation(storage.TopicDir(dataDir, "orders"))
		if !ok || id != live.ID {
			t.Fatalf("marker after set-aside = (%q, %v), want the live incarnation", id, ok)
		}
	})
}

// A quarantined directory is reclaimed by the periodic sweep only after
// the leader confirms its incarnation is gone; it is kept while the
// leader is unreachable or still reports that incarnation live.
func TestMoveSweepReclaimsQuarantineAfterLeaderConfirms(t *testing.T) {
	cases := []struct {
		name     string
		peer     movePeerFake
		wantGone bool
	}{
		{"leader unreachable keeps it", movePeerFake{}, false},
		{"leader still has that incarnation keeps it", movePeerFake{leaderTopic: &topic.Topic{Name: "orders", ID: "1111111111111111"}}, false},
		{"leader has a newer incarnation reclaims it", movePeerFake{leaderTopic: &topic.Topic{Name: "orders", ID: "2222222222222222"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			stale := storage.StaleTopicDir(dataDir, "orders", "1111111111111111")
			if err := storage.WriteTopicIncarnation(stale, "1111111111111111"); err != nil {
				t.Fatalf("WriteTopicIncarnation: %v", err)
			}
			store := &fakeMoveStore{
				assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-dst"},
				member:     metastore.Member{ID: "narad-ldr", Addr: "ldraddr", Status: metastore.MemberAlive},
				topics:     []topic.Topic{{Name: "orders", ID: "2222222222222222", Partitions: 1}},
				leaderID:   "narad-ldr",
			}
			r := NewMoveRunner(store, "narad-dst", dataDir, tc.peer, newKeepingReclaimer(t, dataDir), nil, nil, MoveConfig{})
			r.sweepStaleCopies(context.Background())
			_, err := os.Stat(stale)
			if tc.wantGone && !os.IsNotExist(err) {
				t.Fatalf("quarantined dir still present (stat err %v), want reclaimed", err)
			}
			if !tc.wantGone && err != nil {
				t.Fatalf("quarantined dir reclaimed without leader confirmation: %v", err)
			}
		})
	}
}
