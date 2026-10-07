package runtime

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A retention alter is committed through the Raft leader, the only node
// that closed its cached logs for it. Every other owner learns of it
// only by applying the replicated entry to its own replica. These tests
// model such an owner: its Logs reads its own replica, and nothing on
// the node closes the logs it has open.

const followTopic = "orders"

// raftPair starts a two-voter metastore in process and returns its
// leader and its follower once both have applied a probe create.
func raftPair(t *testing.T) (leader, follower *metastore.Store) {
	t.Helper()
	ids := []string{"n1", "n2"}
	addrs := map[string]string{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[id] = ln.Addr().String()
		_ = ln.Close()
	}
	base := t.TempDir()
	stores := map[string]*metastore.Store{}
	for _, id := range ids {
		var peers []metastore.Peer
		for _, p := range ids {
			if p != id {
				peers = append(peers, metastore.Peer{ID: p, Addr: addrs[p]})
			}
		}
		s, err := metastore.New(metastore.Config{
			NodeID: id, DataDir: filepath.Join(base, id),
			BindAddr: addrs[id], AdvertiseAddr: addrs[id], Peers: peers,
		})
		if err != nil {
			t.Fatalf("metastore.New(%s): %v", id, err)
		}
		stores[id] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	waitUntil(t, "a raft leader", func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	for _, s := range stores {
		if s != leader {
			follower = s
		}
	}
	return leader, follower
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// agedOpenLog gives g an open log of followTopic/0 holding three
// records, each in its own sealed segment last written two hours ago,
// opened under the record's retention. The shared reaper sweeps it
// three seconds after the open (the Logs' check interval).
func agedOpenLog(t *testing.T, g *Logs) *storage.Log {
	t.Helper()
	l, err := g.Get(followTopic, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := range 3 {
		if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte{byte('a' + i)})); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := l.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if err := l.AdvanceHighWatermark(l.NextOffset()); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
	}
	if err := g.CloseTopic(followTopic); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	dir := topicPartitionDirT(t, g.DataDir(), followTopic, 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, e := range entries {
		if e.Type().IsRegular() {
			if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	l, err = g.Get(followTopic, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if l.OldestOffset() != 0 || l.SegmentCount() < 3 {
		t.Fatalf("setup: oldest %d, %d segments", l.OldestOffset(), l.SegmentCount())
	}
	return l
}

// followLogs is a Logs over ms whose shared-reaper check interval is
// three seconds and whose every synced frame seals a segment.
func followLogs(t *testing.T, ms metastore.Metastore) *Logs {
	t.Helper()
	g := NewLogs(t.TempDir(), storage.Options{
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  1,
		Retention:     storage.RetentionConfig{CheckInterval: 3 * time.Second},
	}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	return g
}

// alterRetention applies a retention alter through store.
func alterRetention(t *testing.T, store *metastore.Store, retention time.Duration) {
	t.Helper()
	ctx := context.Background()
	cur, err := store.GetTopic(ctx, followTopic)
	if err != nil {
		t.Fatal(err)
	}
	cur.RetentionMs = retention.Milliseconds()
	if err := store.UpdateTopic(ctx, cur); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
}

// The owner keeps using the partition after the leader raised its
// retention from 1h to 7d: records the old bound would reap must
// survive the shared reaper's pass on the owner too.
func TestRetentionAlterReachesAnOpenLogOnANonLeaderOwner(t *testing.T) {
	ctx := context.Background()
	leader, follower := raftPair(t)
	waitUntil(t, "a create through the leader", func() bool {
		return leader.CreateTopic(ctx, topic.Topic{
			Name: followTopic, ID: "inc-1", Partitions: 1, RetentionMs: time.Hour.Milliseconds(),
		}) == nil
	})
	waitUntil(t, "the follower to apply the create", func() bool {
		_, err := follower.GetTopic(ctx, followTopic)
		return err == nil
	})
	g := followLogs(t, follower)
	l := agedOpenLog(t, g)

	alterRetention(t, leader, 7*24*time.Hour)
	waitUntil(t, "the follower to apply the alter", func() bool {
		tp, err := follower.GetTopic(ctx, followTopic)
		return err == nil && tp.RetentionMs == (7*24*time.Hour).Milliseconds()
	})

	// Traffic keeps the partition in use past the reaper's first pass.
	deadline := time.Now().Add(4500 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, err := g.Get(followTopic, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got != l {
			t.Fatal("an alter of the same incarnation replaced the open log")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := l.OldestOffset(); got != 0 {
		t.Fatalf("the owner reaped at the old 1h bound after the leader raised retention to 7d: oldest offset %d", got)
	}
	if got := l.RetentionMaxAge(); got != 7*24*time.Hour {
		t.Fatalf("the owner's open log runs under %v, want the altered 7d", got)
	}
}

// An open log nobody touches after the alter follows it too: the
// background pass applies the record before the reaper's next pass.
func TestRetentionAlterReachesAnIdleOpenLog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newIncarnationStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: followTopic, ID: "inc-1", Partitions: 1, RetentionMs: time.Hour.Milliseconds(),
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	g := followLogs(t, store)
	l := agedOpenLog(t, g)
	go g.RunIdleEviction(ctx, 0) // eviction off; the node's background passes still run

	alterRetention(t, store, 7*24*time.Hour)
	time.Sleep(4500 * time.Millisecond)
	if got := l.OldestOffset(); got != 0 {
		t.Fatalf("an idle open log was reaped at the old 1h bound after a raise to 7d: oldest offset %d", got)
	}
}

// A lowered bound frees what it expires on an open log, at the shared
// reaper's next tick.
func TestRetentionLoweredOnAnOpenLogReapsAtTheNewBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newIncarnationStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{
		Name: followTopic, ID: "inc-1", Partitions: 1, RetentionMs: (30 * 24 * time.Hour).Milliseconds(),
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	g := followLogs(t, store)
	l := agedOpenLog(t, g)
	go g.RunColdRetention(ctx, 0) // the walk off; the node's background passes still run

	alterRetention(t, store, time.Hour)
	deadline := time.Now().Add(6 * time.Second)
	for l.OldestOffset() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("an open log kept 2h-old records after retention was lowered to 1h")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The cold walk never runs over a replica that is behind the leader: it
// would open closed logs under a retention an alter already changed.
func TestColdWalkWaitsForACaughtUpReplica(t *testing.T) {
	ms := &laggingMetastore{runtimeFakeMetastore: newRuntimeFakeMetastore()}
	ms.topics[followTopic] = topic.Topic{Name: followTopic, ID: "inc-1", Partitions: 1, RetentionMs: time.Hour.Milliseconds()}
	g := coldTestLogs(t, ms.runtimeFakeMetastore)
	g.metastore = ms
	dueClosedPartition(t, g, followTopic)

	n, err := g.ColdRetentionOnce(context.Background(), time.Now())
	if err != nil || n != 0 {
		t.Fatalf("a walk over a lagging replica swept %d partitions (err %v), want 0", n, err)
	}
	if g.isOpen(keyOf(followTopic, 0)) {
		t.Fatal("a walk over a lagging replica opened a partition")
	}
	ms.caughtUp = true
	if n, err := g.ColdRetentionOnce(context.Background(), time.Now()); err != nil || n != 1 {
		t.Fatalf("a walk over a caught-up replica swept %d partitions (err %v), want 1", n, err)
	}
}

type laggingMetastore struct {
	*runtimeFakeMetastore
	caughtUp bool
}

func (m *laggingMetastore) AppliedCaughtUp() bool { return m.caughtUp }
