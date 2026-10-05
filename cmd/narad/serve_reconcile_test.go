package main

// The startup orphan sweep must never delete a live topic's data. Local
// absence is untrustworthy (a freshly restarted replica can be restored
// from an old snapshot and read "caught up" against its own log), so a
// deletion requires the LEADER to confirm absence — and every failure
// mode keeps the directory. A node that leads ITSELF is authoritative
// only after a Raft barrier plus a re-read: election guarantees a fresh
// leader's log, not that its FSM has applied it.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeLeaderView struct {
	leaderID   string
	member     metastore.Member
	memberErr  error
	barrierErr error
	localTopic topic.Topic
	localErr   error

	barriers int
}

func (f *fakeLeaderView) LeaderID() string { return f.leaderID }
func (f *fakeLeaderView) GetMember(string) (metastore.Member, error) {
	return f.member, f.memberErr
}

func (f *fakeLeaderView) Barrier() error {
	f.barriers++
	return f.barrierErr
}

func (f *fakeLeaderView) GetTopic(context.Context, string) (topic.Topic, error) {
	return f.localTopic, f.localErr
}

type fakeTopicGetter struct {
	status int
	body   []byte
	err    error
	calls  int
}

func (f *fakeTopicGetter) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	f.calls++
	if f.err != nil {
		return nodewire.Response{}, f.err
	}
	return nodewire.Response{Status: f.status, Body: f.body}, nil
}

func TestConfirmedAbsentOnLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	remoteLeader := func() *fakeLeaderView {
		return &fakeLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}
	}

	cases := []struct {
		name   string
		view   *fakeLeaderView
		peer   *fakeTopicGetter
		nodeID string
		want   bool
	}{
		{"no leader known keeps the dir", &fakeLeaderView{}, &fakeTopicGetter{status: 404}, "narad-1", false},
		{
			"leader member unresolvable keeps the dir",
			&fakeLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicGetter{status: 404}, "narad-1", false,
		},
		{"leader unreachable keeps the dir", remoteLeader(), &fakeTopicGetter{err: errors.New("timeout")}, "narad-1", false},
		{"leader has the topic keeps the dir", remoteLeader(), &fakeTopicGetter{status: http.StatusOK}, "narad-1", false},
		{"leader 5xx keeps the dir", remoteLeader(), &fakeTopicGetter{status: http.StatusInternalServerError}, "narad-1", false},
		{"leader confirms absence allows deletion", remoteLeader(), &fakeTopicGetter{status: http.StatusNotFound}, "narad-1", true},
		{
			"self leader: barrier then local absence allows deletion",
			&fakeLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound},
			&fakeTopicGetter{status: http.StatusOK}, "narad-1", true,
		},
		{
			"self leader: barrier failure keeps the dir",
			&fakeLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound, barrierErr: errors.New("lost leadership")},
			&fakeTopicGetter{status: http.StatusNotFound}, "narad-1", false,
		},
		{
			"self leader: topic present after the barrier keeps the dir",
			&fakeLeaderView{leaderID: "narad-1", localTopic: topic.Topic{Name: "orphan-topic"}},
			&fakeTopicGetter{status: http.StatusNotFound}, "narad-1", false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := confirmedAbsentOnLeader(context.Background(), tc.view, tc.peer, tc.nodeID, "orphan-topic", log)
			if got != tc.want {
				t.Fatalf("confirmedAbsentOnLeader() = %v, want %v", got, tc.want)
			}
		})
	}

	// Self-leader must barrier exactly once and never RPC itself.
	view := &fakeLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound}
	selfPeer := &fakeTopicGetter{status: http.StatusNotFound}
	if !confirmedAbsentOnLeader(context.Background(), view, selfPeer, "narad-1", "x", log) {
		t.Fatal("self-leader with locally absent topic should confirm after barrier")
	}
	if view.barriers != 1 || selfPeer.calls != 0 {
		t.Fatalf("self-leader path: barriers=%d rpcs=%d, want 1/0", view.barriers, selfPeer.calls)
	}
}

// newCaughtUpStore is a single-node Raft metastore that leads itself and
// has applied everything it committed.
func newCaughtUpStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{NodeID: "n0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	waitForLeadership(t, s)
	if !waitMetastoreCaughtUp(context.Background(), s, 5*time.Second) {
		t.Fatal("replica never caught up")
	}
	return s
}

// logsReclaimer reaches a runtime.Logs the way the move runner reaches
// the broker: the orphan reclaim through the log map, nothing else.
type logsReclaimer struct{ *runtime.Logs }

func (logsReclaimer) ReclaimMovedPartition(context.Context, string, int) error {
	return errors.New("not used by the orphan reclaim")
}

// A boot whose replica catches up only after the bounded startup wait
// gives up forfeits the create-gated orphan sweep. The leader-confirmed
// reclaim still runs once the replica is current: a deleted topic's
// directory whose purge never reached this node, and a quarantined copy
// of a deleted incarnation, go; a live topic stays.
func TestStartupOrphanSweepRunsOnceTheReplicaCatchesUp(t *testing.T) {
	ctx := context.Background()
	store := newCaughtUpStore(t)
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	for _, tp := range []topic.Topic{
		{Name: "orders", ID: "1111111111111111", Partitions: 1},
		{Name: "keep", ID: "2222222222222222", Partitions: 1},
	} {
		if err := store.CreateTopic(ctx, tp); err != nil {
			t.Fatal(err)
		}
		l, err := logs.Get(tp.Name, 0)
		if err != nil {
			t.Fatal(err)
		}
		for range 20 {
			if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte(strings.Repeat("x", 64)))); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.CommitDurable(0, 19); err != nil {
			t.Fatal(err)
		}
	}
	stale := storage.StaleTopicDir(dataDir, "keep", "3333333333333333")
	if err := storage.WriteTopicIncarnation(stale, "3333333333333333"); err != nil {
		t.Fatal(err)
	}
	// The delete commits; its purge never reaches this node.
	if err := logs.CloseTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	orig := startupCatchUpWait
	startupCatchUpWait = func(ctx context.Context, s *metastore.Store, timeout time.Duration) bool {
		if timeout > 0 {
			return false // the replica lagged past the bounded wait
		}
		return orig(ctx, s, timeout)
	}
	t.Cleanup(func() { startupCatchUpWait = orig })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if runStartupReconcile(ctx, store, logs, nil, dataDir, "n0", log) {
		t.Fatal("runStartupReconcile reported caught up through a wait that timed out")
	}
	if _, err := os.Stat(storage.TopicDir(dataDir, "orders")); err != nil {
		t.Fatalf("setup: the forfeited startup sweep touched the deleted topic's directory: %v", err)
	}

	runner := cluster.NewMoveRunner(store, "n0", dataDir, (*cluster.PeerClient)(nil), logsReclaimer{logs}, nil, log, cluster.MoveConfig{})
	var wg sync.WaitGroup
	if !finishLateStartup(ctx, store, logs, runner, wg.Go, "n0", log) {
		t.Fatal("finishLateStartup gave up on a caught-up replica")
	}
	wg.Wait()
	for _, dir := range []string{storage.TopicDir(dataDir, "orders"), stale} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived the deferred sweep (%v)", dir, err)
		}
	}
	l, err := logs.Get("keep", 0)
	if err != nil || l.NextOffset() != 20 {
		t.Fatalf("the live topic after the deferred sweep: err %v", err)
	}
}
