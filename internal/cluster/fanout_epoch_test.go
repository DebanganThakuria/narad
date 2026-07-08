package cluster

// A fan-out cursor may only tail-anchor (skip to the parent's tail and
// overwrite the shared offset file) under an attach epoch the leader
// confirms is live. Every failure mode must defer, never anchor — and
// "this node is the leader" is only authority AFTER a Raft barrier plus
// a re-read, because a freshly elected leader's FSM can still be
// replaying an old snapshot.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeFanoutLeaderView struct {
	leaderID   string
	member     metastore.Member
	memberErr  error
	barrierErr error
	localTopic topic.Topic
	localErr   error

	barriers int
}

func (f *fakeFanoutLeaderView) LeaderID() string { return f.leaderID }
func (f *fakeFanoutLeaderView) GetMember(string) (metastore.Member, error) {
	return f.member, f.memberErr
}

func (f *fakeFanoutLeaderView) Barrier() error {
	f.barriers++
	return f.barrierErr
}

func (f *fakeFanoutLeaderView) GetTopic(context.Context, string) (topic.Topic, error) {
	return f.localTopic, f.localErr
}

type fakeTopicFetcher struct {
	res   nodewire.Response
	err   error
	calls int
}

func (f *fakeTopicFetcher) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	f.calls++
	return f.res, f.err
}

func topicResponse(t *testing.T, status int, rec topic.Topic) nodewire.Response {
	t.Helper()
	body, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal topic: %v", err)
	}
	return nodewire.Response{Status: status, Body: body}
}

func TestEpochConfirmedByLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	key := fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: "live-epoch"}
	liveChild := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "live-epoch"}
	staleChild := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "other-epoch"}
	remoteLeader := func() *fakeFanoutLeaderView {
		return &fakeFanoutLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}
	}

	cases := []struct {
		name   string
		view   *fakeFanoutLeaderView
		peer   *fakeTopicFetcher
		selfID string
		want   bool
	}{
		{"no cluster identity reads local state", &fakeFanoutLeaderView{localTopic: liveChild}, &fakeTopicFetcher{}, "", true},
		{"no cluster identity, local epoch differs, defers", &fakeFanoutLeaderView{localTopic: staleChild}, &fakeTopicFetcher{}, "", false},
		{"no leader known defers", &fakeFanoutLeaderView{}, &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", false},
		{
			"self leader: barrier then matching local state confirms",
			&fakeFanoutLeaderView{leaderID: "narad-1", localTopic: liveChild}, &fakeTopicFetcher{}, "narad-1", true,
		},
		{
			"self leader: barrier failure defers",
			&fakeFanoutLeaderView{leaderID: "narad-1", localTopic: liveChild, barrierErr: errors.New("not leader anymore")},
			&fakeTopicFetcher{}, "narad-1", false,
		},
		{
			"self leader: post-barrier state shows another epoch, defers",
			&fakeFanoutLeaderView{leaderID: "narad-1", localTopic: staleChild}, &fakeTopicFetcher{}, "narad-1", false,
		},
		{
			"self leader: post-barrier child gone defers",
			&fakeFanoutLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound}, &fakeTopicFetcher{}, "narad-1", false,
		},
		{
			"leader member unresolvable defers",
			&fakeFanoutLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", false,
		},
		{"leader unreachable defers", remoteLeader(), &fakeTopicFetcher{err: errors.New("timeout")}, "narad-1", false},
		{"leader 5xx defers", remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusInternalServerError}}, "narad-1", false},
		{"child gone on leader defers", remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false},
		{
			"undecodable body defers",
			remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusOK, Body: []byte("not json")}}, "narad-1", false,
		},
		{
			"leader shows a different epoch defers",
			remoteLeader(), &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, staleChild)}, "narad-1", false,
		},
		{
			"leader shows a different parent defers",
			remoteLeader(),
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "other-parent", AttachEpoch: "live-epoch"})},
			"narad-1", false,
		},
		{"leader confirms the live epoch", remoteLeader(), &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := epochConfirmedByLeader(context.Background(), tc.view, tc.peer, tc.selfID, key, log)
			if got != tc.want {
				t.Fatalf("epochConfirmedByLeader() = %v, want %v", got, tc.want)
			}
		})
	}

	// The self-leader path must barrier before trusting local state, and
	// neither it nor the identity-less path may issue a peer RPC.
	view := &fakeFanoutLeaderView{leaderID: "narad-1", localTopic: liveChild}
	peer := &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}
	if !epochConfirmedByLeader(context.Background(), view, peer, "narad-1", key, log) {
		t.Fatal("self-leader with matching state should confirm")
	}
	if view.barriers != 1 {
		t.Fatalf("self-leader path ran %d barriers, want 1", view.barriers)
	}
	if peer.calls != 0 {
		t.Fatalf("self-leader path issued %d RPCs, want 0", peer.calls)
	}
	noID := &fakeFanoutLeaderView{localTopic: liveChild}
	epochConfirmedByLeader(context.Background(), noID, peer, "", key, log)
	if noID.barriers != 0 || peer.calls != 0 {
		t.Fatalf("identity-less path: barriers=%d rpcs=%d, want 0/0", noID.barriers, peer.calls)
	}
}

// The offset file is shared across attachments of (parent, partition,
// child). A stopped cursor may remove it only when the file's contents
// belong to that cursor's own epoch — a cursor spawned from a stale
// replica carries a dead epoch and must not erase the live attachment's
// resume point.
func TestCleanUpStoppedCursorFileEpochGuard(t *testing.T) {
	newRunner := func(t *testing.T) (*FanoutRunner, string) {
		t.Helper()
		dataDir := t.TempDir()
		r := &FanoutRunner{
			dataDir: dataDir,
			logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			cursors: map[fanoutCursorKey]*fanoutCursorHandle{},
		}
		dir := storage.TopicPartitionDir(dataDir, "parent", 0)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir partition dir: %v", err)
		}
		return r, dir
	}
	writeCursor := func(t *testing.T, dir, epoch string) {
		t.Helper()
		err := storage.WriteFanoutCursorIfPartitionDirExists(dir, "child", storage.FanoutCursor{Epoch: epoch, NextOffset: 42})
		if err != nil {
			t.Fatalf("write cursor file: %v", err)
		}
	}
	fileEpoch := func(t *testing.T, dir string) (string, bool) {
		t.Helper()
		cur, ok, err := storage.ReadFanoutCursor(dir, "child")
		if err != nil {
			t.Fatalf("read cursor file: %v", err)
		}
		return cur.Epoch, ok
	}
	liveByName := map[string]topic.Topic{
		"child": {Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "live-epoch"},
	}

	t.Run("keeps a file owned by another attachment", func(t *testing.T) {
		r, dir := newRunner(t)
		writeCursor(t, dir, "live-epoch")
		r.cleanUpStoppedCursor(fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: "stale-epoch"}, liveByName)
		if epoch, ok := fileEpoch(t, dir); !ok || epoch != "live-epoch" {
			t.Fatalf("live attachment's cursor file destroyed (ok=%v epoch=%q)", ok, epoch)
		}
	})

	t.Run("removes its own file when the link dissolved", func(t *testing.T) {
		r, dir := newRunner(t)
		writeCursor(t, dir, "dead-epoch")
		r.cleanUpStoppedCursor(fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: "dead-epoch"}, map[string]topic.Topic{})
		if _, ok := fileEpoch(t, dir); ok {
			t.Fatal("dissolved link's cursor file should be removed")
		}
	})

	t.Run("keeps the file while the link is still live", func(t *testing.T) {
		r, dir := newRunner(t)
		writeCursor(t, dir, "live-epoch")
		r.cleanUpStoppedCursor(fanoutCursorKey{parent: "parent", partition: 0, child: "child", epoch: "live-epoch"}, liveByName)
		if epoch, ok := fileEpoch(t, dir); !ok || epoch != "live-epoch" {
			t.Fatalf("live link's cursor file destroyed (ok=%v epoch=%q)", ok, epoch)
		}
	})
}
