package cluster

// A fan-out cursor may only tail-anchor (skip to the parent's tail and
// overwrite the shared offset file) under an attach epoch the leader
// confirms is live. Every failure mode must defer, never anchor.

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
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeFanoutLeaderView struct {
	leaderID  string
	member    metastore.Member
	memberErr error
}

func (f fakeFanoutLeaderView) LeaderID() string { return f.leaderID }
func (f fakeFanoutLeaderView) GetMember(string) (metastore.Member, error) {
	return f.member, f.memberErr
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
	leader := fakeFanoutLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}
	liveChild := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "live-epoch"}

	cases := []struct {
		name   string
		view   fakeFanoutLeaderView
		peer   *fakeTopicFetcher
		selfID string
		want   bool
	}{
		{"no cluster identity: local store is authoritative", fakeFanoutLeaderView{}, &fakeTopicFetcher{}, "", true},
		{"no leader known defers", fakeFanoutLeaderView{}, &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", false},
		{"self is leader confirms without an RPC", fakeFanoutLeaderView{leaderID: "narad-1"}, &fakeTopicFetcher{}, "narad-1", true},
		{
			"leader member unresolvable defers",
			fakeFanoutLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", false,
		},
		{
			"leader member without address defers",
			fakeFanoutLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9"}},
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", false,
		},
		{"leader unreachable defers", leader, &fakeTopicFetcher{err: errors.New("timeout")}, "narad-1", false},
		{"leader 5xx defers", leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusInternalServerError}}, "narad-1", false},
		{"child gone on leader defers", leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false},
		{
			"undecodable body defers",
			leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusOK, Body: []byte("not json")}}, "narad-1", false,
		},
		{
			"leader shows a different epoch defers",
			leader,
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "other-epoch"})},
			"narad-1", false,
		},
		{
			"leader shows a different parent defers",
			leader,
			&fakeTopicFetcher{res: topicResponse(t, http.StatusOK, topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "other-parent", AttachEpoch: "live-epoch"})},
			"narad-1", false,
		},
		{"leader confirms the live epoch", leader, &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}, "narad-1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := epochConfirmedByLeader(context.Background(), tc.view, tc.peer, tc.selfID, key, log)
			if got != tc.want {
				t.Fatalf("epochConfirmedByLeader() = %v, want %v", got, tc.want)
			}
		})
	}

	// Neither the identity-less nor the self-leader path may issue an RPC.
	for _, selfID := range []string{"", "narad-1"} {
		peer := &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, liveChild)}
		epochConfirmedByLeader(context.Background(), fakeFanoutLeaderView{leaderID: "narad-1"}, peer, selfID, key, log)
		if peer.calls != 0 {
			t.Fatalf("selfID %q issued %d RPCs, want 0", selfID, peer.calls)
		}
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
