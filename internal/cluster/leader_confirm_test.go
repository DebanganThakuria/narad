package cluster

// Discarding a WAL record destroys a 202-acked produce, so absence must
// be confirmed by the leader; every failure mode keeps the record. A
// self-leading node confirms only through a Raft barrier plus re-read.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestTopicAbsentOnLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
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
		{"no cluster identity: local absence is authoritative", &fakeFanoutLeaderView{localErr: errs.ErrNotFound}, &fakeTopicFetcher{}, "", true},
		{"no cluster identity: locally present keeps the records", &fakeFanoutLeaderView{localTopic: topic.Topic{Name: "bench"}}, &fakeTopicFetcher{}, "", false},
		{"no leader known keeps the records", &fakeFanoutLeaderView{}, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false},
		{
			"self leader: barrier then local absence confirms",
			&fakeFanoutLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound}, &fakeTopicFetcher{}, "narad-1", true,
		},
		{
			"self leader: barrier failure keeps the records",
			&fakeFanoutLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound, barrierErr: errors.New("lost leadership")},
			&fakeTopicFetcher{}, "narad-1", false,
		},
		{
			"self leader: topic reappears after the barrier keeps the records",
			&fakeFanoutLeaderView{leaderID: "narad-1", localTopic: topic.Topic{Name: "bench"}}, &fakeTopicFetcher{}, "narad-1", false,
		},
		{
			"leader member unresolvable keeps the records",
			&fakeFanoutLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false,
		},
		{"leader unreachable keeps the records", remoteLeader(), &fakeTopicFetcher{err: errors.New("timeout")}, "narad-1", false},
		{"leader has the topic keeps the records", remoteLeader(), &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, topic.Topic{Name: "bench"})}, "narad-1", false},
		{"leader 5xx keeps the records", remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusInternalServerError}}, "narad-1", false},
		{"leader confirms absence allows the discard", remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := topicAbsentOnLeader(context.Background(), tc.view, tc.peer, tc.selfID, "bench", log)
			if got != tc.want {
				t.Fatalf("topicAbsentOnLeader() = %v, want %v", got, tc.want)
			}
		})
	}

	// Self-leader must barrier exactly once and issue no peer RPC.
	view := &fakeFanoutLeaderView{leaderID: "narad-1", localErr: errs.ErrNotFound}
	peer := &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}
	if !topicAbsentOnLeader(context.Background(), view, peer, "narad-1", "bench", log) {
		t.Fatal("self-leader with locally absent topic should confirm after barrier")
	}
	if view.barriers != 1 || peer.calls != 0 {
		t.Fatalf("self-leader path: barriers=%d rpcs=%d, want 1/0", view.barriers, peer.calls)
	}
}

// Removing a cursor offset file requires the leader to confirm the link
// is dissolved — a stale replica missing a live link would otherwise
// force the real cursor to tail-anchor and skip its backlog.
func TestFanoutLinkDissolvedOnLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	remoteLeader := func() *fakeFanoutLeaderView {
		return &fakeFanoutLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}
	}
	linked := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "parent", AttachEpoch: "e"}
	reattached := topic.Topic{Name: "child", Role: topic.RoleChild, Parent: "other-parent", AttachEpoch: "e2"}

	cases := []struct {
		name string
		view *fakeFanoutLeaderView
		peer *fakeTopicFetcher
		want bool
	}{
		{"leader unreachable keeps the file", remoteLeader(), &fakeTopicFetcher{err: errors.New("timeout")}, false},
		{"leader shows the live link keeps the file", remoteLeader(), &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, linked)}, false},
		{"leader shows the child gone allows removal", remoteLeader(), &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, true},
		{"leader shows the child under another parent allows removal", remoteLeader(), &fakeTopicFetcher{res: topicResponse(t, http.StatusOK, reattached)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fanoutLinkDissolvedOnLeader(context.Background(), tc.view, tc.peer, "narad-1", "parent", "child", log)
			if got != tc.want {
				t.Fatalf("fanoutLinkDissolvedOnLeader() = %v, want %v", got, tc.want)
			}
		})
	}
}
