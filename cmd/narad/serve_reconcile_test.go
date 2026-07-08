package main

// The startup orphan sweep must never delete a live topic's data. Local
// absence is untrustworthy (a freshly restarted replica can be restored
// from an old snapshot and read "caught up" against its own log), so a
// deletion requires the LEADER to confirm absence — and every failure
// mode keeps the directory.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakeLeaderView struct {
	leaderID  string
	member    metastore.Member
	memberErr error
}

func (f fakeLeaderView) LeaderID() string { return f.leaderID }
func (f fakeLeaderView) GetMember(string) (metastore.Member, error) {
	return f.member, f.memberErr
}

type fakeTopicGetter struct {
	status int
	err    error
	calls  int
}

func (f *fakeTopicGetter) GetTopic(context.Context, string, string) (nodewire.Response, error) {
	f.calls++
	if f.err != nil {
		return nodewire.Response{}, f.err
	}
	return nodewire.Response{Status: f.status}, nil
}

func TestConfirmedAbsentOnLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	leader := fakeLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}

	cases := []struct {
		name   string
		view   fakeLeaderView
		peer   *fakeTopicGetter
		nodeID string
		want   bool
	}{
		{"no leader known keeps the dir", fakeLeaderView{}, &fakeTopicGetter{status: 404}, "narad-1", false},
		{
			"leader member unresolvable keeps the dir",
			fakeLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicGetter{status: 404}, "narad-1", false,
		},
		{
			"leader unreachable keeps the dir",
			leader, &fakeTopicGetter{err: errors.New("timeout")}, "narad-1", false,
		},
		{
			"leader has the topic keeps the dir",
			leader, &fakeTopicGetter{status: http.StatusOK}, "narad-1", false,
		},
		{
			"leader 5xx keeps the dir",
			leader, &fakeTopicGetter{status: http.StatusInternalServerError}, "narad-1", false,
		},
		{
			"leader confirms absence allows deletion",
			leader, &fakeTopicGetter{status: http.StatusNotFound}, "narad-1", true,
		},
		{
			"self is leader: local state is the authority",
			fakeLeaderView{leaderID: "narad-1"},
			&fakeTopicGetter{status: http.StatusOK}, "narad-1", true,
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

	// Self-leader must not issue an RPC to itself.
	selfPeer := &fakeTopicGetter{status: http.StatusNotFound}
	confirmedAbsentOnLeader(context.Background(), fakeLeaderView{leaderID: "narad-1"}, selfPeer, "narad-1", "x", log)
	if selfPeer.calls != 0 {
		t.Fatalf("self-leader check issued %d RPCs, want 0", selfPeer.calls)
	}
}
