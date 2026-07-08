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
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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
