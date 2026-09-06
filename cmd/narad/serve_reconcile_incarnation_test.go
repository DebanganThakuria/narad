package main

// The startup sweep decides by INCARNATION: a directory whose marker is
// not the live topic's ID is an orphan whether or not the name exists
// (deleted and recreated while this node was down), and a quarantined
// directory is reclaimed once its incarnation is confirmed gone. Every
// decision to delete still needs the leader's word.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func topicBody(t *testing.T, rec topic.Topic) []byte {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestKeepTopicDirByIncarnation(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	remoteLeader := func(local topic.Topic, localErr error) *fakeLeaderView {
		return &fakeLeaderView{
			leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"},
			localTopic: local, localErr: localErr,
		}
	}
	oldDir := runtime.OrphanCandidate{Topic: "orders", Incarnation: "1111111111111111"}
	quarantined := runtime.OrphanCandidate{Topic: "orders", Incarnation: "1111111111111111", Quarantined: true}
	unmarked := runtime.OrphanCandidate{Topic: "orders"}
	liveOld := topic.Topic{Name: "orders", ID: "1111111111111111"}
	liveNew := topic.Topic{Name: "orders", ID: "2222222222222222"}
	liveNoID := topic.Topic{Name: "orders"}

	cases := []struct {
		name string
		view *fakeLeaderView
		peer *fakeTopicGetter
		c    runtime.OrphanCandidate
		keep bool
	}{
		{"same incarnation live locally: keep without asking", remoteLeader(liveOld, nil), &fakeTopicGetter{status: http.StatusNotFound}, oldDir, true},
		{"unmarked dir, name live locally: keep (name-based)", remoteLeader(liveNew, nil), &fakeTopicGetter{status: http.StatusNotFound}, unmarked, true},
		{"local record without id: keep (name-based)", remoteLeader(liveNoID, nil), &fakeTopicGetter{status: http.StatusNotFound}, oldDir, true},
		{"recreated locally, leader confirms new incarnation: remove", remoteLeader(liveNew, nil), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveNew)}, oldDir, false},
		{"recreated locally, leader unreachable: keep", remoteLeader(liveNew, nil), &fakeTopicGetter{err: context.DeadlineExceeded}, oldDir, true},
		{"recreated locally, leader still has the old incarnation: keep", remoteLeader(liveNew, nil), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveOld)}, oldDir, true},
		{"recreated locally, leader record without id: keep", remoteLeader(liveNew, nil), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveNoID)}, oldDir, true},
		{"absent locally, leader 404: remove", remoteLeader(topic.Topic{}, errs.ErrNotFound), &fakeTopicGetter{status: http.StatusNotFound}, oldDir, false},
		{"absent locally, leader has it as the same incarnation: keep", remoteLeader(topic.Topic{}, errs.ErrNotFound), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveOld)}, oldDir, true},
		{"quarantined, name live locally as newer incarnation, leader confirms: remove", remoteLeader(liveNew, nil), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveNew)}, quarantined, false},
		{"quarantined, leader still has that incarnation: keep", remoteLeader(liveOld, nil), &fakeTopicGetter{status: http.StatusOK, body: topicBody(t, liveOld)}, quarantined, true},
		{"quarantined, leader 404: remove", remoteLeader(topic.Topic{}, errs.ErrNotFound), &fakeTopicGetter{status: http.StatusNotFound}, quarantined, false},
		{"quarantined, leader unreachable: keep", remoteLeader(liveNew, nil), &fakeTopicGetter{err: context.DeadlineExceeded}, quarantined, true},
		{"local lookup failure: keep", remoteLeader(topic.Topic{}, context.DeadlineExceeded), &fakeTopicGetter{status: http.StatusNotFound}, oldDir, true},
		{
			"self leader: barrier then local newer incarnation: remove",
			&fakeLeaderView{leaderID: "narad-1", localTopic: liveNew},
			&fakeTopicGetter{status: http.StatusNotFound}, oldDir, false,
		},
		{
			"self leader: barrier failure: keep",
			&fakeLeaderView{leaderID: "narad-1", localTopic: liveNew, barrierErr: context.DeadlineExceeded},
			&fakeTopicGetter{status: http.StatusNotFound}, oldDir, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := keepTopicDir(context.Background(), tc.view, tc.peer, "narad-1", tc.c, log); got != tc.keep {
				t.Fatalf("keepTopicDir() = %v, want %v", got, tc.keep)
			}
		})
	}
}
