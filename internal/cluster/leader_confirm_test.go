package cluster

// Discarding a WAL record destroys a 202-acked produce, so absence must
// be confirmed by the leader; every failure mode keeps the record.

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

func TestTopicAbsentOnLeader(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	leader := fakeFanoutLeaderView{leaderID: "narad-9", member: metastore.Member{ID: "narad-9", Addr: "10.0.0.9:7943"}}

	cases := []struct {
		name   string
		view   fakeFanoutLeaderView
		peer   *fakeTopicFetcher
		selfID string
		want   bool
	}{
		{"no cluster identity: local absence is authoritative", fakeFanoutLeaderView{}, &fakeTopicFetcher{}, "", true},
		{"no leader known keeps the records", fakeFanoutLeaderView{}, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false},
		{"self is leader confirms without an RPC", fakeFanoutLeaderView{leaderID: "narad-1"}, &fakeTopicFetcher{}, "narad-1", true},
		{
			"leader member unresolvable keeps the records",
			fakeFanoutLeaderView{leaderID: "narad-9", memberErr: errors.New("nope")},
			&fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", false,
		},
		{"leader unreachable keeps the records", leader, &fakeTopicFetcher{err: errors.New("timeout")}, "narad-1", false},
		{"leader has the topic keeps the records", leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusOK}}, "narad-1", false},
		{"leader 5xx keeps the records", leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusInternalServerError}}, "narad-1", false},
		{"leader confirms absence allows the discard", leader, &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}, "narad-1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := topicAbsentOnLeader(context.Background(), tc.view, tc.peer, tc.selfID, "bench", log)
			if got != tc.want {
				t.Fatalf("topicAbsentOnLeader() = %v, want %v", got, tc.want)
			}
		})
	}

	for _, selfID := range []string{"", "narad-1"} {
		peer := &fakeTopicFetcher{res: nodewire.Response{Status: http.StatusNotFound}}
		topicAbsentOnLeader(context.Background(), fakeFanoutLeaderView{leaderID: "narad-1"}, peer, selfID, "bench", log)
		if peer.calls != 0 {
			t.Fatalf("selfID %q issued %d RPCs, want 0", selfID, peer.calls)
		}
	}
}
