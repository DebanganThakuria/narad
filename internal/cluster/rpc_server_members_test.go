package cluster

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A heartbeat a follower forwards to the leader records the build and
// Raft entry types it reports, which is how the leader learns what every
// member applies. A heartbeat in the v3.0.1 frame records neither.
func TestForwardedHeartbeatRecordsBuildAndEntryTypes(t *testing.T) {
	stores := newTestStoreCluster(t, "node-a")
	leader := stores["node-a"]
	waitStoreLeader(t, leader)
	server := NewRPCServer(nil, leader, slog.New(slog.NewTextHandler(io.Discard, nil)))

	send := func(req nodewire.MemberRequest) {
		t.Helper()
		payload, err := nodewire.EncodeMemberRequest(req)
		if err != nil {
			t.Fatalf("encode member request: %v", err)
		}
		if res := server.handleRegisterMember(payload); res.Status != http.StatusNoContent {
			t.Fatalf("register member status = %d %s, want 204", res.Status, res.Body)
		}
	}
	base := nodewire.MemberRequest{ID: "node-b", Addr: "10.0.0.2:7942", ClusterAddr: "10.0.0.2:7943", Status: "alive", LastHeartbeat: 1}

	upgraded := base
	upgraded.Build, upgraded.EntryTypes = "narad v3.1.0", metastore.MaxEntryType
	send(upgraded)
	got, err := leader.GetMember("node-b")
	if err != nil || got.Build != "narad v3.1.0" || got.EntryTypes != metastore.MaxEntryType {
		t.Fatalf("member after an upgraded heartbeat = %+v, %v; want build narad v3.1.0 and entry types %d", got, err, metastore.MaxEntryType)
	}

	send(base)
	got, err = leader.GetMember("node-b")
	if err != nil || got.Build != "" || got.EntryTypes != 0 {
		t.Fatalf("member after a v3.0.1 heartbeat = %+v, %v; want build and entry types cleared", got, err)
	}
}
