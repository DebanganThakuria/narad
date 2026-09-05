package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestJoinOnlyNode(t *testing.T) {
	cases := []struct {
		name    string
		nodeID  string
		initial []string
		want    bool
	}{
		{"empty list: every node bootstraps (legacy/static)", "narad-3", nil, false},
		{"initial member bootstraps", "narad-1", []string{"narad-0", "narad-1", "narad-2"}, false},
		{"scale-out node joins", "narad-3", []string{"narad-0", "narad-1", "narad-2"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinOnlyNode(tc.nodeID, tc.initial); got != tc.want {
				t.Fatalf("joinOnlyNode(%q, %v) = %v, want %v", tc.nodeID, tc.initial, got, tc.want)
			}
		})
	}
}

func TestPeerMemberAddr(t *testing.T) {
	cases := []struct {
		peerClusterAddr string
		httpAddr        string
		want            string
	}{
		{"narad-0.narad-headless.narad.svc.cluster.local:7943", ":7942", "narad-0.narad-headless.narad.svc.cluster.local:7942"},
		{"10.0.0.9:7943", "0.0.0.0:7942", "10.0.0.9:7942"},
		{"no-port-here", ":7942", ""},
		{"host:7943", "", ""},
	}
	for _, tc := range cases {
		if got := peerMemberAddr(tc.peerClusterAddr, tc.httpAddr); got != tc.want {
			t.Fatalf("peerMemberAddr(%q, %q) = %q, want %q", tc.peerClusterAddr, tc.httpAddr, got, tc.want)
		}
	}
}

// fakeJoiner scripts JoinCluster answers per peer address and records
// the requests it saw.
type fakeJoiner struct {
	mu      sync.Mutex
	answers map[string]nodewire.Response // addr → response; missing = transport error
	calls   []nodewire.JoinClusterRequest
	addrs   []string
}

func (f *fakeJoiner) JoinCluster(_ context.Context, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	f.addrs = append(f.addrs, addr)
	res, ok := f.answers[addr]
	if !ok {
		return nodewire.Response{}, errors.New("connection refused")
	}
	return res, nil
}

func (f *fakeJoiner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fakeLeaderWatcher struct{ leader atomic.Value }

func (f *fakeLeaderWatcher) LeaderID() string {
	v, _ := f.leader.Load().(string)
	return v
}

func joinTestConfig() *config.Config {
	cfg := config.Default()
	cfg.HTTP.Addr = ":7942"
	cfg.Cluster.Addr = ":7943"
	cfg.Cluster.Peers = []config.ClusterPeer{
		{ID: "narad-0", Addr: "10.0.0.10:7943"},
		{ID: "narad-1", Addr: "10.0.0.11:7943"},
		{ID: "narad-2", Addr: "10.0.0.12:7943"},
	}
	cfg.Cluster.InitialMembers = []string{"narad-0", "narad-1", "narad-2"}
	return cfg
}

func TestExistingClusterAnswers(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	cases := []struct {
		name    string
		answers map[string]nodewire.Response
		want    bool
	}{
		{"nobody listening: bootstrap", nil, false},
		{"peers without a configuration: bootstrap", map[string]nodewire.Response{
			"10.0.0.11:7942": {Status: http.StatusPreconditionFailed},
			"10.0.0.12:7942": {Status: http.StatusPreconditionFailed},
		}, false},
		{"a configured non-leader answers: join", map[string]nodewire.Response{
			"10.0.0.11:7942": {Status: http.StatusMisdirectedRequest},
		}, true},
		{"the leader admits: join", map[string]nodewire.Response{
			"10.0.0.12:7942": {Status: http.StatusOK},
		}, true},
		{"the leader refuses a removed id: the cluster exists, join", map[string]nodewire.Response{
			"10.0.0.12:7942": {Status: http.StatusConflict},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &fakeJoiner{answers: tc.answers}
			if got := existingClusterAnswers(context.Background(), j, cfg, "narad-0", log); got != tc.want {
				t.Fatalf("existingClusterAnswers() = %v, want %v", got, tc.want)
			}
			for _, req := range j.calls {
				if !req.Fresh || req.ID != "narad-0" {
					t.Fatalf("probe request = %+v, want Fresh=true for narad-0", req)
				}
			}
			for _, addr := range j.addrs {
				if addr == "10.0.0.10:7942" {
					t.Fatal("probed itself")
				}
			}
		})
	}
}

func TestRunClusterJoinSkipsUnconfiguredPeers(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	watcher := &fakeLeaderWatcher{}
	j := &fakeJoiner{answers: map[string]nodewire.Response{
		"10.0.0.11:7942": {Status: http.StatusPreconditionFailed},
		"10.0.0.12:7942": {Status: http.StatusOK},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoin(ctx, watcher, j, cfg, "narad-3", true, log)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for j.callCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if j.callCount() < 3 {
		t.Fatalf("join loop stopped before reaching the leader (calls=%d)", j.callCount())
	}
	// Admission: the loop exits on the first leader sighting.
	watcher.leader.Store("narad-2")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("join loop did not exit after the leader appeared")
	}
	cancel()
	// Walk order: unreachable peer, 412 peer, then the leader.
	if j.addrs[0] != "10.0.0.10:7942" || j.addrs[1] != "10.0.0.11:7942" || j.addrs[2] != "10.0.0.12:7942" {
		t.Fatalf("peer walk = %v, want unreachable, 412 peer, then leader", j.addrs)
	}
	if !j.calls[0].Fresh {
		t.Fatal("join request did not carry Fresh=true")
	}
}

// An initial member that has state but never sees a leader (it was
// Raft-removed, or its peers were replaced) must run the join loop after
// a bounded wait instead of sitting leaderless forever; a node that has a
// leader must never issue a join.
func TestRunClusterJoinWhenLeaderlessRunsAfterBoundedWait(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	old := leaderlessJoinDelay
	leaderlessJoinDelay = 300 * time.Millisecond
	t.Cleanup(func() { leaderlessJoinDelay = old })

	t.Run("leader in view: no join", func(t *testing.T) {
		watcher := &fakeLeaderWatcher{}
		watcher.leader.Store("narad-1")
		j := &fakeJoiner{}
		ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
		defer cancel()
		runClusterJoinWhenLeaderless(ctx, watcher, j, cfg, "narad-0", false, false, log)
		if j.callCount() != 0 {
			t.Fatalf("issued %d joins with a leader in view", j.callCount())
		}
	})

	t.Run("leaderless past the delay: joins", func(t *testing.T) {
		watcher := &fakeLeaderWatcher{}
		j := &fakeJoiner{answers: map[string]nodewire.Response{"10.0.0.11:7942": {Status: http.StatusConflict}}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			runClusterJoinWhenLeaderless(ctx, watcher, j, cfg, "narad-0", false, false, log)
		}()
		deadline := time.Now().Add(5 * time.Second)
		for j.callCount() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if j.callCount() == 0 {
			t.Fatal("join loop never ran on a leaderless node")
		}
		if j.calls[0].Fresh {
			t.Fatal("a node with prior state declared itself fresh")
		}
		cancel()
		<-done
	})

	t.Run("single node: nothing to join", func(t *testing.T) {
		single := config.Default()
		j := &fakeJoiner{}
		runClusterJoinWhenLeaderless(context.Background(), &fakeLeaderWatcher{}, j, single, "solo", false, true, log)
		if j.callCount() != 0 {
			t.Fatal("single-node deployment issued a join")
		}
	})
}

func TestBootstrapPeersLimitedToInitialMembers(t *testing.T) {
	peers := []config.ClusterPeer{
		{ID: "narad-0", Addr: "10.0.0.10:7943"},
		{ID: "narad-1", Addr: "10.0.0.11:7943"},
		{ID: "narad-2", Addr: "10.0.0.12:7943"},
		{ID: "narad-3", Addr: "10.0.0.13:7943"},
		{ID: "narad-4", Addr: "10.0.0.14:7943"},
	}
	got := bootstrapPeers("narad-0", ":7943", peers, []string{"narad-0", "narad-1", "narad-2"})
	want := []metastore.Peer{{ID: "narad-1", Addr: "10.0.0.11:7943"}, {ID: "narad-2", Addr: "10.0.0.12:7943"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bootstrapPeers = %v, want %v", got, want)
	}
	// No initial-members list: legacy behaviour, every peer is a voter.
	if got := bootstrapPeers("narad-0", ":7943", peers, nil); len(got) != 4 {
		t.Fatalf("bootstrapPeers without initial members = %v, want all 4 peers", got)
	}
}
