package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strings"
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

// notLeader is a 421 join answer naming leaderAddr as the leader's
// node-RPC address.
func notLeader(leaderAddr string) nodewire.Response {
	return nodewire.Response{Status: http.StatusMisdirectedRequest, Body: []byte(`{"error":"not the metastore leader","leader_id":"narad-x","leader_addr":"` + leaderAddr + `"}`)}
}

// joinAddrs returns a copy of the addresses the fake joiner was asked.
func (f *fakeJoiner) joinAddrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.addrs)
}

// waitJoinCalls waits up to d for the fake joiner to have seen at least
// n requests and returns the addresses asked so far.
func waitJoinCalls(f *fakeJoiner, n int, d time.Duration) []string {
	deadline := time.Now().Add(d)
	for f.callCount() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return f.joinAddrs()
}

// The pinned peers (cluster.peers, which the chart pins to the first
// pods) are all followers once leadership has moved to a node that
// joined later. Each answers 421 naming the leader, and the joiner asks
// the leader within the same attempt instead of retrying 421 until
// leadership happens to move back.
func TestJoinReachesALeaderOutsideThePinnedPeers(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	const leaderAddr = "10.0.0.14:7942"
	j := &fakeJoiner{answers: map[string]nodewire.Response{
		"10.0.0.10:7942": notLeader(leaderAddr),
		"10.0.0.11:7942": notLeader(leaderAddr),
		"10.0.0.12:7942": notLeader(leaderAddr),
		leaderAddr:       {Status: http.StatusOK, Body: []byte(`{"status":"staged"}`)},
	}}
	watcher := &fakeLeaderWatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoin(ctx, watcher, j, cfg, "narad-5", true, log)
	}()
	// One attempt: the first pinned peer, then the leader it names.
	addrs := waitJoinCalls(j, 2, clusterJoinRetryInterval/2)
	watcher.leader.Store("narad-4")
	cancel()
	<-done
	if !slices.Equal(addrs, []string{"10.0.0.10:7942", leaderAddr}) {
		t.Fatalf("first join attempt asked %v, want the first pinned peer and then the leader it named", addrs)
	}
}

// A walk follows at most four hints, so a chain of stale hints (each
// hinted node itself answering 421 with yet another address) cannot
// stretch one attempt without bound; the rest of the pinned peers are
// still asked.
func TestJoinFollowsABoundedNumberOfHints(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	chain := []string{"10.0.1.1:7942", "10.0.1.2:7942", "10.0.1.3:7942", "10.0.1.4:7942", "10.0.1.5:7942", "10.0.1.6:7942"}
	answers := map[string]nodewire.Response{
		"10.0.0.10:7942": notLeader(chain[0]),
		"10.0.0.11:7942": {Status: http.StatusPreconditionFailed},
		"10.0.0.12:7942": {Status: http.StatusPreconditionFailed},
	}
	for i := 0; i+1 < len(chain); i++ {
		answers[chain[i]] = notLeader(chain[i+1])
	}
	j := &fakeJoiner{answers: answers}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoin(ctx, &fakeLeaderWatcher{}, j, cfg, "narad-5", true, log)
	}()
	addrs := waitJoinCalls(j, 7, clusterJoinRetryInterval/2)
	cancel()
	<-done
	want := []string{"10.0.0.10:7942", chain[0], chain[1], chain[2], chain[3], "10.0.0.11:7942", "10.0.0.12:7942"}
	if len(addrs) < len(want) || !slices.Equal(addrs[:len(want)], want) {
		t.Fatalf("first join attempt asked %v, want %v (four hints followed, then the remaining pinned peers)", addrs, want)
	}
	if slices.Contains(addrs, chain[4]) {
		t.Fatalf("followed a fifth hint in one attempt: %v", addrs)
	}
}

// A 421 whose leader address is not a usable host:port is not followed;
// a well-formed hint from the next peer still is.
func TestJoinIgnoresAMalformedHint(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	j := &fakeJoiner{answers: map[string]nodewire.Response{
		"10.0.0.10:7942": notLeader("no-port"),
		"10.0.0.11:7942": notLeader("10.0.0.17:7942"),
		"10.0.0.17:7942": {Status: http.StatusOK},
	}}
	watcher := &fakeLeaderWatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoin(ctx, watcher, j, cfg, "narad-5", true, log)
	}()
	addrs := waitJoinCalls(j, 3, clusterJoinRetryInterval/2)
	watcher.leader.Store("narad-7")
	cancel()
	<-done
	if !slices.Equal(addrs, []string{"10.0.0.10:7942", "10.0.0.11:7942", "10.0.0.17:7942"}) {
		t.Fatalf("first join attempt asked %v, want both pinned peers and then the well-formed hint only", addrs)
	}

	for _, tc := range []struct{ body, want string }{
		{`{"error":"not the metastore leader","leader_id":"narad-4","leader_addr":"narad-4.narad-headless:7942"}`, "narad-4.narad-headless:7942"},
		{`{"error":"not the metastore leader","leader_id":"narad-4","leader_addr":"[fd00::4]:7942"}`, "[fd00::4]:7942"},
		{`{"error":"not the metastore leader"}`, ""},
		{`{"error":"not the metastore leader","leader_id":"narad-4","leader_addr":""}`, ""},
		{`{"leader_addr":"no-port"}`, ""},
		{`{"leader_addr":":7942"}`, ""},
		{`{"leader_addr":"narad-4:0"}`, ""},
		{`{"leader_addr":"narad-4:99999"}`, ""},
		{`{"leader_addr":"` + strings.Repeat("a", 300) + `:7942"}`, ""},
		{`not json`, ""},
	} {
		if got := joinHint([]byte(tc.body)); got != tc.want {
			t.Errorf("joinHint(%s) = %q, want %q", tc.body, got, tc.want)
		}
	}
}

// fakeStagedStore is a node's view of its own metastore while it is in
// the Raft configuration: a leader in view, whether the replica has
// caught up, and whether it is a voter yet.
type fakeStagedStore struct {
	fakeLeaderWatcher
	caughtUp atomic.Bool
	voter    atomic.Bool
}

func (f *fakeStagedStore) LocalVoter() (bool, error) { return f.voter.Load(), nil }
func (f *fakeStagedStore) AppliedCaughtUp() bool     { return f.caughtUp.Load() }

// A node staged as a non-voter, with a leader in view, asks for
// promotion (sends its join again) once its replica has caught up,
// keeps asking while the leader defers, and stops once it is a voter.
// This also covers a node that joined while it ran 3.0.x (which never
// asks) and is restarted on this release.
func TestStagedNodeAsksForPromotionOnceCaughtUp(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := joinTestConfig()
	store := &fakeStagedStore{}
	store.leader.Store("narad-1")
	j := &fakeJoiner{answers: map[string]nodewire.Response{
		"10.0.0.11:7942": {Status: http.StatusOK, Body: []byte(`{"status":"deferred","reason":"this node has led for 3s"}`)},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoinWhenLeaderless(ctx, store, j, cfg, "narad-3", false, false, log)
	}()

	time.Sleep(3 * clusterJoinRetryInterval / 2)
	if n := j.callCount(); n != 0 {
		t.Fatalf("sent %d join requests before the replica caught up", n)
	}
	store.caughtUp.Store(true)
	asked := func() int {
		n := 0
		for _, a := range j.joinAddrs() {
			if a == "10.0.0.11:7942" {
				n++
			}
		}
		return n
	}
	deadline := time.Now().Add(3 * clusterJoinRetryInterval)
	for asked() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := asked(); n < 2 {
		t.Fatalf("a caught-up staged node asked the leader for promotion %d times in %s, want it to keep asking while deferred", n, 3*clusterJoinRetryInterval)
	}
	j.mu.Lock()
	for _, req := range j.calls {
		if req.ID != "narad-3" || req.Fresh {
			j.mu.Unlock()
			t.Fatalf("promotion request = %+v, want narad-3 without Fresh", req)
		}
	}
	j.mu.Unlock()

	store.voter.Store(true)
	time.Sleep(clusterJoinRetryInterval / 2) // a request already in flight
	before := j.callCount()
	time.Sleep(3 * clusterJoinRetryInterval / 2)
	if after := j.callCount(); after != before {
		t.Fatalf("a voter kept asking for promotion: %d more requests", after-before)
	}
	cancel()
	<-done
}

// A staged node that loses sight of the leader for leaderlessJoinDelay
// (the cluster lost it, or it was removed) runs the join loop again.
func TestStagedNodeJoinsAgainWhenLeaderless(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	old := leaderlessJoinDelay
	leaderlessJoinDelay = 300 * time.Millisecond
	t.Cleanup(func() { leaderlessJoinDelay = old })
	store := &fakeStagedStore{}
	store.leader.Store("narad-1")
	j := &fakeJoiner{answers: map[string]nodewire.Response{"10.0.0.11:7942": {Status: http.StatusOK}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClusterJoinWhenLeaderless(ctx, store, j, joinTestConfig(), "narad-3", false, false, log)
	}()
	time.Sleep(2 * leaderlessJoinDelay)
	if n := j.callCount(); n != 0 {
		t.Fatalf("sent %d joins while not caught up and with a leader in view", n)
	}
	store.leader.Store("")
	deadline := time.Now().Add(5 * time.Second)
	for j.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if j.callCount() == 0 {
		t.Fatal("a staged node without a leader never ran the join loop again")
	}
	j.mu.Lock()
	fresh := j.calls[0].Fresh
	j.mu.Unlock()
	if fresh {
		t.Fatal("a node with prior state declared itself fresh")
	}
	cancel()
	<-done
}
