package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// syncLog is a goroutine-safe log sink.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// auditLines returns the audit lines with the event.
func (l *syncLog) auditLines(event string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(l.buf.String(), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["component"] == "audit" && m["event"] == event {
			out = append(out, m)
		}
	}
	return out
}

// regNode is one member of a three-node cluster with the real remote
// plane served over QUIC.
type regNode struct {
	id      string
	store   *metastore.Store
	server  *RPCServer
	plane   *RemotePlane
	svc     *remote.Service
	cache   *remote.Cache
	logs    *syncLog
	quic    string
	raft    string
	posture remote.Posture
}

// regTarget answers the checks like a secured Narad target.
func regTarget(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	jsonErr := func(w http.ResponseWriter, status int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); !ok {
			jsonErr(w, http.StatusUnauthorized, "authentication required")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"orders","id":"target-id"}`))
	})
	mux.HandleFunc("GET /v1/topics/{topic}/children", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"parent":"orders","parent_id":"target-id","children":[]}`))
	})
	mux.HandleFunc("POST /v1/topics/{topic}/produce/batch", func(w http.ResponseWriter, _ *http.Request) {
		jsonErr(w, http.StatusBadRequest, "messages required")
	})
	mux.HandleFunc("GET /v1/users", func(w http.ResponseWriter, _ *http.Request) {
		jsonErr(w, http.StatusForbidden, "admin privileges required")
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
}

func freeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	return addr
}

// startRegCluster boots three metastores in one Raft cluster, serves
// each node's RPC server over QUIC, registers the members, and builds
// every node's outbound plane. postures overrides a node's posture;
// handlers overrides the QUIC handler of a node (an older release).
func startRegCluster(t *testing.T, secret string, targetPort int, postures map[string]remote.Posture, handlers map[string]func(*RPCServer) clusterrpc.StreamFrameHandler) []*regNode {
	t.Helper()
	ids := []string{"n0", "n1", "n2"}
	raftAddrs := map[string]string{}
	for _, id := range ids {
		raftAddrs[id] = freeTCPAddr(t)
	}
	base := t.TempDir()
	nodes := make([]*regNode, 0, len(ids))
	for _, id := range ids {
		var peers []metastore.Peer
		for _, p := range ids {
			if p != id {
				peers = append(peers, metastore.Peer{ID: p, Addr: raftAddrs[p]})
			}
		}
		store, err := metastore.New(metastore.Config{
			NodeID: id, DataDir: filepath.Join(base, id), BindAddr: raftAddrs[id], AdvertiseAddr: raftAddrs[id], Peers: peers,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		n := &regNode{id: id, store: store, logs: &syncLog{}, quic: freeUDPAddr(t), raft: raftAddrs[id], posture: remote.Posture{SecurityEnabled: true, APIHopEncrypted: true, RaftTLS: true}}
		if p, ok := postures[id]; ok {
			n.posture = p
		}
		nodes = append(nodes, n)
	}
	stores := map[string]*metastore.Store{}
	for _, n := range nodes {
		stores[n.id] = n.store
	}
	_, leader := waitForClusterLeader(t, stores)
	for _, n := range nodes {
		if err := leader.RegisterMember(context.Background(), metastore.Member{ID: n.id, Addr: n.quic, ClusterAddr: n.raft, Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	// The actor of every registry write these tests send: the leader
	// re-authorizes it against its own replica.
	rigSeedUser(t, leader, "alice", rigRandomString(18), user.Grant{Action: user.ActionAdmin}, false)
	for _, n := range nodes {
		for _, m := range ids {
			waitForMember(t, n.store, m)
		}
		regWaitFor(t, func() bool { _, err := n.store.GetUser(context.Background(), "alice"); return err == nil })
	}
	ctx, cancel := context.WithCancel(context.Background())
	var served sync.WaitGroup
	t.Cleanup(func() { cancel(); served.Wait() })
	for _, n := range nodes {
		log := slog.New(slog.NewJSONHandler(n.logs, nil))
		guard, err := remote.NewGuard(remote.GuardConfig{AllowedPorts: []int{targetPort}, AllowAddresses: []string{"127.0.0.0/8"}, AllowedHosts: []string{"127.0.0.1"}})
		if err != nil {
			t.Fatal(err)
		}
		secrets := remote.Secrets{Current: secret}
		n.cache = remote.NewCache(remote.CacheConfig{Registry: n.store, Secrets: secrets, Guard: guard, Posture: n.posture, Log: log})
		n.svc = remote.NewService(remote.ServiceConfig{Store: n.store, Cache: n.cache, Guard: guard, Secrets: secrets, Posture: n.posture, NodeID: n.id, Log: log, WritesPerMinute: 1000})
		router := NewRouter(n.store, n.id, partition.NewHashRoundRobin(), "")
		peer := NewPeerClient(5*time.Second, "")
		t.Cleanup(func() { _ = peer.Close() })
		router.SetPeerClient(peer)
		n.plane = NewRemotePlane(n.store, router, peer, n.id, log)
		reg := NewRemoteRegistry(RemoteRegistryDeps{Store: n.store, Service: n.svc, Plane: n.plane, Log: log, SelfID: n.id})
		n.plane.Registry, n.plane.Checks = reg, reg
		n.svc.SetCluster(reg)
		n.server = NewRPCServer(nil, n.store, log)
		n.server.SetRemotePlane(n.plane)
		var h clusterrpc.StreamFrameHandler = n.server
		if wrap, ok := handlers[n.id]; ok {
			h = wrap(n.server)
		}
		served.Go(func() { _ = clusterrpc.ServeQUIC(ctx, n.quic, "", nil, h) })
	}
	// Every member's RPC listener answers before the test's first
	// fan-out, so a slow start under load is not read as a member down.
	deadline := time.Now().Add(30 * time.Second)
	for {
		answered := true
		for _, ms := range nodes[0].plane.Checks.(*RemoteRegistry).StatusEverywhere(context.Background()) {
			answered = answered && ms.Class != remote.ClassUnreachable
		}
		if answered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a member's RPC listener did not answer within 30s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nodes
}

// olderRelease answers the two remote ops the way a release without
// them does, and serves everything else.
type olderRelease struct{ inner *RPCServer }

func (o olderRelease) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if op, err := nodewire.OperationOf(frame.Payload); err == nil && (op == nodewire.OpRemoteWrite || op == nodewire.OpRemoteCheck) {
		res := errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", op))
		payload, _ := nodewire.EncodeResponse(res)
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: payload})
		return true
	}
	return o.inner.HandleStreamFrame(frame, respond)
}

func leaderAndFollower(t *testing.T, nodes []*regNode) (leader, follower *regNode) {
	t.Helper()
	for _, n := range nodes {
		if n.store.IsLeader() {
			leader = n
		} else if follower == nil {
			follower = n
		}
	}
	if leader == nil || follower == nil {
		t.Fatal("no leader or follower")
	}
	return leader, follower
}

// clusterSecret is a cluster secret as `openssl rand -base64 32` prints
// it, drawn again in the rare case the strength rule refuses it.
func clusterSecret(t *testing.T) string {
	t.Helper()
	for {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		if s := base64.StdEncoding.EncodeToString(b); remotecred.CheckSecretStrength(s) == nil {
			return s
		}
	}
}

// createThrough runs a create the way the handler does: sealed on the
// ingress node, forwarded (or applied in process) through its plane.
func createThrough(t *testing.T, ingress *regNode, name, url, ca, requestID string) nodewire.Response {
	t.Helper()
	sealed, err := ingress.svc.PrepareCreate(context.Background(), remote.CreateRequest{
		Name: name, URL: url, Username: "repl", Password: domremote.NewSecret([]byte("password-0123456789-abcdef")), CAPEM: ca,
	})
	if err != nil {
		t.Fatalf("PrepareCreate: %v", err)
	}
	body, _ := json.Marshal(metastore.PutRemoteOp{Record: sealed.Record, Salt: sealed.Salt, SealedAtMs: sealed.SealedAtMs})
	res, err := ingress.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubCreate, Actor: "alice", RequestID: requestID, Body: body})
	if err != nil {
		t.Fatalf("RemoteWrite: %v", err)
	}
	return res
}

func targetPort(t *testing.T, srv *httptest.Server) int {
	t.Helper()
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "https://"))
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A create entered on a follower and one entered on the leader each
// give exactly one leader audit line, on the leader, joined to the
// request ID; the record reaches every replica, every node's cache
// opens it once, and the checks pass from every member.
func TestRemoteRegistryThreeNodes(t *testing.T) {
	target, ca := regTarget(t)
	nodes := startRegCluster(t, clusterSecret(t), targetPort(t, target), nil, nil)
	leader, follower := leaderAndFollower(t, nodes)

	res := createThrough(t, follower, "b", target.URL, ca, "req-follower-0001")
	if res.Status != http.StatusCreated {
		t.Fatalf("create through a follower: %d %s", res.Status, res.Body)
	}
	// Read-your-writes on the ingress replica.
	if _, err := follower.store.GetRemote("b"); err != nil {
		t.Fatalf("the follower's replica does not show the create: %v", err)
	}
	res = createThrough(t, leader, "c", target.URL, ca, "req-leader-00002")
	if res.Status != http.StatusCreated {
		t.Fatalf("create through the leader: %d %s", res.Status, res.Body)
	}
	for _, n := range nodes {
		lines := n.logs.auditLines(nodewire.RemoteSubCreate)
		want := 0
		if n == leader {
			want = 2
		}
		if len(lines) != want {
			t.Fatalf("node %s wrote %d leader lines, want %d", n.id, len(lines), want)
		}
		for _, l := range lines {
			if l["outcome"] != "committed" || l["actor"] != "alice" || (l["request_id"] != "req-follower-0001" && l["request_id"] != "req-leader-00002") {
				t.Fatalf("leader line = %v", l)
			}
		}
	}

	// Every replica holds both; every cache opens each once.
	deadline := time.Now().Add(5 * time.Second)
	for _, n := range nodes {
		for {
			n.cache.Refresh()
			_, eb := n.cache.Get("b")
			_, ec := n.cache.Get("c")
			if eb == nil && ec == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s cache: %v %v", n.id, eb, ec)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	rec, _ := leader.store.GetRemote("b")
	reports, err := leader.plane.Checks.CheckEverywhere(context.Background(), remote.CheckRequest{Remote: "b", Topic: "orders", CredentialVersion: rec.CredentialVersion})
	if err != nil {
		t.Fatal(err)
	}
	if id, err := remote.Verdict(reports, rec.CredentialVersion); err != nil || id != "target-id" || len(reports) != 3 {
		t.Fatalf("checks from every member: %v %v %+v", id, err, reports)
	}
	for _, r := range reports {
		if r.RTTMs == nil {
			t.Fatalf("node %s reported no rtt although the allowlist is set", r.Node)
		}
	}
	// The per-remote check limit lives on the member that runs it.
	again, _ := leader.plane.Checks.CheckEverywhere(context.Background(), remote.CheckRequest{Remote: "b", Topic: "orders", CredentialVersion: rec.CredentialVersion})
	if _, err := remote.Verdict(again, rec.CredentialVersion); err == nil {
		t.Fatal("a second check within 5 s was not throttled")
	} else if ce := (*remote.CheckError)(nil); !errors.As(err, &ce) || ce.Status != http.StatusTooManyRequests {
		t.Fatalf("second check: %v", err)
	}

	// Status from every member.
	statuses := follower.svc.Cluster().StatusEverywhere(context.Background())
	if len(statuses) != 3 {
		t.Fatalf("statuses = %+v", statuses)
	}
	for _, s := range statuses {
		if s.Report == nil || len(s.Report.Remotes) != 2 || s.Report.Remotes[0].State != remote.StateReady {
			t.Fatalf("status of %s = %+v", s.Node, s.Report)
		}
	}

	// A delete forwarded from the follower.
	body, _ := json.Marshal(metastore.DeleteRemoteOp{Name: "c"})
	res, err = follower.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubDelete, Actor: "alice", RequestID: "req-delete-00003", Body: body})
	if err != nil || res.Status != http.StatusNoContent {
		t.Fatalf("delete: %v %d %s", err, res.Status, res.Body)
	}
	if lines := leader.logs.auditLines(nodewire.RemoteSubDelete); len(lines) != 1 || lines[0]["outcome"] != "committed" {
		t.Fatalf("delete lines = %v", lines)
	}
}

// The upgrade gate: every member, dead ones included, must answer on
// this release with a posture that allows remotes.
func TestRemoteRegistryUpgradeGate(t *testing.T) {
	target, ca := regTarget(t)
	secret := clusterSecret(t)
	port := targetPort(t, target)

	t.Run("legacy cluster auth on one member", func(t *testing.T) {
		nodes := startRegCluster(t, secret, port, map[string]remote.Posture{"n2": {SecurityEnabled: true, LegacyClusterAuth: true, APIHopEncrypted: true}}, nil)
		leader, _ := leaderAndFollower(t, nodes)
		ingress := leader
		if ingress.id == "n2" {
			ingress = nodes[0]
		}
		res := createThrough(t, ingress, "b", target.URL, ca, "r1")
		var body struct {
			Error   string   `json:"error"`
			Members []string `json:"members"`
		}
		_ = json.Unmarshal(res.Body, &body)
		if res.Status != http.StatusPreconditionFailed || len(body.Members) != 1 || body.Members[0] != "n2" || !strings.Contains(body.Error, "posture") {
			t.Fatalf("legacy auth member: %d %s", res.Status, res.Body)
		}
		if lines := leader.logs.auditLines(nodewire.RemoteSubCreate); len(lines) != 1 || lines[0]["outcome"] != "refused" {
			t.Fatalf("refused line = %v", lines)
		}
	})

	t.Run("a member on an older release", func(t *testing.T) {
		nodes := startRegCluster(t, secret, port, nil, map[string]func(*RPCServer) clusterrpc.StreamFrameHandler{
			"n2": func(s *RPCServer) clusterrpc.StreamFrameHandler { return olderRelease{inner: s} },
		})
		leader, _ := leaderAndFollower(t, nodes)
		if leader.id == "n2" {
			t.Skip("the older member won the election; the gate is exercised when it follows")
		}
		// An older release reports no entry types in its heartbeat.
		older, err := leader.store.GetMember("n2")
		if err != nil {
			t.Fatal(err)
		}
		older.Build, older.EntryTypes = "", 0
		if err := leader.store.RegisterMember(context.Background(), older); err != nil {
			t.Fatal(err)
		}
		res := createThrough(t, leader, "b", target.URL, ca, "r1")
		if res.Status != http.StatusPreconditionFailed || !strings.Contains(string(res.Body), `"n2"`) {
			t.Fatalf("older member: %d %s", res.Status, res.Body)
		}
		// The release gate alone (remote delete, remote child pause and
		// skip) refuses an older member too: it reads the member records.
		var pe *PostureError
		if err := leader.plane.Checks.RequireReleases(context.Background()); !errors.As(err, &pe) ||
			!errors.Is(err, errs.ErrRemoteFeatureGate) || len(pe.Members) != 1 || pe.Members[0] != "n2" {
			t.Fatalf("relaxed gate with an older member: %v", err)
		}
	})

	t.Run("a dead member blocks writes but not a delete", func(t *testing.T) {
		nodes := startRegCluster(t, secret, port, nil, nil)
		leader, _ := leaderAndFollower(t, nodes)
		if res := createThrough(t, leader, "b", target.URL, ca, "r0"); res.Status != http.StatusCreated {
			t.Fatalf("create: %d %s", res.Status, res.Body)
		}
		if err := leader.store.RegisterMember(context.Background(), metastore.Member{ID: "n9", Addr: "127.0.0.1:1", Status: metastore.MemberDead, Build: "narad test", EntryTypes: metastore.MaxEntryType}); err != nil {
			t.Fatal(err)
		}
		res := createThrough(t, leader, "c", target.URL, ca, "r1")
		if res.Status != http.StatusPreconditionFailed || !strings.Contains(string(res.Body), `"n9"`) {
			t.Fatalf("dead member: %d %s", res.Status, res.Body)
		}
		if err := leader.plane.Checks.RequireReleases(context.Background()); err != nil {
			t.Fatalf("relaxed gate with a dead member: %v, want nil", err)
		}
		body, _ := json.Marshal(metastore.DeleteRemoteOp{Name: "b", Force: true})
		res, err := leader.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubDelete, Actor: "alice", RequestID: "r2", Body: body})
		if err != nil || res.Status != http.StatusNoContent {
			t.Fatalf("emergency delete with a member down: %v %d %s", err, res.Status, res.Body)
		}
	})
}

// A forward to a leader that predates the remote ops answers
// ErrLeaderTooOld, which the HTTP layer maps to 412.
func TestRemoteWriteToAnOlderLeader(t *testing.T) {
	target, ca := regTarget(t)
	nodes := startRegCluster(t, clusterSecret(t), targetPort(t, target), nil, nil)
	leader, follower := leaderAndFollower(t, nodes)
	// Stand an older release up at the leader's member address.
	older := NewRPCServer(nil, leader.store, nil)
	addr := freeUDPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = clusterrpc.ServeQUIC(ctx, addr, "", nil, olderRelease{inner: older}) }()
	t.Cleanup(func() { cancel(); <-done })
	if err := leader.store.RegisterMember(context.Background(), metastore.Member{ID: leader.id, Addr: addr, ClusterAddr: leader.raft, Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType}); err != nil {
		t.Fatal(err)
	}
	regWaitFor(t, func() bool { m, err := follower.store.GetMember(leader.id); return err == nil && m.Addr == addr })
	sealed, err := follower.svc.PrepareCreate(context.Background(), remote.CreateRequest{
		Name: "b", URL: target.URL, Username: "repl", Password: domremote.NewSecret([]byte("password-0123456789-abcdef")), CAPEM: ca,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(metastore.PutRemoteOp{Record: sealed.Record, Salt: sealed.Salt})
	_, err = follower.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubCreate, Body: body})
	if !errors.Is(err, ErrLeaderTooOld) || !errors.Is(err, errs.ErrRemoteFeatureGate) {
		t.Fatalf("forward to an older leader: %v", err)
	}
}

// noAppliedIndex refuses the applied-index probe, the way a leader
// that lost its leadership right after the commit, or a probe that
// times out, leaves the forwarding node without a bound to wait for.
type noAppliedIndex struct{ inner *RPCServer }

func (n noAppliedIndex) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if op, err := nodewire.OperationOf(frame.Payload); err == nil && op == nodewire.OpAppliedIndex {
		payload, _ := nodewire.EncodeResponse(errorResponse(http.StatusServiceUnavailable, "not the leader"))
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: payload})
		return true
	}
	return n.inner.HandleStreamFrame(frame, respond)
}

// A remote write the leader committed, forwarded by a node that cannot
// confirm its own replica applied it, returns the leader's answer with
// errs.ErrNotAppliedHere instead of a bare 2xx the client would read
// as safe to read back here.
func TestRemoteWriteReportsAnUnsettledApply(t *testing.T) {
	target, ca := regTarget(t)
	refuse := func(s *RPCServer) clusterrpc.StreamFrameHandler { return noAppliedIndex{inner: s} }
	nodes := startRegCluster(t, clusterSecret(t), targetPort(t, target), nil, map[string]func(*RPCServer) clusterrpc.StreamFrameHandler{"n0": refuse, "n1": refuse, "n2": refuse})
	leader, follower := leaderAndFollower(t, nodes)

	sealed, err := follower.svc.PrepareCreate(context.Background(), remote.CreateRequest{
		Name: "b", URL: target.URL, Username: "repl", Password: domremote.NewSecret([]byte("password-0123456789-abcdef")), CAPEM: ca,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(metastore.PutRemoteOp{Record: sealed.Record, Salt: sealed.Salt, SealedAtMs: sealed.SealedAtMs})
	res, err := follower.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubCreate, Actor: "alice", RequestID: "req-unsettled-01", Body: body})
	if !errors.Is(err, errs.ErrNotAppliedHere) || res.Status != http.StatusCreated {
		t.Fatalf("forwarded create without an applied index: %d, %v; want the leader's 201 with ErrNotAppliedHere", res.Status, err)
	}
	if _, err := leader.store.GetRemote("b"); err != nil {
		t.Fatalf("the leader did not commit the create: %v", err)
	}

	// On the leader nothing is forwarded: raft Apply already waited for
	// its own replica.
	if _, err := leader.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubDelete, Actor: "alice", RequestID: "req-unsettled-02", Body: []byte(`{"name":"b"}`)}); err != nil {
		t.Fatalf("a write on the leader: %v", err)
	}
}

func regWaitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
