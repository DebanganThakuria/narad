package cluster_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpcluster "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/cluster"
)

// stubBroker satisfies handlers.New; the cluster handlers only touch the
// metastore, so any accidental broker call panics on the nil interface.
type stubBroker struct{ broker.Broker }

func newStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{
		NodeID: "cluster-0", DataDir: t.TempDir(),
		BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "secret-topic", Partitions: 1}); err == nil {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

func seededSet(t *testing.T) *handlers.Set {
	t.Helper()
	s := newStore(t)
	ctx := context.Background()
	if err := s.RegisterMember(ctx, metastore.Member{ID: "cluster-0", Addr: "10.0.0.7:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := s.AssignPartition(ctx, "secret-topic", 0, "cluster-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := s.SetAssignmentTarget(ctx, "secret-topic", 0, "cluster-1"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	return handlers.New(handlers.Deps{
		Broker: stubBroker{}, Metastore: s,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func asUser(r *http.Request, u user.User) *http.Request {
	return r.WithContext(security.WithIdentity(r.Context(), u))
}

// A principal holding a single produce grant must not be able to read
// member addresses, liveness, or the topic names of in-flight moves;
// admins (and dev mode with no identity) still can.
func TestClusterReadEndpointsAreAdminOnly(t *testing.T) {
	set := seededSet(t)
	lowPriv := user.User{Username: "lowpriv", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"only-this-topic"}}}}
	admin := user.User{Username: "root", Root: true}

	endpoints := []struct {
		name    string
		path    string
		handler http.HandlerFunc
		leak    string
	}{
		{"members", "/v1/cluster/members", httpcluster.Members(set), "10.0.0.7:7942"},
		{"moves", "/v1/cluster/moves", httpcluster.Moves(set), "secret-topic"},
		{"members detail", "/v1/cluster/members?detail=true", httpcluster.Members(set), "10.0.0.7:7942"},
		{"moves detail", "/v1/cluster/moves?detail=true", httpcluster.Moves(set), "secret-topic"},
	}
	for _, ep := range endpoints {
		t.Run(ep.name+"/non-admin", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, asUser(httptest.NewRequest(http.MethodGet, ep.path, nil), lowPriv))
			if res.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", res.Code, res.Body)
			}
			if strings.Contains(res.Body.String(), ep.leak) {
				t.Fatalf("403 body leaked %q: %s", ep.leak, res.Body)
			}
		})
		t.Run(ep.name+"/admin", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, asUser(httptest.NewRequest(http.MethodGet, ep.path, nil), admin))
			if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), ep.leak) {
				t.Fatalf("status = %d body = %s, want 200 containing %q", res.Code, res.Body, ep.leak)
			}
		})
		t.Run(ep.name+"/security-disabled", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, ep.path, nil))
			if res.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 with no identity (dev mode)", res.Code)
			}
		})
	}
}

func TestDecommissionIsAdminOnly(t *testing.T) {
	set := seededSet(t)
	lowPriv := user.User{Username: "lowpriv", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}}

	for _, tc := range []struct {
		method, target string
		handler        http.HandlerFunc
		pathValues     []string
	}{
		{http.MethodPost, "/v1/cluster/members/cluster-0/decommission", httpcluster.Decommission(set), []string{"id", "cluster-0"}},
		{http.MethodPost, "/v1/cluster/members/cluster-0/decommission?dry_run=true", httpcluster.Decommission(set), []string{"id", "cluster-0"}},
		{http.MethodDelete, "/v1/cluster/members/cluster-0/decommission", httpcluster.CancelDecommission(set), []string{"id", "cluster-0"}},
		{http.MethodPost, "/v1/cluster/moves/secret-topic/0/abort", httpcluster.AbortMove(set), []string{"topic", "secret-topic", "partition", "0"}},
	} {
		req := asUser(httptest.NewRequest(tc.method, tc.target, nil), lowPriv)
		for i := 0; i+1 < len(tc.pathValues); i += 2 {
			req.SetPathValue(tc.pathValues[i], tc.pathValues[i+1])
		}
		res := httptest.NewRecorder()
		tc.handler.ServeHTTP(res, req)
		if res.Code != http.StatusForbidden {
			t.Fatalf("%s %s: status = %d, want 403", tc.method, tc.target, res.Code)
		}
		if strings.Contains(res.Body.String(), "cluster-1") {
			t.Fatalf("%s %s: 403 body leaked the move target: %s", tc.method, tc.target, res.Body)
		}
	}
	if a, _ := set.Deps.Metastore.GetAssignment("secret-topic", 0); a.TargetID != "cluster-1" {
		t.Fatal("a non-admin request changed the move")
	}
	if m, _ := set.Deps.Metastore.GetMember("cluster-0"); m.Draining {
		t.Fatal("a non-admin request changed the drain flag")
	}
}

func adminRequest(method, target string, pathValues ...string) *http.Request {
	req := asUser(httptest.NewRequest(method, target, nil), user.User{Username: "root", Root: true})
	for i := 0; i+1 < len(pathValues); i += 2 {
		req.SetPathValue(pathValues[i], pathValues[i+1])
	}
	return req
}

func isDraining(t *testing.T, s *metastore.Store, id string) bool {
	t.Helper()
	m, err := s.GetMember(id)
	if err != nil {
		t.Fatalf("GetMember(%s): %v", id, err)
	}
	return m.Draining
}

func decodeBody[T any](t *testing.T, res *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(res.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", res.Body, err)
	}
	return v
}

// A decommission that could never complete safely is refused up front
// with 409 and every reason, and nothing is written: here the only voter
// (too few voters would be left, and nobody could take its partition),
// and a dead member that owns a partition.
func TestDecommissionPreflightRefusesWithReasons(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	ctx := context.Background()
	if err := ms.RegisterMember(ctx, metastore.Member{ID: "cluster-2", Addr: "10.0.0.9:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "dead-owned", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := ms.AssignPartition(ctx, "dead-owned", 0, "cluster-2"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MarkMemberDead(ctx, "cluster-2"); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		id    string
		codes []string
	}{
		{"cluster-0", []string{controller.BlockedBelowMinVoters, controller.BlockedNoReceivers}},
		{"cluster-2", []string{controller.BlockedOwnerDead}},
	} {
		res := httptest.NewRecorder()
		httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/"+tc.id+"/decommission", "id", tc.id))
		if res.Code != http.StatusConflict {
			t.Fatalf("decommission %s: status %d (%s), want 409", tc.id, res.Code, res.Body)
		}
		body := decodeBody[controller.DecommissionRefusal](t, res)
		var codes []string
		for _, r := range body.Reasons {
			codes = append(codes, r.Code)
			if r.Message == "" {
				t.Fatalf("reason %s has no message", r.Code)
			}
		}
		if !slices.Equal(codes, tc.codes) || body.Error == "" {
			t.Fatalf("decommission %s refused with %v (error %q), want %v", tc.id, codes, body.Error, tc.codes)
		}
		if isDraining(t, ms, tc.id) {
			t.Fatalf("a refused decommission still marked %s draining", tc.id)
		}
	}
}

// ?dry_run=true says what a decommission would do and writes nothing,
// and a cancel cannot be dry-run.
func TestDecommissionDryRunWritesNothing(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	if err := ms.RegisterMember(context.Background(), metastore.Member{ID: "cluster-1", Addr: "10.0.0.8:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-1/decommission?dry_run=true", "id", "cluster-1"))
	if res.Code != http.StatusOK {
		t.Fatalf("dry run: status %d (%s), want 200", res.Code, res.Body)
	}
	got := decodeBody[httpcluster.DryRunView](t, res)
	if got.Member != "cluster-1" || !got.WouldDecommission || got.Voter || got.InboundMoves != 1 || got.Reasons == nil || len(got.Reasons) != 0 {
		t.Fatalf("dry run = %+v, want cluster-1 decommissionable, not a voter, one inbound move, no reasons", got)
	}
	if isDraining(t, ms, "cluster-1") {
		t.Fatal("a dry run marked cluster-1 draining")
	}

	res = httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-0/decommission?dry_run=1", "id", "cluster-0"))
	if got := decodeBody[httpcluster.DryRunView](t, res); res.Code != http.StatusOK || got.WouldDecommission || !got.Voter || got.OwnedPartitions != 1 || len(got.Reasons) != 1 || got.Reasons[0].Code != controller.BlockedBelowMinVoters {
		t.Fatalf("dry run of the only voter: status %d, %+v; want would_decommission false, below_min_voters", res.Code, got)
	}

	res = httptest.NewRecorder()
	httpcluster.CancelDecommission(set).ServeHTTP(res, adminRequest(http.MethodDelete, "/v1/cluster/members/cluster-1/decommission?dry_run=true", "id", "cluster-1"))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("dry-run cancel: status %d, want 400", res.Code)
	}
	res = httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-1/decommission?dry_run=maybe", "id", "cluster-1"))
	if res.Code != http.StatusBadRequest || isDraining(t, ms, "cluster-1") {
		t.Fatalf("dry_run=maybe: status %d, want 400 and nothing written", res.Code)
	}
}

// The members view shows each member's Raft role, how long ago its
// heartbeat was stamped, and why a draining member's decommission is
// blocked, without asking any other node.
func TestMembersViewShowsVoterLeaderHeartbeatAgeAndBlockedReason(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	ctx := context.Background()
	if err := ms.RegisterMember(ctx, metastore.Member{ID: "cluster-0", Addr: "10.0.0.7:7942", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Add(-42 * time.Second).Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := ms.SetMemberDraining(ctx, "cluster-0", true); err != nil {
		t.Fatal(err)
	}
	asked := 0
	set.Deps.NodeStatus = func(context.Context, metastore.Member) (nodewire.NodeStatus, error) {
		asked++
		return nodewire.NodeStatus{}, nil
	}

	res := httptest.NewRecorder()
	httpcluster.Members(set).ServeHTTP(res, adminRequest(http.MethodGet, "/v1/cluster/members"))
	if res.Code != http.StatusOK {
		t.Fatalf("status %d (%s)", res.Code, res.Body)
	}
	got := decodeBody[struct{ Members []httpcluster.MemberView }](t, res).Members
	if len(got) != 1 {
		t.Fatalf("members = %+v", got)
	}
	m := got[0]
	if !m.Voter || !m.Leader || m.HeartbeatAgeSeconds < 40 || m.HeartbeatAgeSeconds > 60 {
		t.Fatalf("member = %+v; want the voter and leader, heartbeat about 42 s old", m)
	}
	var codes []string
	for _, b := range m.DecommissionBlocked {
		codes = append(codes, b.Code)
	}
	if !slices.Equal(codes, []string{controller.BlockedNoReceivers, controller.BlockedBelowMinVoters}) {
		t.Fatalf("decommission_blocked = %v, want no_receivers and below_min_voters", codes)
	}
	if asked != 0 || m.NodeStatus != nil || m.StatusError != "" {
		t.Fatalf("the default view asked %d nodes for their status; it must make no node RPC", asked)
	}
}

// ?detail=true adds each member's own status, asked in parallel, and
// names a member that could not answer.
func TestMembersDetailIncludesNodeStatus(t *testing.T) {
	set := seededSet(t)
	if err := set.Deps.Metastore.RegisterMember(context.Background(), metastore.Member{ID: "cluster-1", Addr: "10.0.0.8:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	set.Deps.NodeStatus = func(_ context.Context, m metastore.Member) (nodewire.NodeStatus, error) {
		if m.ID == "cluster-1" {
			return nodewire.NodeStatus{}, errors.New("peer does not support node status (an older release)")
		}
		return nodewire.NodeStatus{Node: m.ID, DispatchBacklog: 4, Quarantine: nodewire.QuarantineStatus{Copies: 1}}, nil
	}

	res := httptest.NewRecorder()
	httpcluster.Members(set).ServeHTTP(res, adminRequest(http.MethodGet, "/v1/cluster/members?detail=true"))
	got := decodeBody[struct{ Members []httpcluster.MemberView }](t, res).Members
	if res.Code != http.StatusOK || len(got) != 2 {
		t.Fatalf("status %d, members %+v", res.Code, got)
	}
	if got[0].NodeStatus == nil || got[0].NodeStatus.DispatchBacklog != 4 || got[0].NodeStatus.Quarantine.Copies != 1 {
		t.Fatalf("cluster-0 detail = %+v", got[0])
	}
	if got[1].NodeStatus != nil || !strings.Contains(got[1].StatusError, "older release") {
		t.Fatalf("cluster-1 detail = %+v; want a status error naming the older release", got[1])
	}
}

// The moves view shows each side's liveness and why a move is blocked;
// ?detail=true adds the destination's own report of the move.
func TestMovesViewShowsLivenessAndBlockedReason(t *testing.T) {
	set := seededSet(t) // secret-topic/0: cluster-0 -> cluster-1, which is not a member
	ms := set.Deps.Metastore
	ctx := context.Background()
	for _, id := range []string{"cluster-2", "cluster-3"} {
		if err := ms.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	for p, move := range [][2]string{{"cluster-2", "cluster-0"}, {"cluster-0", "cluster-3"}} {
		if err := ms.AssignPartition(ctx, "orders", p, move[0]); err != nil {
			t.Fatal(err)
		}
		if err := ms.SetAssignmentTarget(ctx, "orders", p, move[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.MarkMemberDead(ctx, "cluster-2"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MarkMemberDead(ctx, "cluster-3"); err != nil {
		t.Fatal(err)
	}
	set.Deps.NodeStatus = func(_ context.Context, m metastore.Member) (nodewire.NodeStatus, error) {
		if m.ID != "cluster-0" {
			return nodewire.NodeStatus{}, errors.New("dial: connection refused")
		}
		return nodewire.NodeStatus{Node: "cluster-0", Moves: []nodewire.MoveState{{Topic: "orders", Partition: 0, Phase: "waiting_for_source"}}}, nil
	}

	res := httptest.NewRecorder()
	httpcluster.Moves(set).ServeHTTP(res, adminRequest(http.MethodGet, "/v1/cluster/moves?detail=true"))
	got := decodeBody[struct{ Moves []httpcluster.MoveView }](t, res).Moves
	if res.Code != http.StatusOK || len(got) != 3 {
		t.Fatalf("status %d, moves %+v", res.Code, got)
	}
	want := []struct{ from, to, blocked string }{
		{"dead", "alive", "source_dead"},               // orders/0: cluster-2 (dead) -> cluster-0
		{"alive", "dead", "target_dead"},               // orders/1: cluster-0 -> cluster-3 (dead)
		{"alive", "not_a_member", "target_not_member"}, // secret-topic/0
	}
	for i, w := range want {
		if got[i].FromStatus != w.from || got[i].ToStatus != w.to || got[i].Blocked != w.blocked {
			t.Fatalf("move %d = %+v, want from %s, to %s, blocked %s", i, got[i], w.from, w.to, w.blocked)
		}
	}
	if got[0].Worker == nil || got[0].Worker.Phase != "waiting_for_source" {
		t.Fatalf("orders/0 worker = %+v, want the destination's report", got[0].Worker)
	}
	if got[1].Worker != nil || !strings.Contains(got[1].WorkerError, "connection refused") {
		t.Fatalf("orders/1 = %+v, want the destination's error", got[1])
	}
	if got[2].WorkerError == "" {
		t.Fatalf("secret-topic/0 = %+v, want a worker error for a target that is not a member", got[2])
	}
}

// fakeAbortRouter is a router that forwards a move abort to a leader,
// played by ms: flipFirst commits the move's flip there before the abort
// arrives, and readErr fails the read-back of the leader's assignment.
type fakeAbortRouter struct {
	handlers.Router
	ms        *metastore.Store
	forwarded []string
	err       error
	flipFirst bool
	readErr   error
}

func (f *fakeAbortRouter) ForwardAbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) (bool, error) {
	f.forwarded = append(f.forwarded, fmt.Sprintf("%s/%d->%s", topicName, partition, expectedTarget))
	if f.err != nil {
		return true, f.err
	}
	if f.flipFirst {
		if err := f.ms.CompleteMove(ctx, topicName, partition, "cluster-0", expectedTarget); err != nil {
			return true, err
		}
	}
	return true, f.ms.AbortMove(ctx, topicName, partition, expectedTarget)
}

func (f *fakeAbortRouter) LeaderAssignment(_ context.Context, topicName string, partition int) (metastore.Assignment, bool, error) {
	if f.readErr != nil {
		return metastore.Assignment{}, false, f.readErr
	}
	a, err := f.ms.GetAssignment(topicName, partition)
	if errors.Is(err, errs.ErrNotFound) {
		return metastore.Assignment{}, false, nil
	}
	return a, err == nil, err
}

func abortRequest(target string) *http.Request {
	return adminRequest(http.MethodPost, target, "topic", "secret-topic", "partition", "0")
}

// An operator can clear a move's target: on the leader directly, from a
// follower through the leader. The partition stays with its owner.
func TestMoveAbortClearsTheTargetOnTheLeader(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore

	res := httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort?target=cluster-1"))
	if res.Code != http.StatusAccepted {
		t.Fatalf("abort: status %d (%s), want 202", res.Code, res.Body)
	}
	a, err := ms.GetAssignment("secret-topic", 0)
	if err != nil || a.TargetID != "" || a.OwnerID != "cluster-0" {
		t.Fatalf("assignment after abort = %+v, %v; want owner cluster-0 and no target", a, err)
	}
	body := decodeBody[httpcluster.AbortView](t, res)
	if body.Move.To != "cluster-1" || body.Note == "" {
		t.Fatalf("abort body = %+v", body)
	}
	res = httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort"))
	if res.Code != http.StatusConflict {
		t.Fatalf("abort with no move in flight: status %d, want 409", res.Code)
	}

	// From a follower the abort goes to the leader as the compare-and-set
	// on the target this node read.
	if err := ms.SetAssignmentTarget(context.Background(), "secret-topic", 0, "cluster-1"); err != nil {
		t.Fatal(err)
	}
	router := &fakeAbortRouter{ms: ms}
	set.Deps.Router = router
	res = httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort"))
	if res.Code != http.StatusAccepted || !slices.Equal(router.forwarded, []string{"secret-topic/0->cluster-1"}) {
		t.Fatalf("forwarded abort: status %d (%s), forwarded %v", res.Code, res.Body, router.forwarded)
	}
	if err := ms.SetAssignmentTarget(context.Background(), "secret-topic", 0, "cluster-1"); err != nil {
		t.Fatal(err)
	}
	router.err = errors.New("abort move returned status 503")
	res = httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort"))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("forwarded abort the leader failed: status %d, want 503", res.Code)
	}
}

// An abort answers what the leader did, not what this node read before
// it asked: a move whose flip committed before the abort reached the
// leader has moved, and the abort is refused with 409 naming the owner
// now, not answered 202 "stays with its owner", and not audited.
func TestMoveAbortThatLostTheRaceToTheFlipIsRefused(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	var logs bytes.Buffer
	set.Deps.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	set.Deps.Router = &fakeAbortRouter{ms: ms, flipFirst: true}

	res := httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort?target=cluster-1"))

	a, _ := ms.GetAssignment("secret-topic", 0)
	if a.OwnerID != "cluster-1" {
		t.Fatalf("assignment after = %+v; the flip was meant to commit first", a)
	}
	if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), "cluster-1 owns it now") {
		t.Fatalf("abort that lost to the flip: status %d (%s), want 409 naming cluster-1 as the owner now", res.Code, res.Body)
	}
	if strings.Contains(logs.String(), "cluster.move.abort") {
		t.Fatalf("audited an abort the leader never applied:\n%s", logs.String())
	}
}

// An abort whose outcome cannot be read back from the leader is not
// reported as done: 503, and the audit line says the outcome is unknown.
func TestMoveAbortWhoseOutcomeCannotBeReadBackSaysSo(t *testing.T) {
	set := seededSet(t)
	var logs bytes.Buffer
	set.Deps.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	set.Deps.Router = &fakeAbortRouter{ms: set.Deps.Metastore, readErr: errors.New("get assignment returned status 503")}

	res := httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort"))

	if res.Code != http.StatusServiceUnavailable || !strings.Contains(res.Body.String(), "could not be read back") {
		t.Fatalf("status %d (%s), want 503 saying the outcome could not be read back", res.Code, res.Body)
	}
	if !strings.Contains(logs.String(), "cluster.move.abort") || !strings.Contains(logs.String(), "outcome unknown") {
		t.Fatalf("audit line does not record the unknown outcome:\n%s", logs.String())
	}
}

// An abort that names another destination than the move's current one
// changes nothing, and a partition with no assignment is a 404.
func TestMoveAbortRefusesAStaleTarget(t *testing.T) {
	set := seededSet(t)
	res := httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort?target=cluster-9"))
	if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), "cluster-1") {
		t.Fatalf("stale target: status %d (%s), want 409 naming the current target", res.Code, res.Body)
	}
	if a, _ := set.Deps.Metastore.GetAssignment("secret-topic", 0); a.TargetID != "cluster-1" {
		t.Fatalf("a refused abort cleared the target: %+v", a)
	}
	res = httptest.NewRecorder()
	req := adminRequest(http.MethodPost, "/v1/cluster/moves/secret-topic/7/abort", "topic", "secret-topic", "partition", "7")
	httpcluster.AbortMove(set).ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("no assignment: status %d, want 404", res.Code)
	}
}

// Every abort is an audited admin action.
func TestMoveAbortIsAudited(t *testing.T) {
	set := seededSet(t)
	var logs bytes.Buffer
	set.Deps.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	res := httptest.NewRecorder()
	httpcluster.AbortMove(set).ServeHTTP(res, abortRequest("/v1/cluster/moves/secret-topic/0/abort"))
	if res.Code != http.StatusAccepted {
		t.Fatalf("status %d (%s)", res.Code, res.Body)
	}
	line := logs.String()
	for _, want := range []string{"component=audit", "cluster.move.abort", "secret-topic/0", "root"} {
		if !strings.Contains(line, want) {
			t.Fatalf("audit line %q lacks %q", line, want)
		}
	}
}
