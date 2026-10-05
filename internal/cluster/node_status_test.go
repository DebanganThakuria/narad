package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// serverTransport carries a PeerClient's requests straight into an
// RPCServer's dispatch, so a test exercises both ends of an op without a
// network.
type serverTransport struct{ s *RPCServer }

func (t serverTransport) RequestOnLane(ctx context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	res := t.s.dispatch(ctx, requestKey{stream: 1, request: 1}, payload)
	body, err := nodewire.EncodeResponse(res)
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: body}, nil
}

func (t serverTransport) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return t.RequestOnLane(ctx, addr, lane, frameType, payload)
}

// olderNodeTransport answers every request the way a 3.0.x server
// answers an op it does not know.
type olderNodeTransport struct{}

func (olderNodeTransport) RequestOnLane(_ context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	op, _ := nodewire.OperationOf(payload)
	body, err := nodewire.EncodeResponse(errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", op)))
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: body}, nil
}

func (t olderNodeTransport) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return t.RequestOnLane(ctx, addr, lane, frameType, payload)
}

func TestNodeStatusRPCReportsBacklogQuarantineAndMoves(t *testing.T) {
	server := NewRPCServer(stubBroker{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	started := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	server.SetNodeStatus(func(context.Context) nodewire.NodeStatus {
		return nodewire.NodeStatus{
			Node: "narad-2", Draining: true, DispatchBacklog: 12,
			Quarantine: nodewire.QuarantineStatus{Copies: 2, Bytes: 4096, List: []nodewire.QuarantinedCopy{
				{Kind: "partition", Topic: "orders", Partition: 3, Dir: "topics/orders/p00003.quarantine", Bytes: 4000},
				{Kind: "staging", Topic: "orders", Partition: 1, Bytes: 96},
			}},
			Moves: []MoveState{{Topic: "orders", Partition: 1, Source: "narad-0", Target: "narad-2", StartedAt: started, Phase: MovePhaseBlocked, Blocked: MoveBlockedCopyUnverifiable}},
		}
	})
	client := &PeerClient{frames: serverTransport{s: server}}

	st, err := client.NodeStatus(context.Background(), "narad-2:7942")
	if err != nil {
		t.Fatalf("NodeStatus: %v", err)
	}
	if st.Node != "narad-2" || !st.Draining || st.DispatchBacklog != 12 {
		t.Fatalf("status = %+v, want narad-2 draining with a backlog of 12", st)
	}
	if st.Quarantine.Copies != 2 || st.Quarantine.Bytes != 4096 || len(st.Quarantine.List) != 2 || st.Quarantine.List[0].Partition != 3 {
		t.Fatalf("quarantine = %+v", st.Quarantine)
	}
	if len(st.Moves) != 1 || st.Moves[0].Blocked != MoveBlockedCopyUnverifiable || !st.Moves[0].StartedAt.Equal(started) {
		t.Fatalf("moves = %+v", st.Moves)
	}
}

// A 3.0.x node answers the op as unsupported: the caller learns the
// status is unknown, not that the node is unhealthy. A server with no
// status wired answers the same way.
func TestPeerClientReportsNodeStatusUnsupportedByAnOlderNode(t *testing.T) {
	older := &PeerClient{frames: olderNodeTransport{}}
	if _, err := older.NodeStatus(context.Background(), "old:7942"); !errors.Is(err, ErrNodeStatusUnsupported) {
		t.Fatalf("NodeStatus from a 3.0.x node = %v, want ErrNodeStatusUnsupported", err)
	}
	unwired := &PeerClient{frames: serverTransport{s: NewRPCServer(stubBroker{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))}}
	if _, err := unwired.NodeStatus(context.Background(), "peer:7942"); !errors.Is(err, ErrNodeStatusUnsupported) {
		t.Fatalf("NodeStatus from a server without status = %v, want ErrNodeStatusUnsupported", err)
	}
}

// committingBroker commits every record it is handed.
type committingBroker struct {
	stubBroker
	committed int
}

func (b *committingBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	b.committed++
	return int64(b.committed), nil
}

func (b *committingBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	offsets := make([]int64, len(records))
	for i := range records {
		b.committed++
		offsets[i] = int64(b.committed)
	}
	return offsets, nil
}

// Only client produce is refused on a draining node: an owner being
// decommissioned still commits what other nodes' dispatchers send it, or
// records accepted elsewhere for its partitions would stall until its
// partitions moved.
func TestDrainingOwnerStillCommitsForwardedRecords(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "127.0.0.1:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := store.SetMemberDraining(ctx, "node-self", true); err != nil {
		t.Fatalf("SetMemberDraining: %v", err)
	}
	br := &committingBroker{}
	server := NewRPCServer(br, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

	single, err := nodewire.EncodeCommitProduceRequest(nodewire.CommitProduceRequest{Topic: "orders", Key: "k", Payload: []byte("p"), CreatedAtUnixMs: 1})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{
		{Topic: "orders", Key: "a", Payload: []byte("1"), CreatedAtUnixMs: 1},
		{Topic: "orders", Key: "b", Payload: []byte("2"), CreatedAtUnixMs: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{"commit": single, "batch commit": batch} {
		if res := server.dispatch(ctx, requestKey{stream: 1, request: 1}, payload); res.Status != http.StatusOK {
			t.Fatalf("%s on a draining owner: status %d (%s), want 200", name, res.Status, res.Body)
		}
	}
	if br.committed != 3 {
		t.Fatalf("committed %d records, want 3", br.committed)
	}
}

// A decommission forwarded to the leader is preflighted there too: a
// 3.0.x follower forwards without checking. Removing the only voter
// could never complete, so it is refused with 409 and nothing is
// written; a cancel always goes through.
func TestForwardedDecommissionIsPreflightedOnTheLeader(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "127.0.0.1:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	server := NewRPCServer(stubBroker{}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))

	payload, err := nodewire.EncodeDecommissionRequest(nodewire.DecommissionRequest{ID: "node-self"})
	if err != nil {
		t.Fatal(err)
	}
	res := server.dispatch(ctx, requestKey{stream: 1, request: 1}, payload)
	if res.Status != http.StatusConflict {
		t.Fatalf("forwarded decommission of the only voter: status %d (%s), want 409", res.Status, res.Body)
	}
	var body controller.DecommissionRefusal
	if err := json.Unmarshal(res.Body, &body); err != nil || len(body.Reasons) == 0 || body.Reasons[0].Code != controller.BlockedBelowMinVoters {
		t.Fatalf("refusal body = %s (%v), want the below_min_voters reason", res.Body, err)
	}
	if m, _ := store.GetMember("node-self"); m.Draining {
		t.Fatal("a refused forwarded decommission marked the node draining")
	}

	cancelPayload, err := nodewire.EncodeDecommissionRequest(nodewire.DecommissionRequest{ID: "node-self", Cancel: true})
	if err != nil {
		t.Fatal(err)
	}
	if res := server.dispatch(ctx, requestKey{stream: 1, request: 2}, cancelPayload); res.Status != http.StatusNoContent {
		t.Fatalf("forwarded cancel: status %d (%s), want 204", res.Status, res.Body)
	}
}

// The HTTP abort handler finds the leader forwarder and the leader read
// it answers from through these methods.
var _ interface {
	ForwardAbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) (bool, error)
	LeaderAssignment(ctx context.Context, topicName string, partition int) (metastore.Assignment, bool, error)
} = (*Router)(nil)

// leaderReadPeer answers the leader assignment read, recording the
// addresses asked.
type leaderReadPeer struct {
	fakePeerClient
	a     metastore.Assignment
	asked []string
}

func (p *leaderReadPeer) leaderAssignment(_ context.Context, addr, _ string, _ int) (metastore.Assignment, bool, bool, error) {
	p.asked = append(p.asked, addr)
	return p.a, true, true, nil
}

// A move abort answers from the leader's assignment: the leader reads
// its own FSM behind a Barrier, and a follower asks the leader, never its
// own replica.
func TestRouterReadsTheAssignmentAsTheLeaderHasIt(t *testing.T) {
	stores := newTestStoreCluster(t, "node-a", "node-b")
	leaderID, leader := waitForClusterLeader(t, stores)
	followerID := "node-a"
	if leaderID == followerID {
		followerID = "node-b"
	}
	ctx := context.Background()
	for _, id := range []string{"node-a", "node-b"} {
		if err := leader.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := leader.AssignPartition(ctx, "orders", 0, leaderID); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := leader.SetAssignmentTarget(ctx, "orders", 0, followerID); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}

	onLeader := NewRouter(leader, leaderID, partition.NewHashRoundRobin(), "")
	a, found, err := onLeader.LeaderAssignment(ctx, "orders", 0)
	if err != nil || !found || a.OwnerID != leaderID || a.TargetID != followerID {
		t.Fatalf("leader read = %+v, %v, %v; want owner %s, target %s", a, found, err, leaderID, followerID)
	}
	if _, found, err := onLeader.LeaderAssignment(ctx, "orders", 7); err != nil || found {
		t.Fatalf("leader read of a partition with no assignment = found %v, %v; want not found", found, err)
	}

	waitForMember(t, stores[followerID], leaderID)
	onFollower := NewRouter(stores[followerID], followerID, partition.NewHashRoundRobin(), "")
	peer := &leaderReadPeer{a: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: followerID}}
	onFollower.peer = peer
	a, found, err = onFollower.LeaderAssignment(ctx, "orders", 0)
	if err != nil || !found || a.OwnerID != followerID || !slices.Equal(peer.asked, []string{leaderID + ":7942"}) {
		t.Fatalf("follower read = %+v, %v, %v (asked %v); want the leader's answer from %s:7942", a, found, err, peer.asked, leaderID)
	}
}
