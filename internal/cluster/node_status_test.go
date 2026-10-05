package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
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
