package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const defaultPeerRPCTimeout = 5 * time.Second

// peerClient is the node-to-node RPC surface the router and dispatcher use.
// *PeerClient implements it; tests substitute fakes.
type peerClient interface {
	Produce(context.Context, string, nodewire.ProduceRequest) (nodewire.Response, error)
	CommitProduce(context.Context, string, nodewire.CommitProduceRequest) (nodewire.Response, error)
	CommitProduceBatch(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error)
	Consume(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error)
	Ack(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	ExtendAck(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	Nack(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	CreateTopic(context.Context, string, []byte) (nodewire.Response, error)
	AlterTopic(context.Context, string, string, []byte) (nodewire.Response, error)
	DeleteTopic(context.Context, string, string) (nodewire.Response, error)
	GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error)
	JoinCluster(ctx context.Context, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error)
	AttachChild(ctx context.Context, addr, parent, child string, delayMs int64) (nodewire.Response, error)
	DetachChild(ctx context.Context, addr, parent, child string) (nodewire.Response, error)
	FanoutCursors(ctx context.Context, addr, parent string) ([]topic.FanoutCursorStat, error)
	PurgeTopic(ctx context.Context, addr, topicName, id string) (nodewire.Response, error)
	TopicPartitionStats(context.Context, string, string, int) (topic.PartitionStats, error)
	NotifyToken(context.Context, string, nodewire.TokenNotifyRequest) (nodewire.Response, error)
	RegisterTokens(context.Context, string, nodewire.TokenDelta) (nodewire.Response, error)

	// The Within forms bound one call by a budget of the caller's own
	// (see clusterrpc.QUICFrameClient.RequestOnLaneTimeout): it runs out
	// with an error wrapping context.DeadlineExceeded, exactly like a
	// ctx deadline, while ctx still carries cancellation. The hot paths
	// use them instead of deriving a context.WithTimeout per call.
	ConsumeWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.ConsumeRequest) (nodewire.Response, error)
	AckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error)
	ExtendAckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error)
	NackWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error)
	NotifyTokenWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.TokenNotifyRequest) (nodewire.Response, error)
	RegisterTokensWithin(ctx context.Context, addr string, timeout time.Duration, delta nodewire.TokenDelta) (nodewire.Response, error)
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
	CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error)
	UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error)
	DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error)
	DecommissionMember(ctx context.Context, addr, id string, cancel bool) (nodewire.Response, error)
	AppliedIndex(ctx context.Context, addr string) (uint64, error)
}

// frameTransport is the request/reply surface PeerClient needs from the
// cluster transport. *clusterrpc.QUICFrameClient implements it; tests
// substitute a fake to observe which lane each operation selects.
type frameTransport interface {
	RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error)
	RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, timeout time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error)
}

// RPCMetrics receives one observation per peer RPC issued by a
// PeerClient: the operation name, its outcome, and the round-trip time.
// The observability metrics package is owned elsewhere, so the client
// records through this small hook; PrometheusRPCMetrics is the production
// implementation and a nil hook records nothing.
type RPCMetrics interface {
	ObserveRPC(op, outcome string, elapsed time.Duration)
}

// RPC outcome labels. "ok" is any 2xx/3xx reply; "rejected" a 4xx;
// "failed" a 5xx; "timeout" a transport deadline; "error" any other
// transport failure.
const (
	rpcOutcomeOK       = "ok"
	rpcOutcomeRejected = "rejected"
	rpcOutcomeFailed   = "failed"
	rpcOutcomeTimeout  = "timeout"
	rpcOutcomeError    = "error"
)

// PrometheusRPCMetrics records PeerClient observations as
// narad_cluster_rpc_requests_total{op,outcome} and
// narad_cluster_rpc_request_seconds{op}.
type PrometheusRPCMetrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

// NewPrometheusRPCMetrics builds and registers the peer RPC metrics on reg.
func NewPrometheusRPCMetrics(reg prometheus.Registerer) *PrometheusRPCMetrics {
	m := &PrometheusRPCMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "narad_cluster_rpc_requests_total",
			Help: "Peer RPCs issued by this node, by operation and outcome.",
		}, []string{"op", "outcome"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "narad_cluster_rpc_request_seconds",
			Help:    "Peer RPC round-trip time in seconds, by operation.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"op"}),
	}
	reg.MustRegister(m.requests, m.latency)
	return m
}

// ObserveRPC implements RPCMetrics.
func (m *PrometheusRPCMetrics) ObserveRPC(op, outcome string, elapsed time.Duration) {
	m.requests.WithLabelValues(op, outcome).Inc()
	m.latency.WithLabelValues(op).Observe(elapsed.Seconds())
}

// PeerClient issues node RPCs to peers over the QUIC frame transport. It is
// the client side of RPCServer.
type PeerClient struct {
	frames frameTransport
	// bulk carries the partition-transfer RPCs (segment chunks of up to
	// storage.MaxSegmentReadBytes, handoff preparation, fan-out cursors)
	// on a connection of their own. On the produce lane they shared
	// streams with commit batches, and a commit reply queued behind a
	// multi-megabyte chunk on the same stream for as long as the chunk
	// took to cross (a p99 of 16 ms against 0.4 ms on loopback, the
	// chunk's transfer time on a real network); on the same connection
	// they would still share its flow-control window and congestion
	// state. Its socket and its connection to a peer are made on first
	// use, so a node that never moves a partition or lists fan-out
	// cursors never opens either. nil sends them on frames.
	bulk    frameTransport
	metrics RPCMetrics
}

// SetMetrics wires the RPC metrics hook; nil disables recording. Call
// before the client is shared across goroutines.
func (c *PeerClient) SetMetrics(m RPCMetrics) {
	c.metrics = m
}

// NewPeerClient constructs a PeerClient. timeout is the transport's default
// reply timeout, applied to requests whose context carries no deadline;
// <= 0 uses defaultPeerRPCTimeout. secret authenticates to peers that
// require a cluster secret (empty disables it).
func NewPeerClient(timeout time.Duration, secret string) *PeerClient {
	if timeout <= 0 {
		timeout = defaultPeerRPCTimeout
	}
	return &PeerClient{
		frames: clusterrpc.NewQUICFrameClient(timeout, secret),
		bulk:   clusterrpc.NewQUICFrameClient(timeout, secret),
	}
}

// Close releases the transports' pooled connections and sockets. A nil
// client is a no-op.
func (c *PeerClient) Close() error {
	if c == nil {
		return nil
	}
	var errs []error
	for _, frames := range []frameTransport{c.frames, c.bulk} {
		if closer, ok := frames.(io.Closer); ok {
			errs = append(errs, closer.Close())
		}
	}
	return errors.Join(errs...)
}

// Lanes per operation. The transport pools 16 streams per bulk lane and
// 4 for control; passing the wrong lane silently funnels bulk traffic
// through the narrow control pool (which is exactly what happened when
// operation names were passed as lanes), so every send names its lane
// explicitly:
//
//   - produce and commit_produce(_batch) carry record payloads: produce lane.
//   - consume replies carry record payloads: consume lane.
//   - ack, extend_ack, nack are small but high-rate: ack lane.
//   - fan-out cursors, segment chunks, and prepare_handoff are bulk
//     transfers: their own connection (see PeerClient.bulk), on its
//     produce lane.
//   - everything else (topic/user/member admin, leader confirmations,
//     move coordination) is light control traffic: control lane.
const (
	laneControl = clusterrpc.LaneControl
	laneProduce = clusterrpc.LaneProduce
	laneConsume = clusterrpc.LaneConsume
	laneAck     = clusterrpc.LaneAck
)

// Produce forwards a produce request to the peer at addr.
func (c *PeerClient) Produce(ctx context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeProduceRequest(req)
	return c.send(ctx, addr, "produce", laneProduce, payload, err)
}

// CommitProduce commits a single accepted produce record on the peer at addr.
func (c *PeerClient) CommitProduce(ctx context.Context, addr string, req nodewire.CommitProduceRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeCommitProduceRequest(req)
	return c.send(ctx, addr, "commit_produce", laneProduce, payload, err)
}

// CommitProduceBatch commits a batch of accepted produce records on the peer
// at addr.
func (c *PeerClient) CommitProduceBatch(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeCommitProduceBatchRequest(req)
	return c.send(ctx, addr, "commit_produce_batch", laneProduce, payload, err)
}

// Consume forwards a consume request to the peer at addr.
func (c *PeerClient) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	return c.ConsumeWithin(ctx, addr, 0, req)
}

// ConsumeWithin is Consume bounded by timeout (see peerClient).
func (c *PeerClient) ConsumeWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeConsumeRequest(req)
	return c.sendWithin(ctx, addr, "consume", laneConsume, timeout, payload, err)
}

// Ack forwards an ack request to the peer at addr.
func (c *PeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	return c.AckWithin(ctx, addr, 0, req)
}

// AckWithin is Ack bounded by timeout (see peerClient).
func (c *PeerClient) AckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeAckRequest(req)
	return c.sendWithin(ctx, addr, "ack", laneAck, timeout, payload, err)
}

// ExtendAck forwards a visibility-window extension to the peer at addr.
func (c *PeerClient) ExtendAck(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	return c.ExtendAckWithin(ctx, addr, 0, req)
}

// ExtendAckWithin is ExtendAck bounded by timeout (see peerClient).
func (c *PeerClient) ExtendAckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeExtendAckRequest(req)
	return c.sendWithin(ctx, addr, "extend_ack", laneAck, timeout, payload, err)
}

// Nack forwards an immediate reservation release to the peer at addr.
func (c *PeerClient) Nack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	return c.NackWithin(ctx, addr, 0, req)
}

// NackWithin is Nack bounded by timeout (see peerClient).
func (c *PeerClient) NackWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeNackRequest(req)
	return c.sendWithin(ctx, addr, "nack", laneAck, timeout, payload, err)
}

// CreateTopic forwards a raw topic create body to the peer at addr.
func (c *PeerClient) CreateTopic(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Body: body})
	return c.send(ctx, addr, "create_topic", laneControl, payload, err)
}

// AlterTopic forwards a raw topic alter body to the peer at addr.
func (c *PeerClient) AlterTopic(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: topicName, Body: body})
	return c.send(ctx, addr, "alter_topic", laneControl, payload, err)
}

// AppliedIndex asks the peer at addr, the leader a control-plane write
// was just forwarded to, for the highest log index its FSM has applied.
// The caller waits for its own replica to reach that index before
// answering the client (Router.settleForwardedWrite). A peer that is
// not the leader, or predates the operation, is an error the caller
// treats as "no bound available".
func (c *PeerClient) AppliedIndex(ctx context.Context, addr string) (uint64, error) {
	res, err := c.send(ctx, addr, "applied_index", laneControl, nodewire.EncodeAppliedIndexRequest(), nil)
	if err != nil {
		return 0, err
	}
	if res.Status != http.StatusOK {
		return 0, fmt.Errorf("applied index: peer answered %d", res.Status)
	}
	var body appliedIndexResponse
	if err := json.Unmarshal(res.Body, &body); err != nil {
		return 0, fmt.Errorf("applied index: decode: %w", err)
	}
	return body.AppliedIndex, nil
}

// DeleteTopic asks the peer at addr to delete the topic.
func (c *PeerClient) DeleteTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpDeleteTopic, "delete_topic", topicName)
}

// GetTopic fetches a topic record from the peer at addr. Used by the
// startup orphan sweep to confirm absence with the LEADER before
// deleting a topic directory — a freshly restarted local replica can
// be arbitrarily stale.
func (c *PeerClient) GetTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpGetTopic, "get_topic", topicName)
}

// PurgeTopic asks the peer at addr to purge the on-disk state of the
// topic incarnation id (the deleted record's ID). A peer that predates
// incarnation IDs cannot decode the trailing ID field and answers 400;
// the request is then repeated by name only, which is the purge that
// peer has always performed.
func (c *PeerClient) PurgeTopic(ctx context.Context, addr, topicName, id string) (nodewire.Response, error) {
	if id == "" {
		return c.topicNameRequest(ctx, addr, nodewire.OpPurgeTopic, "purge_topic", topicName)
	}
	payload, err := nodewire.EncodeTopicNameRequest(nodewire.OpPurgeTopic, nodewire.TopicNameRequest{Topic: topicName, ID: id})
	res, err := c.send(ctx, addr, "purge_topic", laneControl, payload, err)
	if err != nil || res.Status != http.StatusBadRequest {
		return res, err
	}
	return c.topicNameRequest(ctx, addr, nodewire.OpPurgeTopic, "purge_topic", topicName)
}

// AttachChild forwards a fan-out attach to the peer at addr (the leader).
func (c *PeerClient) AttachChild(ctx context.Context, addr, parent, child string, delayMs int64) (nodewire.Response, error) {
	payload, err := nodewire.EncodeChildLinkRequest(nodewire.OpAttachChild, nodewire.ChildLinkRequest{Parent: parent, Child: child, DelayMs: delayMs})
	return c.send(ctx, addr, "attach_child", laneControl, payload, err)
}

// DetachChild forwards a fan-out detach to the peer at addr (the leader).
func (c *PeerClient) DetachChild(ctx context.Context, addr, parent, child string) (nodewire.Response, error) {
	payload, err := nodewire.EncodeChildLinkRequest(nodewire.OpDetachChild, nodewire.ChildLinkRequest{Parent: parent, Child: child})
	return c.send(ctx, addr, "detach_child", laneControl, payload, err)
}

// FanoutCursors fetches the fan-out cursor positions the peer at addr
// holds for the parent's partitions it owns.
func (c *PeerClient) FanoutCursors(ctx context.Context, addr, parent string) ([]topic.FanoutCursorStat, error) {
	payload, err := nodewire.EncodeTopicNameRequest(nodewire.OpFanoutCursors, nodewire.TopicNameRequest{Topic: parent})
	res, err := c.sendBulk(ctx, addr, "fanout_cursors", payload, err)
	if err != nil {
		return nil, err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("fanout cursors returned status %d", res.Status)
	}
	var stats []topic.FanoutCursorStat
	if err := json.Unmarshal(res.Body, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

// TopicPartitionStats fetches one partition's stats from the peer at addr and
// validates that the peer answered for the requested partition.
func (c *PeerClient) TopicPartitionStats(ctx context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
	payload, err := nodewire.EncodeTopicPartitionStatsRequest(nodewire.TopicPartitionStatsRequest{
		Topic:     topicName,
		Partition: partition,
	})
	res, err := c.send(ctx, addr, "topic_partition_stats", laneControl, payload, err)
	if err != nil {
		return topic.PartitionStats{}, err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return topic.PartitionStats{}, fmt.Errorf("topic partition stats returned status %d", res.Status)
	}
	var stats topic.PartitionStats
	if err := json.Unmarshal(res.Body, &stats); err != nil {
		return topic.PartitionStats{}, err
	}
	if stats.Index != partition {
		return topic.PartitionStats{}, fmt.Errorf("topic get returned partition %d, want %d", stats.Index, partition)
	}
	return stats, nil
}

// RegisterMember upserts a member record on the peer at addr.
// JoinCluster asks the node at addr — retried across peers until the
// leader is found — to admit this node into the Raft voter set.
func (c *PeerClient) JoinCluster(ctx context.Context, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeJoinClusterRequest(req)
	return c.send(ctx, addr, "join_cluster", laneControl, payload, err)
}

func (c *PeerClient) RegisterMember(ctx context.Context, addr string, req nodewire.MemberRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeMemberRequest(req)
	return c.send(ctx, addr, "register_member", laneControl, payload, err)
}

// CreateUser forwards a user create to the leader at addr.
func (c *PeerClient) CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpCreateUser, nodewire.UserRequest{Body: body})
	return c.send(ctx, addr, "create_user", laneControl, payload, err)
}

// UpdateUser forwards a user update to the leader at addr.
func (c *PeerClient) UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpUpdateUser, nodewire.UserRequest{Username: username, Body: body})
	return c.send(ctx, addr, "update_user", laneControl, payload, err)
}

// DeleteUser forwards a user delete to the leader at addr.
func (c *PeerClient) DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpDeleteUser, nodewire.UserRequest{Username: username})
	return c.send(ctx, addr, "delete_user", laneControl, payload, err)
}

// DecommissionMember forwards a decommission (mark/clear draining) to the
// leader at addr.
func (c *PeerClient) DecommissionMember(ctx context.Context, addr, id string, cancel bool) (nodewire.Response, error) {
	payload, err := nodewire.EncodeDecommissionRequest(nodewire.DecommissionRequest{ID: id, Cancel: cancel})
	return c.send(ctx, addr, "decommission_member", laneControl, payload, err)
}

// CompleteMove forwards the guarded ownership flip to the leader at addr.
// Returns an error unless the leader applied it (the CAS may legitimately
// fail, which the caller treats as "flip not done").
func (c *PeerClient) CompleteMove(ctx context.Context, addr, topicName string, partition int, expectedOwner, targetID string) error {
	payload, err := nodewire.EncodeCompleteMoveRequest(nodewire.CompleteMoveRequest{
		Topic: topicName, Partition: partition, ExpectedOwner: expectedOwner, TargetID: targetID,
	})
	res, err := c.send(ctx, addr, "complete_move", laneControl, payload, err)
	if err != nil {
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("complete move returned status %d", res.Status)
	}
	return nil
}

// AbortMove forwards a move-target clear to the leader at addr.
func (c *PeerClient) AbortMove(ctx context.Context, addr, topicName string, partition int, expectedTarget string) error {
	payload, err := nodewire.EncodeAbortMoveRequest(nodewire.AbortMoveRequest{
		Topic: topicName, Partition: partition, ExpectedTarget: expectedTarget,
	})
	res, err := c.send(ctx, addr, "abort_move", laneControl, payload, err)
	if err != nil {
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("abort move returned status %d", res.Status)
	}
	return nil
}

func (c *PeerClient) topicNameRequest(ctx context.Context, addr string, op nodewire.Operation, operation, topicName string) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicNameRequest(op, nodewire.TopicNameRequest{Topic: topicName})
	return c.send(ctx, addr, operation, laneControl, payload, err)
}

// send performs the request round trip once the encode step succeeded.
func (c *PeerClient) send(ctx context.Context, addr, operation string, lane clusterrpc.Lane, payload []byte, encodeErr error) (nodewire.Response, error) {
	return c.sendWithin(ctx, addr, operation, lane, 0, payload, encodeErr)
}

// sendWithin is send bounded by timeout; <= 0 adds no budget of its own.
func (c *PeerClient) sendWithin(ctx context.Context, addr, operation string, lane clusterrpc.Lane, timeout time.Duration, payload []byte, encodeErr error) (nodewire.Response, error) {
	if encodeErr != nil {
		return nodewire.Response{}, encodeErr
	}
	return c.request(ctx, addr, operation, lane, timeout, payload)
}

// sendBulk is send for the partition-transfer RPCs: they ride the bulk
// transport (see PeerClient.bulk) when the client has one.
func (c *PeerClient) sendBulk(ctx context.Context, addr, operation string, payload []byte, encodeErr error) (nodewire.Response, error) {
	if encodeErr != nil {
		return nodewire.Response{}, encodeErr
	}
	if c != nil && c.bulk != nil {
		return c.requestOn(ctx, c.bulk, addr, operation, laneProduce, 0, payload)
	}
	return c.request(ctx, addr, operation, laneProduce, 0, payload)
}

func (c *PeerClient) request(ctx context.Context, addr, operation string, lane clusterrpc.Lane, timeout time.Duration, payload []byte) (nodewire.Response, error) {
	if c == nil || c.frames == nil {
		return nodewire.Response{}, fmt.Errorf("peer rpc client is nil")
	}
	return c.requestOn(ctx, c.frames, addr, operation, lane, timeout, payload)
}

func (c *PeerClient) requestOn(ctx context.Context, frames frameTransport, addr, operation string, lane clusterrpc.Lane, timeout time.Duration, payload []byte) (nodewire.Response, error) {
	if c.metrics == nil {
		return roundTrip(ctx, frames, addr, lane, timeout, payload)
	}
	start := time.Now()
	res, err := roundTrip(ctx, frames, addr, lane, timeout, payload)
	c.metrics.ObserveRPC(operation, rpcOutcome(res, err), time.Since(start))
	return res, err
}

func roundTrip(ctx context.Context, frames frameTransport, addr string, lane clusterrpc.Lane, timeout time.Duration, payload []byte) (nodewire.Response, error) {
	var (
		frame clusterwire.StreamFrame
		err   error
	)
	if timeout > 0 {
		frame, err = frames.RequestOnLaneTimeout(ctx, addr, lane, timeout, clusterwire.StreamFrameNodeRequest, payload)
	} else {
		frame, err = frames.RequestOnLane(ctx, addr, lane, clusterwire.StreamFrameNodeRequest, payload)
	}
	if err != nil {
		return nodewire.Response{}, err
	}
	if frame.Type != clusterwire.StreamFrameNodeReply {
		return nodewire.Response{}, fmt.Errorf("unexpected peer rpc frame type %d", frame.Type)
	}
	return nodewire.DecodeResponse(frame.Payload)
}

// rpcOutcome classifies a round trip for the metrics hook.
func rpcOutcome(res nodewire.Response, err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return rpcOutcomeTimeout
	case err != nil:
		return rpcOutcomeError
	case res.Status >= http.StatusInternalServerError:
		return rpcOutcomeFailed
	case res.Status >= http.StatusBadRequest:
		return rpcOutcomeRejected
	default:
		return rpcOutcomeOK
	}
}

func writePeerResponse(w http.ResponseWriter, res nodewire.Response) {
	if res.Status == 0 {
		res.Status = http.StatusOK
	}
	if len(res.Body) == 0 {
		// Nothing to type or sniff, which is every forwarded ack. Not
		// touching the header map spares net/http building one and
		// cloning it at WriteHeader, the way a local ack's bare 204 does.
		w.WriteHeader(res.Status)
		return
	}
	setContentHeaders(w.Header(), res.ContentType)
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
}

// Header values shared by every response that carries a body. Assigned
// directly rather than through Header.Set, which allocates a fresh
// one-element slice per call; net/http only reads them, and Add or Set
// replace the slice rather than writing into it.
var (
	headerJSON        = []string{nodewire.ContentTypeJSON}
	headerOctetStream = []string{"application/octet-stream"}
	headerNoSniff     = []string{"nosniff"}
)

// setContentHeaders types a proxied body. An untyped one is served as
// application/octet-stream and never content-sniffed: a payload that
// happens to look like HTML must not be served as text/html.
func setContentHeaders(h http.Header, contentType string) {
	switch contentType {
	case nodewire.ContentTypeJSON:
		h["Content-Type"] = headerJSON
	case "", "application/octet-stream":
		h["Content-Type"] = headerOctetStream
	default:
		h.Set("Content-Type", contentType)
	}
	h["X-Content-Type-Options"] = headerNoSniff
}

// writeOwnerDown answers a partition-pinned consume/ack whose owner node is
// currently down. Without replication the partition is unavailable until the
// owner returns (or the partition moves), so this is a RETRYABLE condition:
// a 503 tells clients to back off and try again, where falling through to
// local handling would surface a terminal-looking 421.
func writeOwnerDown(w http.ResponseWriter) {
	http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
}

// ListPartitionSegments asks the owner at addr for a partition's segment
// list and durable positions (rebalance copy, serve side).
func (c *PeerClient) ListPartitionSegments(ctx context.Context, addr, topicName string, partition int) (messaging.PartitionTransferInfo, error) {
	payload, err := nodewire.EncodePartitionSegmentsRequest(nodewire.PartitionSegmentsRequest{Topic: topicName, Partition: partition})
	res, err := c.send(ctx, addr, "list_partition_segments", laneControl, payload, err)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return messaging.PartitionTransferInfo{}, fmt.Errorf("list partition segments returned status %d", res.Status)
	}
	var info messaging.PartitionTransferInfo
	if err := json.Unmarshal(res.Body, &info); err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	return info, nil
}

// FetchSegmentChunk fetches a bounded byte range of one segment from the
// owner at addr (rebalance copy, serve side).
func (c *PeerClient) FetchSegmentChunk(ctx context.Context, addr, topicName string, partition int, baseOffset, at, length int64) ([]byte, error) {
	payload, err := nodewire.EncodeFetchSegmentChunkRequest(nodewire.FetchSegmentChunkRequest{
		Topic: topicName, Partition: partition, BaseOffset: baseOffset, At: at, Length: length,
	})
	res, err := c.sendBulk(ctx, addr, "fetch_segment_chunk", payload, err)
	if err != nil {
		return nil, err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("fetch segment chunk returned status %d", res.Status)
	}
	return res.Body, nil
}

// PrepareHandoff asks the owner at addr to freeze (Topic, Partition) for
// a rebalance handoff and return its final positions. With an empty
// freezeToken it arms (or idempotently extends) the freeze and returns
// its token; with a token it only extends the freeze that token names,
// failing if that freeze lapsed, which is how the destination re-arms
// while draining and fences the flip.
func (c *PeerClient) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, freezeTTL time.Duration, freezeToken string) (messaging.PartitionTransferInfo, error) {
	payload, err := nodewire.EncodePrepareHandoffRequest(nodewire.PrepareHandoffRequest{
		Topic: topicName, Partition: partition, FreezeTTLNanos: int64(freezeTTL), FreezeToken: freezeToken,
	})
	res, err := c.sendBulk(ctx, addr, "prepare_handoff", payload, err)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	if res.Status == http.StatusConflict && freezeToken != "" {
		return messaging.PartitionTransferInfo{}, messaging.ErrHandoffFreezeLapsed
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return messaging.PartitionTransferInfo{}, fmt.Errorf("prepare handoff returned status %d", res.Status)
	}
	var info messaging.PartitionTransferInfo
	if err := json.Unmarshal(res.Body, &info); err != nil {
		return messaging.PartitionTransferInfo{}, err
	}
	return info, nil
}

// GetAssignment fetches the node at addr's view of a partition assignment.
// Aimed at the LEADER for authoritative confirmation before destructive
// decisions (the stale-copy sweep).
func (c *PeerClient) GetAssignment(ctx context.Context, addr, topicName string, partition int) (metastore.Assignment, error) {
	payload, err := nodewire.EncodeGetAssignmentRequest(nodewire.GetAssignmentRequest{Topic: topicName, Partition: partition})
	res, err := c.send(ctx, addr, "get_assignment", laneControl, payload, err)
	if err != nil {
		return metastore.Assignment{}, err
	}
	if res.Status != http.StatusOK {
		return metastore.Assignment{}, fmt.Errorf("get assignment returned status %d", res.Status)
	}
	var a metastore.Assignment
	if err := json.Unmarshal(res.Body, &a); err != nil {
		return metastore.Assignment{}, err
	}
	return a, nil
}

// NotifyToken spends one of a peer's tokens: it tells that peer records
// may be available for a topic and reports what it said back. The reply
// is a single verdict byte, so this is about as small as an RPC gets.
//
// A failure is not retried. The token is spent either way, and telling
// a peer twice about one record means two claims for one consumer; the
// peer re-registers on its own if it still wants a turn.
func (c *PeerClient) NotifyToken(ctx context.Context, addr string, req nodewire.TokenNotifyRequest) (nodewire.Response, error) {
	return c.NotifyTokenWithin(ctx, addr, 0, req)
}

// NotifyTokenWithin is NotifyToken bounded by timeout (see peerClient).
func (c *PeerClient) NotifyTokenWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.TokenNotifyRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTokenNotifyRequest(req)
	return c.sendWithin(ctx, addr, "token_notify", laneControl, timeout, payload, err)
}

// RegisterTokens sends one peer its batched token delta: the topics this
// node now wants to hear about, and the ones it no longer does. Both
// travel together so retiring stale interest costs bytes in a frame that
// was already going out rather than an RPC of its own.
func (c *PeerClient) RegisterTokens(ctx context.Context, addr string, delta nodewire.TokenDelta) (nodewire.Response, error) {
	return c.RegisterTokensWithin(ctx, addr, 0, delta)
}

// RegisterTokensWithin is RegisterTokens bounded by timeout (see
// peerClient).
func (c *PeerClient) RegisterTokensWithin(ctx context.Context, addr string, timeout time.Duration, delta nodewire.TokenDelta) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTokenDelta(delta)
	return c.sendWithin(ctx, addr, "token_register", laneControl, timeout, payload, err)
}
