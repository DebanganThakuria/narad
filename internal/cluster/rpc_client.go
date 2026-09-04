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
	PurgeTopic(context.Context, string, string) (nodewire.Response, error)
	TopicPartitionStats(context.Context, string, string, int) (topic.PartitionStats, error)
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
	CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error)
	UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error)
	DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error)
	DecommissionMember(ctx context.Context, addr, id string, cancel bool) (nodewire.Response, error)
}

// frameTransport is the request/reply surface PeerClient needs from the
// cluster transport. *clusterrpc.QUICFrameClient implements it; tests
// substitute a fake to observe which lane each operation selects.
type frameTransport interface {
	RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error)
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
	frames  frameTransport
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
	return &PeerClient{frames: clusterrpc.NewQUICFrameClient(timeout, secret)}
}

// Close releases the transport's pooled connections and socket. A nil
// client is a no-op.
func (c *PeerClient) Close() error {
	if c == nil {
		return nil
	}
	if closer, ok := c.frames.(io.Closer); ok {
		return closer.Close()
	}
	return nil
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
//     transfers: produce lane (the bulk data lane).
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
	payload, err := nodewire.EncodeConsumeRequest(req)
	return c.send(ctx, addr, "consume", laneConsume, payload, err)
}

// Ack forwards an ack request to the peer at addr.
func (c *PeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeAckRequest(req)
	return c.send(ctx, addr, "ack", laneAck, payload, err)
}

// ExtendAck forwards a visibility-window extension to the peer at addr.
func (c *PeerClient) ExtendAck(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeExtendAckRequest(req)
	return c.send(ctx, addr, "extend_ack", laneAck, payload, err)
}

// Nack forwards an immediate reservation release to the peer at addr.
func (c *PeerClient) Nack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeNackRequest(req)
	return c.send(ctx, addr, "nack", laneAck, payload, err)
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

// PurgeTopic asks the peer at addr to purge the topic's on-disk state.
func (c *PeerClient) PurgeTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
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
	res, err := c.send(ctx, addr, "fanout_cursors", laneProduce, payload, err)
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
	if encodeErr != nil {
		return nodewire.Response{}, encodeErr
	}
	return c.request(ctx, addr, operation, lane, payload)
}

func (c *PeerClient) request(ctx context.Context, addr, operation string, lane clusterrpc.Lane, payload []byte) (nodewire.Response, error) {
	if c == nil || c.frames == nil {
		return nodewire.Response{}, fmt.Errorf("peer rpc client is nil")
	}
	if c.metrics == nil {
		return c.roundTrip(ctx, addr, lane, payload)
	}
	start := time.Now()
	res, err := c.roundTrip(ctx, addr, lane, payload)
	c.metrics.ObserveRPC(operation, rpcOutcome(res, err), time.Since(start))
	return res, err
}

func (c *PeerClient) roundTrip(ctx context.Context, addr string, lane clusterrpc.Lane, payload []byte) (nodewire.Response, error) {
	frame, err := c.frames.RequestOnLane(ctx, addr, lane, clusterwire.StreamFrameNodeRequest, payload)
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
	contentType := res.ContentType
	if contentType == "" {
		// Never let net/http content-sniff a proxied body: a payload that
		// happens to look like HTML must not be served as text/html.
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if res.Status == 0 {
		res.Status = http.StatusOK
	}
	w.WriteHeader(res.Status)
	if len(res.Body) > 0 {
		_, _ = w.Write(res.Body)
	}
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
	res, err := c.send(ctx, addr, "fetch_segment_chunk", laneProduce, payload, err)
	if err != nil {
		return nil, err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("fetch segment chunk returned status %d", res.Status)
	}
	return res.Body, nil
}

// PrepareHandoff asks the owner at addr to freeze (Topic, Partition) for
// a rebalance handoff and return its final positions.
func (c *PeerClient) PrepareHandoff(ctx context.Context, addr, topicName string, partition int, freezeTTL time.Duration) (messaging.PartitionTransferInfo, error) {
	payload, err := nodewire.EncodePrepareHandoffRequest(nodewire.PrepareHandoffRequest{
		Topic: topicName, Partition: partition, FreezeTTLNanos: int64(freezeTTL),
	})
	res, err := c.send(ctx, addr, "prepare_handoff", laneProduce, payload, err)
	if err != nil {
		return messaging.PartitionTransferInfo{}, err
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
