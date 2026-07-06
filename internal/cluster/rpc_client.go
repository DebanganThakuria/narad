package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
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
	CreateTopic(context.Context, string, []byte) (nodewire.Response, error)
	AlterTopic(context.Context, string, string, []byte) (nodewire.Response, error)
	DeleteTopic(context.Context, string, string) (nodewire.Response, error)
	PurgeTopic(context.Context, string, string) (nodewire.Response, error)
	TopicPartitionStats(context.Context, string, string, int) (topic.PartitionStats, error)
	RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
	CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error)
	UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error)
	DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error)
}

// PeerClient issues node RPCs to peers over the QUIC frame transport. It is
// the client side of RPCServer.
type PeerClient struct {
	frames *clusterrpc.QUICFrameClient
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

// Produce forwards a produce request to the peer at addr.
func (c *PeerClient) Produce(ctx context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeProduceRequest(req)
	return c.send(ctx, addr, "produce", payload, err)
}

// CommitProduce commits a single accepted produce record on the peer at addr.
func (c *PeerClient) CommitProduce(ctx context.Context, addr string, req nodewire.CommitProduceRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeCommitProduceRequest(req)
	return c.send(ctx, addr, "commit_produce", payload, err)
}

// CommitProduceBatch commits a batch of accepted produce records on the peer
// at addr.
func (c *PeerClient) CommitProduceBatch(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeCommitProduceBatchRequest(req)
	return c.send(ctx, addr, "commit_produce_batch", payload, err)
}

// Consume forwards a consume request to the peer at addr.
func (c *PeerClient) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeConsumeRequest(req)
	return c.send(ctx, addr, "consume", payload, err)
}

// Ack forwards an ack request to the peer at addr.
func (c *PeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeAckRequest(req)
	return c.send(ctx, addr, "ack", payload, err)
}

// CreateTopic forwards a raw topic create body to the peer at addr.
func (c *PeerClient) CreateTopic(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Body: body})
	return c.send(ctx, addr, "create_topic", payload, err)
}

// AlterTopic forwards a raw topic alter body to the peer at addr.
func (c *PeerClient) AlterTopic(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: topicName, Body: body})
	return c.send(ctx, addr, "alter_topic", payload, err)
}

// DeleteTopic asks the peer at addr to delete the topic.
func (c *PeerClient) DeleteTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpDeleteTopic, "delete_topic", topicName)
}

// PurgeTopic asks the peer at addr to purge the topic's on-disk state.
func (c *PeerClient) PurgeTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpPurgeTopic, "purge_topic", topicName)
}

// TopicPartitionStats fetches one partition's stats from the peer at addr and
// validates that the peer answered for the requested partition.
func (c *PeerClient) TopicPartitionStats(ctx context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
	payload, err := nodewire.EncodeTopicPartitionStatsRequest(nodewire.TopicPartitionStatsRequest{
		Topic:     topicName,
		Partition: partition,
	})
	res, err := c.send(ctx, addr, "topic_partition_stats", payload, err)
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
func (c *PeerClient) RegisterMember(ctx context.Context, addr string, req nodewire.MemberRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeMemberRequest(req)
	return c.send(ctx, addr, "register_member", payload, err)
}

// CreateUser forwards a user create to the leader at addr.
func (c *PeerClient) CreateUser(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpCreateUser, nodewire.UserRequest{Body: body})
	return c.send(ctx, addr, "create_user", payload, err)
}

// UpdateUser forwards a user update to the leader at addr.
func (c *PeerClient) UpdateUser(ctx context.Context, addr, username string, body []byte) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpUpdateUser, nodewire.UserRequest{Username: username, Body: body})
	return c.send(ctx, addr, "update_user", payload, err)
}

// DeleteUser forwards a user delete to the leader at addr.
func (c *PeerClient) DeleteUser(ctx context.Context, addr, username string) (nodewire.Response, error) {
	payload, err := nodewire.EncodeUserRequest(nodewire.OpDeleteUser, nodewire.UserRequest{Username: username})
	return c.send(ctx, addr, "delete_user", payload, err)
}

func (c *PeerClient) topicNameRequest(ctx context.Context, addr string, op nodewire.Operation, operation, topicName string) (nodewire.Response, error) {
	payload, err := nodewire.EncodeTopicNameRequest(op, nodewire.TopicNameRequest{Topic: topicName})
	return c.send(ctx, addr, operation, payload, err)
}

// send performs the request round trip once the encode step succeeded.
func (c *PeerClient) send(ctx context.Context, addr, operation string, payload []byte, encodeErr error) (nodewire.Response, error) {
	if encodeErr != nil {
		return nodewire.Response{}, encodeErr
	}
	return c.request(ctx, addr, operation, payload)
}

func (c *PeerClient) request(ctx context.Context, addr, operation string, payload []byte) (nodewire.Response, error) {
	if c == nil || c.frames == nil {
		return nodewire.Response{}, fmt.Errorf("peer rpc client is nil")
	}
	frame, err := c.frames.RequestOnLane(ctx, addr, operation, clusterwire.StreamFrameNodeRequest, payload)
	if err != nil {
		return nodewire.Response{}, err
	}
	if frame.Type != clusterwire.StreamFrameNodeReply {
		return nodewire.Response{}, fmt.Errorf("unexpected peer rpc frame type %d", frame.Type)
	}
	return nodewire.DecodeResponse(frame.Payload)
}

func writePeerResponse(w http.ResponseWriter, res nodewire.Response) {
	if res.ContentType != "" {
		w.Header().Set("Content-Type", res.ContentType)
	}
	if res.Status == 0 {
		res.Status = http.StatusOK
	}
	w.WriteHeader(res.Status)
	if len(res.Body) > 0 {
		_, _ = w.Write(res.Body)
	}
}
