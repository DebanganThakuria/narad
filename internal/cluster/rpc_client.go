package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

const defaultPeerRPCTimeout = 5 * time.Second

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
}

type PeerClient struct {
	frames  *replication.QUICFrameClient
	metrics stageObserver
}

func NewPeerClient(timeout time.Duration, observers ...stageObserver) *PeerClient {
	if timeout <= 0 {
		timeout = defaultPeerRPCTimeout
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &PeerClient{frames: replication.NewQUICFrameClient(timeout, observer), metrics: observer}
}

func (c *PeerClient) Produce(ctx context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeProduceRequest(req)
	c.observe("produce", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "produce", payload)
}

func (c *PeerClient) CommitProduce(ctx context.Context, addr string, req nodewire.CommitProduceRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeCommitProduceRequest(req)
	c.observe("commit_produce", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "commit_produce", payload)
}

func (c *PeerClient) CommitProduceBatch(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeCommitProduceBatchRequest(req)
	c.observe("commit_produce_batch", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "commit_produce_batch", payload)
}

func (c *PeerClient) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeConsumeRequest(req)
	c.observe("consume", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "consume", payload)
}

func (c *PeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeAckRequest(req)
	c.observe("ack", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "ack", payload)
}

func (c *PeerClient) CreateTopic(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpCreateTopic, nodewire.TopicBodyRequest{Body: body})
	c.observe("create_topic", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "create_topic", payload)
}

func (c *PeerClient) AlterTopic(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: topicName, Body: body})
	c.observe("alter_topic", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "alter_topic", payload)
}

func (c *PeerClient) DeleteTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpDeleteTopic, topicName)
}

func (c *PeerClient) PurgeTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	return c.topicNameRequest(ctx, addr, nodewire.OpPurgeTopic, topicName)
}

func (c *PeerClient) TopicPartitionStats(ctx context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
	start := time.Now()
	payload, err := nodewire.EncodeTopicPartitionStatsRequest(nodewire.TopicPartitionStatsRequest{
		Topic:     topicName,
		Partition: partition,
	})
	c.observe("topic_partition_stats", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return topic.PartitionStats{}, err
	}
	res, err := c.request(ctx, addr, "topic_partition_stats", payload)
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

func (c *PeerClient) RegisterMember(ctx context.Context, addr string, req nodewire.MemberRequest) (nodewire.Response, error) {
	start := time.Now()
	payload, err := nodewire.EncodeMemberRequest(req)
	c.observe("register_member", "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, "register_member", payload)
}

func (c *PeerClient) topicNameRequest(ctx context.Context, addr string, op nodewire.Operation, topicName string) (nodewire.Response, error) {
	operation := nodeOperationName(op)
	start := time.Now()
	payload, err := nodewire.EncodeTopicNameRequest(op, nodewire.TopicNameRequest{Topic: topicName})
	c.observe(operation, "encode", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	return c.request(ctx, addr, operation, payload)
}

func (c *PeerClient) request(ctx context.Context, addr, operation string, payload []byte) (nodewire.Response, error) {
	if c == nil || c.frames == nil {
		return nodewire.Response{}, fmt.Errorf("peer rpc client is nil")
	}
	start := time.Now()
	frame, err := c.frames.RequestOnLane(ctx, addr, operation, replicationwire.StreamFrameNodeRequest, payload)
	c.observe(operation, "round_trip", observeOutcome(err), time.Since(start))
	if err != nil {
		return nodewire.Response{}, err
	}
	if frame.Type != replicationwire.StreamFrameNodeReply {
		return nodewire.Response{}, fmt.Errorf("unexpected peer rpc frame type %d", frame.Type)
	}
	start = time.Now()
	res, err := nodewire.DecodeResponse(frame.Payload)
	c.observe(operation, "decode_response", observeOutcome(err), time.Since(start))
	return res, err
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
