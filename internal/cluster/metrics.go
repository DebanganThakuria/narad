package cluster

import (
	"net/http"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type stageObserver interface {
	ObserveHotPathStage(component, operation, stage, outcome string, duration time.Duration)
}

func (rt *Router) observe(operation, stage, outcome string, duration time.Duration) {
	if rt.metrics == nil {
		return
	}
	rt.metrics.ObserveHotPathStage("router", operation, stage, outcome, duration)
}

func (c *PeerClient) observe(operation, stage, outcome string, duration time.Duration) {
	if c == nil || c.metrics == nil {
		return
	}
	c.metrics.ObserveHotPathStage("peer_rpc", operation, stage, outcome, duration)
}

func (s *RPCServer) observe(operation, stage, outcome string, duration time.Duration) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.ObserveHotPathStage("peer_rpc_server", operation, stage, outcome, duration)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func responseOutcome(status int) string {
	switch {
	case status >= http.StatusInternalServerError:
		return "error"
	case status >= http.StatusBadRequest:
		return "rejected"
	default:
		return "ok"
	}
}

func nodeOperationName(op nodewire.Operation) string {
	switch op {
	case nodewire.OpProduce:
		return "produce"
	case nodewire.OpConsume:
		return "consume"
	case nodewire.OpAck:
		return "ack"
	case nodewire.OpCreateTopic:
		return "create_topic"
	case nodewire.OpAlterTopic:
		return "alter_topic"
	case nodewire.OpDeleteTopic:
		return "delete_topic"
	case nodewire.OpPurgeTopic:
		return "purge_topic"
	case nodewire.OpTopicPartitionStats:
		return "topic_partition_stats"
	case nodewire.OpRegisterMember:
		return "register_member"
	case nodewire.OpCommitProduce:
		return "commit_produce"
	case nodewire.OpCommitProduceBatch:
		return "commit_produce_batch"
	default:
		return "unknown"
	}
}
