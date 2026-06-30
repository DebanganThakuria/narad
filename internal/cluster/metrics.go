package cluster

import (
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

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
