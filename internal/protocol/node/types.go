package node

import "io"

const (
	ContentTypeJSON = "application/json"
)

type Operation uint8

const (
	OpProduce Operation = iota + 1
	OpConsume
	OpAck
	OpCreateTopic
	OpAlterTopic
	OpDeleteTopic
	OpPurgeTopic
	OpTopicPartitionStats
	OpRegisterMember
	OpCommitProduce
	OpCommitProduceBatch
)

type ProduceRequest struct {
	Topic     string
	Key       string
	Partition int
	Payload   []byte
}

type CommitProduceRequest struct {
	Topic           string
	Key             string
	TargetPartition int
	Payload         []byte
	CreatedAtUnixMs int64
}

type CommitProduceBatchRequest struct {
	Records []CommitProduceRequest
}

type ConsumeRequest struct {
	Topic        string
	Partition    int
	HasPartition bool
	Offset       int64
	HasOffset    bool
	WaitNanos    int64
	LocalOnly    bool
}

type AckRequest struct {
	Topic     string
	Partition int
	Offset    int64
	Nonce     int64
}

type TopicBodyRequest struct {
	Topic string
	Body  []byte
}

type TopicNameRequest struct {
	Topic string
}

type TopicPartitionStatsRequest struct {
	Topic     string
	Partition int
}

type MemberRequest struct {
	ID            string
	Addr          string
	ClusterAddr   string
	Status        string
	LastHeartbeat int64
}

func OperationOf(payload []byte) (Operation, error) {
	if len(payload) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return Operation(payload[0]), nil
}
