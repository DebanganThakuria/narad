// Package node defines the node-to-node RPC wire format carried as
// opaque payloads inside clusterwire frames. Every request payload
// starts with a one-byte Operation followed by the request fields;
// multi-byte integers are big-endian and strings/byte slices are
// length-prefixed with a uint32.
package node

import "io"

// ContentTypeJSON is the Response.ContentType for JSON bodies.
const ContentTypeJSON = "application/json"

// Operation is the one-byte request discriminator that leads every
// node-RPC payload.
type Operation uint8

// Operations. Values are stable on the wire.
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

// ProduceRequest asks a node to route and append one record.
type ProduceRequest struct {
	Topic     string
	Key       string
	Partition int
	Payload   []byte
}

// CommitProduceRequest asks the owner of TargetPartition to durably
// commit one already-routed record.
type CommitProduceRequest struct {
	Topic           string
	Key             string
	TargetPartition int
	Payload         []byte
	CreatedAtUnixMs int64
}

// CommitProduceBatchRequest carries multiple commit-produce records in
// one RPC.
type CommitProduceBatchRequest struct {
	Records []CommitProduceRequest
}

// ConsumeRequest asks a node for the next available record. Partition
// and Offset are only meaningful when their Has* flags are set;
// LocalOnly stops the node from forwarding to other owners.
type ConsumeRequest struct {
	Topic        string
	Partition    int
	HasPartition bool
	Offset       int64
	HasOffset    bool
	WaitNanos    int64
	LocalOnly    bool
}

// AckRequest acknowledges a reserved record identified by its receipt
// coordinates.
type AckRequest struct {
	Topic     string
	Partition int
	Offset    int64
	Nonce     int64
}

// TopicBodyRequest is the shared shape for topic operations that carry
// an opaque body (create, alter).
type TopicBodyRequest struct {
	Topic string
	Body  []byte
}

// TopicNameRequest is the shared shape for topic operations that need
// only the topic name (delete, purge).
type TopicNameRequest struct {
	Topic string
}

// TopicPartitionStatsRequest asks the owner of one partition for its
// runtime stats.
type TopicPartitionStatsRequest struct {
	Topic     string
	Partition int
}

// MemberRequest registers or refreshes a cluster member.
type MemberRequest struct {
	ID            string
	Addr          string
	ClusterAddr   string
	Status        string
	LastHeartbeat int64
}

// OperationOf returns the Operation a request payload starts with,
// without decoding the rest of the payload.
func OperationOf(payload []byte) (Operation, error) {
	if len(payload) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return Operation(payload[0]), nil
}
