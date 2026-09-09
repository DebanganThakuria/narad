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
	OpCreateUser
	OpUpdateUser
	OpDeleteUser
	OpAttachChild
	OpDetachChild
	OpFanoutCursors
	OpExtendAck
	OpNack
	OpGetTopic
	OpJoinCluster
	OpListPartitionSegments
	OpFetchSegmentChunk
	OpPrepareHandoff
	OpDecommissionMember
	OpCompleteMove
	OpAbortMove
	OpGetAssignment
	OpAppliedIndex
	OpTokenRegister
	OpTokenNotify
)

// CompleteMoveRequest asks the leader to perform the guarded ownership flip
// for a partition move (set Owner=TargetID iff Owner is still ExpectedOwner
// and Target is still TargetID). Forwarded from the destination node, which
// is often not the leader.
type CompleteMoveRequest struct {
	Topic         string
	Partition     int
	ExpectedOwner string
	TargetID      string
}

// AbortMoveRequest asks the leader to clear a move target (iff it still
// matches ExpectedTarget). Forwarded from the destination node.
type AbortMoveRequest struct {
	Topic          string
	Partition      int
	ExpectedTarget string
}

// JoinClusterRequest asks the metastore leader to admit a new node into
// the Raft voter set (scale-out). ID is the joining node's identity and
// ClusterAddr its advertised Raft address. Fresh declares that the
// joiner starts from an EMPTY data directory: the leader admits a
// decommissioned (tombstoned) ID again only when its old state is gone,
// so a removed pod that is merely still running, or restarted with its
// old volume, cannot undo its own decommission.
type JoinClusterRequest struct {
	ID          string
	ClusterAddr string
	Fresh       bool
}

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
// only the topic name (delete, purge). ID optionally names the topic
// incarnation the operation is about (topic.Topic.ID): a purge carries
// the deleted record's ID so the receiver purges that incarnation's
// directory and no other. It is encoded only when set, as a trailing
// field, so a sender without one produces the pre-ID payload.
type TopicNameRequest struct {
	Topic string
	ID    string
}

// ChildLinkRequest is the shared shape for fan-out attach and detach,
// forwarded to the cluster leader. DelayMs is meaningful on attach
// only.
type ChildLinkRequest struct {
	Parent  string
	Child   string
	DelayMs int64
}

// TopicPartitionStatsRequest asks the owner of one partition for its
// runtime stats.
type TopicPartitionStatsRequest struct {
	Topic     string
	Partition int
}

// UserRequest is the shared shape for user write operations forwarded
// to the leader. Body is the JSON-encoded user.User for create/update
// and empty for delete; Username identifies the target for update and
// delete.
type UserRequest struct {
	Username string
	Body     []byte
}

// MemberRequest registers or refreshes a cluster member.
type MemberRequest struct {
	ID            string
	Addr          string
	ClusterAddr   string
	Status        string
	LastHeartbeat int64
}

// PartitionSegmentsRequest asks the owner of (Topic, Partition) for its
// segment list and durable positions, for a rebalance copy.
type PartitionSegmentsRequest struct {
	Topic     string
	Partition int
}

// FetchSegmentChunkRequest asks the owner of (Topic, Partition) for a
// bounded byte range of one segment file (identified by its base
// offset), for a rebalance copy.
type FetchSegmentChunkRequest struct {
	Topic      string
	Partition  int
	BaseOffset int64
	At         int64
	Length     int64
}

// PrepareHandoffRequest asks the owner of (Topic, Partition) to freeze
// the partition for a rebalance handoff and return its final positions.
type PrepareHandoffRequest struct {
	Topic          string
	Partition      int
	FreezeTTLNanos int64
	// FreezeToken, when set, asks the owner to EXTEND the freeze that
	// token names rather than arm a fresh one, and to fail if that freeze
	// lapsed. Encoded only when non-empty, so a request without it is
	// byte-identical to the pre-token format and older owners keep
	// decoding it.
	FreezeToken string
}

// DecommissionRequest asks the leader to mark a member draining (Cancel
// false) or to clear the drain (Cancel true), forwarded from any node.
type DecommissionRequest struct {
	ID     string
	Cancel bool
}

// GetAssignmentRequest asks a node (in practice the leader, for
// authoritative confirmation) for the current assignment of one
// partition. The response body is the metastore Assignment as JSON.
type GetAssignmentRequest struct {
	Topic     string
	Partition int
}

// OperationOf returns the Operation a request payload starts with,
// without decoding the rest of the payload.
func OperationOf(payload []byte) (Operation, error) {
	if len(payload) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return Operation(payload[0]), nil
}

// TokenDelta is one batched update to the standing interest this node
// has registered with a peer. Adds and drops travel together so a
// consumer that was served elsewhere can retire its unused interest in
// a frame that was already going out, rather than paying an RPC per
// stale token.
//
// A token reserves nothing. It says only "I am here, tell me if records
// show up", which is why losing one costs a round trip and never
// correctness.
type TokenDelta struct {
	// From is the sender's own node address, self-advertised the same
	// way RegisterMember does it, so the owner can reach back through
	// the normal peer client to spend a token.
	From string
	// Add carries the topics this node now wants to hear about.
	Add []TokenRegistration
	// Drop carries topics it no longer wants. Best effort: a lost drop
	// costs one wasted notification that the peer declines.
	Drop []string
}

// TokenRegistration is one topic's worth of interest.
type TokenRegistration struct {
	Topic string
	// TTLNanos is how much longer the interest is useful, expressed as a
	// DURATION rather than a deadline: the sender subtracts on its own
	// clock and the receiver adds on its own, so clock skew between the
	// two can never expire a live consumer's token early.
	TTLNanos int64
}

// TokenNotifyRequest tells a peer that records may be available for a
// topic it holds a token on.
//
// It deliberately carries no count. The receiver's next move is a single
// ordinary consume either way, and any number the owner put here would
// be an estimate that a racing claim can invalidate before the frame
// lands. The frame says only "look now", and the claim finds out.
type TokenNotifyRequest struct {
	// From is the owner's own node address. The claim has to be aimed at
	// the node that actually has the record, so the notification says who
	// is calling rather than making the receiver guess or ask everyone.
	From  string
	Topic string
}

// TokenNotifyReply is the peer's verdict. Claiming false is a "pass":
// the interest is spent and the owner should offer the record to
// somebody else immediately rather than waiting out a deadline.
type TokenNotifyReply struct {
	Claiming bool
}
