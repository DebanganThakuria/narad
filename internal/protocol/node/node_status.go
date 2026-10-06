package node

import "time"

// The node-status probe carries no fields: the operation byte is the
// whole request. An operator surface (the leader's decommission pass,
// GET /v1/cluster/members?detail=true) sends it to one member and gets
// back what that member observes about itself as a JSON NodeStatus:
// whether it believes it is draining, how many client produce requests
// it is still answering, how many records its ingress WAL still has to
// hand to their owners, the partition copies it has set
// aside, and the moves it runs as the destination. Pulling this keeps it
// out of the Raft log.
//
// Mixed versions: a node that predates the operation (3.0.x) answers 400
// "unsupported rpc operation"; callers treat that as "status unknown",
// never as unhealthy. A reply from a newer node may carry fields this
// release does not know; decoding ignores them.

// NodeStatus is one node's answer to OpNodeStatus.
type NodeStatus struct {
	// Node is the answering node's ID.
	Node string `json:"node"`
	// Draining is the node's own replica's view of its drain flag.
	Draining bool `json:"draining"`
	// ProduceInFlight is how many client produce requests the node
	// admitted and has not answered yet. Read after Draining and before
	// DispatchBacklog: a draining node reporting 0 here has nothing left
	// on its way into its ingress WAL.
	ProduceInFlight int64 `json:"produce_in_flight"`
	// DispatchBacklog is how many records the node's ingress WAL
	// accepted and has not yet handed to their owners. A node must
	// reach zero before it leaves Raft.
	DispatchBacklog uint64 `json:"dispatch_backlog"`
	// Quarantine is the node's last inventory of partition copies it set
	// aside instead of deleting.
	Quarantine QuarantineStatus `json:"quarantine"`
	// Moves are the moves this node runs as the destination.
	Moves []MoveState `json:"moves"`
}

// QuarantineStatus summarizes a node's quarantined copies.
type QuarantineStatus struct {
	// Copies and Bytes total every copy found.
	Copies int   `json:"copies"`
	Bytes  int64 `json:"bytes"`
	// List names the first copies found (MaxStatusQuarantineList).
	List []QuarantinedCopy `json:"list"`
}

// MaxStatusQuarantineList bounds QuarantineStatus.List.
const MaxStatusQuarantineList = 100

// QuarantinedCopy is one set-aside copy on a node.
type QuarantinedCopy struct {
	Kind  string `json:"kind"`
	Topic string `json:"topic"`
	// Partition is the partition index, or -1 for a whole topic
	// directory.
	Partition int       `json:"partition"`
	Dir       string    `json:"dir"`
	Bytes     int64     `json:"bytes"`
	ModTime   time.Time `json:"mod_time"`
}

// MoveState is one move a node runs as the destination.
type MoveState struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Source    string    `json:"source"`
	Target    string    `json:"target"`
	StartedAt time.Time `json:"started_at"`
	// Phase is what the move worker is doing (copying, frozen,
	// flip_pending, waiting_for_source, blocked).
	Phase string `json:"phase"`
	// Attempts counts copy attempts against a live source.
	Attempts int `json:"attempts"`
	// LastError is the last thing that failed, kept until the next one.
	LastError string `json:"last_error,omitempty"`
	// CopiedBytes is how many bytes the current copy fetched from the
	// source; a copy started again from scratch counts from 0.
	CopiedBytes int64 `json:"copied_bytes"`
	// Blocked says why the move cannot finish on its own, empty while
	// it can.
	Blocked string `json:"blocked,omitempty"`
}

// EncodeNodeStatusRequest encodes the payload-free node-status probe.
func EncodeNodeStatusRequest() []byte {
	return opWriter(OpNodeStatus, 0).finish()
}

// DecodeNodeStatusRequest verifies a node-status probe payload.
func DecodeNodeStatusRequest(payload []byte) error {
	r, err := opReader(payload, OpNodeStatus)
	if err != nil {
		return err
	}
	return r.done()
}
