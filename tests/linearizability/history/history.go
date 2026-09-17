// Package history defines the operation history that the load driver
// writes and the linearizability checker reads.
//
// A history is the record of every operation a client performed against
// the broker, with the interval each one was in flight for. It exists
// because a counter cannot answer the question that matters after a
// chaotic run. A counter says a message was redelivered after its ack;
// only a history says whether any ordering of the concurrent calls could
// explain that, and whether it happened while a node was down. The
// history also outlives the run, so a verdict can be re-derived later
// under a stricter model, or disputed, without repeating the run that
// produced it.
//
// The format is JSONL: one record per line, appended as operations
// complete, so a run that is killed still leaves a readable prefix.
//
// Timestamps are wall-clock UnixNano rather than an offset from a
// monotonic base, because the fault injector is a shell script in a
// different process and its records have to interleave with the
// driver's. One host, one clock, and a run measured in minutes: a step
// from clock discipline would have to be larger than the intervals here
// to change a verdict.
package history

// Op is the kind of operation a record describes.
type Op string

const (
	// OpProduce is a produce request. Its outcome decides whether the
	// broker is known to hold the message.
	OpProduce Op = "produce"
	// OpDeliver is a consume that handed back a message. An empty poll is
	// not an operation on any message and is not recorded.
	OpDeliver Op = "deliver"
	// OpAck is an ack request.
	OpAck Op = "ack"
	// OpFault is written by the fault injector, not the driver. Fault
	// windows are what let the checker separate a redelivery explained by
	// a broker that was being killed or partitioned from one nobody has
	// accounted for.
	OpFault Op = "fault"
	// OpMeta carries run-level context: the run id, the topics, the
	// visibility timeout. Written once, first.
	OpMeta Op = "meta"
)

// Outcome records what the caller learned, which is not always what
// happened. An ambiguous operation is one whose request failed in a way
// that leaves the broker's state unknown to the client, and the model
// must not assume either answer.
type Outcome string

const (
	// OutcomeOK means the broker gave the success status for this
	// operation: 202 for a produce, 200 for a delivery, 204 for an ack.
	OutcomeOK Outcome = "ok"
	// OutcomeRejected means the broker answered, and said no. A 429 or
	// 503 produce was not accepted; a 410 ack found the lease already
	// lapsed.
	OutcomeRejected Outcome = "rejected"
	// OutcomeAmbiguous means the request errored in flight. The broker may
	// or may not have applied it.
	OutcomeAmbiguous Outcome = "ambiguous"
)

// Fault kinds. The checker treats them identically; they are
// distinguished for the report, so a reader can tell which kind of fault
// a given anomaly sat inside.
const (
	// FaultKill is a broker process stopped and restarted.
	FaultKill = "kill"
	// FaultPartition is a broker cut off from its peers on the cluster
	// plane while it keeps serving clients.
	FaultPartition = "partition"
)

// Record is one line of the history.
type Record struct {
	Op Op `json:"op"`
	// Msg is the message id, for produce, deliver and ack.
	Msg string `json:"msg,omitempty"`
	// Path is the topic the operation happened on. For a fan-out child
	// this differs from the topic the message was produced to, which is
	// why the checker keys by (msg, path) and not by msg alone: the same
	// message is a separate delivery obligation on every path.
	Path string `json:"path,omitempty"`
	// Client is the worker index, used only to lay out a visualization.
	Client int `json:"client,omitempty"`
	// Call and Ret bound the interval the operation was in flight, in
	// wall-clock UnixNano. The interval is closed at both ends.
	Call int64 `json:"call"`
	Ret  int64 `json:"ret"`
	// Status is the HTTP status, 0 when the request never got one.
	Status  int     `json:"status,omitempty"`
	Outcome Outcome `json:"outcome,omitempty"`

	// Fault records only. Kind is one of the Fault* constants; Target
	// names the broker.
	Kind   string `json:"kind,omitempty"`
	Target string `json:"target,omitempty"`

	// Meta records only.
	RunID  string   `json:"run_id,omitempty"`
	Topics []string `json:"topics,omitempty"`
	// Paths declares, per topic, every path a message produced to it is
	// expected to be delivered on: the topic itself plus any fan-out
	// child. Declaring it matters more than it sounds. If the checker
	// instead inferred paths from the deliveries it observed, then a
	// message delivered on a topic it was never produced to would define
	// itself as legal, and a broker misrouting messages between topics
	// would read as a clean run.
	Paths               map[string][]string `json:"paths,omitempty"`
	VisibilityTimeoutMs int64               `json:"visibility_timeout_ms,omitempty"`
}

// ProduceOutcome classifies a produce. A transport error leaves the
// broker's state unknown: it may hold the record and deliver it later,
// so the checker must not treat the message as absent.
func ProduceOutcome(status int, err error) Outcome {
	switch {
	case err != nil:
		return OutcomeAmbiguous
	case status == 202:
		return OutcomeOK
	default:
		// 429 or 503: the broker answered, and it does not have it.
		return OutcomeRejected
	}
}

// AckOutcome classifies an ack. Only a 204 proves the lease was live and
// the message is done. A 410 says the lease had already lapsed, so this
// delivery never acked anything and the message is coming back; an
// errored ack may or may not have landed. The checker excludes both from
// the model rather than guessing, which is what keeps it free of false
// positives.
func AckOutcome(status int, err error) Outcome {
	switch {
	case err != nil:
		return OutcomeAmbiguous
	case status == 204:
		return OutcomeOK
	default:
		return OutcomeRejected
	}
}
