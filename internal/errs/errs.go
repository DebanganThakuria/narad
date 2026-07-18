// Package errs defines the named error sentinels shared across narad.
// Sub-packages return these values (usually wrapped with %w) so callers
// can classify failures with errors.Is no matter how many layers the
// error crossed.
package errs

import "errors"

// Topic management.
var (
	// ErrTopicNotFound reports that the named topic does not exist.
	ErrTopicNotFound = errors.New("topic not found")

	// ErrTopicAlreadyExists reports that the requested topic name is
	// already taken.
	ErrTopicAlreadyExists = errors.New("topic already exists")
)

// Message delivery.
var (
	// ErrHandleMalformed reports a receipt handle that cannot be decoded.
	ErrHandleMalformed = errors.New("receipt handle is malformed")

	// ErrHandleStale reports a receipt handle whose reservation has
	// expired or was already committed.
	ErrHandleStale = errors.New("receipt handle no longer matches an active reservation")

	// ErrAckedAheadFull reports that the out-of-order ack set is full,
	// meaning the head of the queue may be stuck on a poison message.
	ErrAckedAheadFull = errors.New("acked-ahead set is full; head of queue may be stuck")
)

// Metastore.
var (
	// ErrNotFound reports that a metastore record does not exist.
	ErrNotFound = errors.New("not found")

	// ErrAlreadyExists reports that a metastore record already exists.
	ErrAlreadyExists = errors.New("already exists")

	// ErrUnavailable reports that a control-plane write could not be
	// committed right now because there is no leader, leadership was
	// lost mid-apply, or the leader could not be reached — all
	// transient during elections, partitions, and rolling restarts.
	// It maps to 503 so clients retry instead of treating it as a bug.
	ErrUnavailable = errors.New("control plane temporarily unavailable")
)

// Partition log (storage).
var (
	// ErrOffsetNotFound reports an offset outside the retained range.
	ErrOffsetNotFound = errors.New("offset not found")

	// ErrCorruptRecord reports a frame that failed CRC or decode
	// validation.
	ErrCorruptRecord = errors.New("corrupt record")

	// ErrLogClosed reports an operation on a closed partition log.
	ErrLogClosed = errors.New("log closed")
)

// Schema registry.
var (
	// ErrSchemaNotFound reports that no schema is registered for the
	// topic.
	ErrSchemaNotFound = errors.New("schema not registered for topic")

	// ErrSchemaIncompatible reports a schema that breaks backwards
	// compatibility with the previous version.
	ErrSchemaIncompatible = errors.New("schema incompatible with previous version")
)

// Fan-out (parent/child topic links).
var (
	// ErrFanoutRoleConflict reports an attach that would violate the
	// fan-out role invariants: roles are exclusive (a parent is never a
	// child and vice versa), fan-out is depth 1, and a child has
	// exactly one parent.
	ErrFanoutRoleConflict = errors.New("fan-out role conflict")

	// ErrFanoutChildLimit reports an attach to a parent that already
	// has the maximum number of children.
	ErrFanoutChildLimit = errors.New("parent has reached the maximum number of children")

	// ErrFanoutSchemaMismatch reports an attach whose child schema is
	// neither absent nor identical to the parent's.
	ErrFanoutSchemaMismatch = errors.New("child schema incompatible with parent")

	// ErrFanoutSchemaManaged reports a schema change on an attached
	// child; its schema is parent-managed until detach.
	ErrFanoutSchemaManaged = errors.New("schema is parent-managed while attached to a fan-out parent")

	// ErrFanoutDelayTooLong reports a child delay the parent's
	// retention cannot buffer: the parent must retain records for the
	// delay plus the minimum outage-tolerance floor. Returned both at
	// attach and when shrinking a parent's retention below an attached
	// child's requirement.
	ErrFanoutDelayTooLong = errors.New("delay exceeds what the parent's retention can buffer")

	// ErrDelayedChildProduce reports a direct produce to a delay
	// child. Delayed children only receive records through fan-out, so
	// the delay holds for every record in the topic.
	ErrDelayedChildProduce = errors.New("direct produce to a delayed child topic is not allowed")
)

// Input validation and routing.
var (
	// ErrInvalidArgument is the generic bad-input sentinel.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrPartitionRequired reports a replay-mode consume that did not
	// name a partition.
	ErrPartitionRequired = errors.New("partition required for replay-mode consume")

	// ErrNotPartitionOwner reports a request routed to a node that does
	// not own the requested partition.
	ErrNotPartitionOwner = errors.New("this node does not own the requested partition")
)
