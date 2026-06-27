// Package errs is the single source of truth for every named error
// sentinel in narad. Callers use errors.Is(err, errs.ErrXxx) to
// distinguish error categories; sub-packages return these values
// (often wrapped with %w) so the category is preserved through any
// number of layers.
//
// Error inventory by domain:
//
//	Topic management
//	  ErrTopicNotFound       topic does not exist
//	  ErrTopicAlreadyExists  CreateTopic: name taken
//
//	Message delivery
//	  ErrHandleMalformed     receipt handle cannot be decoded
//	  ErrHandleStale         reservation expired or already committed
//	  ErrAckedAheadFull      out-of-order ack set full; head stuck
//
//	Metastore
//	  ErrNotFound            record does not exist
//	  ErrAlreadyExists       record already exists
//
//	Partition log (storage)
//	  ErrOffsetNotFound      offset outside the retained range
//	  ErrCorruptRecord       frame failed CRC / decode validation
//	  ErrLogClosed           operation on a closed log
//
//	Schema registry
//	  ErrSchemaNotFound      no schema registered for topic
//	  ErrSchemaIncompatible  schema breaks backwards compatibility
//
//	Input validation
//	  ErrInvalidArgument     generic bad-input sentinel
//	  ErrPartitionRequired   replay-mode consume without partition
package errs

import "errors"

// -- topic management ----------------------------------------------------

var (
	ErrTopicNotFound      = errors.New("topic not found")
	ErrTopicAlreadyExists = errors.New("topic already exists")
)

// -- message delivery ----------------------------------------------------

var (
	ErrHandleMalformed = errors.New("receipt handle is malformed")
	ErrHandleStale     = errors.New("receipt handle no longer matches an active reservation")
	ErrAckedAheadFull  = errors.New("acked-ahead set is full; head of queue may be stuck")
)

// -- metastore -----------------------------------------------------------

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// -- storage / partition log ---------------------------------------------

var (
	ErrOffsetNotFound = errors.New("offset not found")
	ErrCorruptRecord  = errors.New("corrupt record")
	ErrLogClosed      = errors.New("log closed")
)

// -- schema registry -----------------------------------------------------

var (
	ErrSchemaNotFound     = errors.New("schema not registered for topic")
	ErrSchemaIncompatible = errors.New("schema incompatible with previous version")
)

// -- input validation ----------------------------------------------------

var (
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrPartitionRequired = errors.New("partition required for replay-mode consume")
	ErrNotPartitionOwner = errors.New("this node does not own the requested partition")
)
