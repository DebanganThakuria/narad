// Package errs holds the broker-wide error sentinels shared across
// the topics, messaging, and runtime subpackages.
//
// Subpackages return these sentinels (often wrapped with %w + a
// human-readable detail) so the parent broker package and the HTTP
// transport layer can map them with a single errors.Is check
// regardless of which manager generated the error.
package errs

import "errors"

var (
	// TopicNotFound is returned when an operation references a topic
	// the metastore has no record of.
	TopicNotFound = errors.New("topic not found")

	// TopicAlreadyExists is returned by CreateTopic when a topic with
	// the same name is already registered.
	TopicAlreadyExists = errors.New("topic already exists")

	// InvalidArgument is the generic "caller-supplied input is bad"
	// sentinel. Subpackages wrap it with a specific reason via %w.
	InvalidArgument = errors.New("invalid argument")

	// PartitionRequired is returned when Consume is called in
	// replay-by-offset mode without specifying a partition.
	PartitionRequired = errors.New("partition required")
)
