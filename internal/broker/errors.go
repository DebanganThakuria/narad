package broker

import "errors"

// Sentinel errors callers may want to discriminate on.
var (
	ErrTopicNotFound      = errors.New("broker: topic not found")
	ErrTopicAlreadyExists = errors.New("broker: topic already exists")
	ErrInvalidArgument    = errors.New("broker: invalid argument")
	ErrPartitionRequired  = errors.New("broker: partition is required when offset is set")
)
