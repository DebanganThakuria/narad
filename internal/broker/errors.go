package broker

import "github.com/debanganthakuria/narad/internal/broker/errs"

// Public broker error sentinels — aliases of the shared values in
// the errs subpackage. Subpackages return errs.* directly; callers
// compare against these for ergonomics.
var (
	ErrTopicNotFound      = errs.TopicNotFound
	ErrTopicAlreadyExists = errs.TopicAlreadyExists
	ErrInvalidArgument    = errs.InvalidArgument
	ErrPartitionRequired  = errs.PartitionRequired
)
