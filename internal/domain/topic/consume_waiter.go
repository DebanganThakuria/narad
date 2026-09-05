package topic

import (
	"context"
	"time"
)

// LocalConsumeWaiter is the local half of a raced long-poll (see the
// cluster router's RouteConsumeWait): Wait parks on the node's own
// partitions until ctx ends or wait elapses; Release gives back a
// message Wait returned but the race discarded (a nack of its handle).
// Defined here, below both the HTTP handlers and the cluster router, so
// the two share one type.
type LocalConsumeWaiter interface {
	Wait(ctx context.Context, wait time.Duration) (Message, bool, error)
	Release(ctx context.Context, msg Message) error
}
