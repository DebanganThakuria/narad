package topic

import (
	"context"
	"time"
)

// LocalConsumeWaiter is the local half of a raced long-poll (see the
// cluster router's RouteConsumeWait). Defined here, below both the HTTP
// handlers and the cluster router, so the two share one type.
//
// Wait parks on the node's own partitions until one of four things
// happens: a local record arrives, the external channel fires, wait
// elapses, or ctx ends. Taking the external channel here rather than
// racing Wait in a second goroutine is deliberate: the cross-node half
// of a consume is a channel too, so folding it into the same select
// means a parked consumer costs ONE goroutine instead of two. With
// thousands of consumers parked on a gateway node that difference is
// the bulk of the per-waiter cost.
//
// external may be nil, in which case Wait behaves as an ordinary local
// long-poll. It reports whether the external channel is what woke it,
// which the caller needs in order to tell "nothing arrived" apart from
// "the cross-node side has something for you".
//
// Release gives back a message Wait returned but the race discarded (a
// nack of its handle).
type LocalConsumeWaiter interface {
	Wait(ctx context.Context, wait time.Duration, external <-chan struct{}) (msg Message, found bool, wokeExternal bool, err error)
	Release(ctx context.Context, msg Message) error
}
