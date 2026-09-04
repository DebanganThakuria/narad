// Package clusterrpc is Narad's node-to-node RPC transport:
// length-framed request/reply frames (internal/protocol/clusterwire)
// multiplexed over pooled QUIC streams. The client correlates replies
// by RequestID so many in-flight RPCs share one stream; the server
// answers each frame on the stream it arrived on.
package clusterrpc

import (
	"context"
	"errors"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// Lane selects which group of pooled streams a request rides. Lanes keep
// bulk traffic (produce commits, consumes, acks) from head-of-line
// blocking each other and the light control RPCs. It is a small integer
// rather than a string so the per-request lookup is a table index.
type Lane uint8

// Lanes. LaneControl is the zero value so an unset lane is the safe
// default; values at or beyond laneCount normalize to it.
const (
	LaneControl Lane = iota
	LaneProduce
	LaneConsume
	LaneAck
	laneCount
)

// laneWidths is the number of streams per lane. Bulk lanes are wide so
// concurrent produces, consumes, and acks spread across streams; control
// traffic is light and gets a few.
var laneWidths = [laneCount]int{
	LaneControl: quicControlLanes,
	LaneProduce: quicProduceLanes,
	LaneConsume: quicConsumeLanes,
	LaneAck:     quicAckLanes,
}

var laneNames = [laneCount]string{
	LaneControl: "control",
	LaneProduce: "produce",
	LaneConsume: "consume",
	LaneAck:     "ack",
}

// normalize maps any out-of-range value to LaneControl.
func (l Lane) normalize() Lane {
	if l >= laneCount {
		return LaneControl
	}
	return l
}

// width is the number of pooled streams for the lane.
func (l Lane) width() int {
	return laneWidths[l.normalize()]
}

func (l Lane) String() string {
	return laneNames[l.normalize()]
}

// QUICFrameClient sends cluster-RPC request frames to peer nodes over
// pooled QUIC streams and returns the matching reply frames. It is safe
// for concurrent use; a nil client fails every request instead of
// panicking.
type QUICFrameClient struct {
	pool *quicClientPool
}

// NewQUICFrameClient returns a client whose reply waits and dials fall
// back to timeout when the caller's context carries no deadline. A
// non-positive timeout selects the package default. secret, when
// non-empty, is presented on every new stream to authenticate to peers
// that require a cluster secret; it also derives the QUIC stateless
// reset key, so the client and its peers can recognise each other's
// resets across restarts.
func NewQUICFrameClient(timeout time.Duration, secret string) *QUICFrameClient {
	return &QUICFrameClient{pool: newQUICClientPool(timeout, secret)}
}

// Request sends one request frame to addr on the control lane and waits
// for the correlated reply.
func (c *QUICFrameClient) Request(ctx context.Context, addr string, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return c.RequestOnLane(ctx, addr, LaneControl, frameType, payload)
}

// RequestOnLane is Request with an explicit lane. Unknown lane values
// fall back to LaneControl.
func (c *QUICFrameClient) RequestOnLane(ctx context.Context, addr string, lane Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	if c == nil || c.pool == nil {
		return clusterwire.StreamFrame{}, errors.New("quic frame client is nil")
	}
	return c.pool.request(ctx, addr, lane, frameType, payload)
}

// Close tears down every pooled connection and the client's UDP socket.
// In-flight requests fail; the client must not be used afterwards.
func (c *QUICFrameClient) Close() error {
	if c == nil || c.pool == nil {
		return nil
	}
	return c.pool.close()
}
