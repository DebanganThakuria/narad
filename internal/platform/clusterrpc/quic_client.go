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
// that require a cluster secret.
func NewQUICFrameClient(timeout time.Duration, secret string) *QUICFrameClient {
	return &QUICFrameClient{pool: newQUICClientPool(timeout, secret)}
}

// Request sends one request frame to addr on the lane implied by the
// frame type and waits for the correlated reply.
func (c *QUICFrameClient) Request(ctx context.Context, addr string, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	if c == nil || c.pool == nil {
		return clusterwire.StreamFrame{}, errors.New("quic frame client is nil")
	}
	return c.pool.request(ctx, addr, quicLaneForFrame(frameType), frameType, payload)
}

// RequestOnLane is Request with an explicit lane ("produce", "consume",
// "ack", or "control"; anything else falls back to "control"). Lanes
// keep bulk traffic from head-of-line blocking control RPCs.
func (c *QUICFrameClient) RequestOnLane(ctx context.Context, addr, lane string, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	if c == nil || c.pool == nil {
		return clusterwire.StreamFrame{}, errors.New("quic frame client is nil")
	}
	return c.pool.request(ctx, addr, lane, frameType, payload)
}

// quicLaneForFrame routes a frame type to its default lane. Every
// current frame type rides the control lane; callers with bulk traffic
// pick a lane explicitly via RequestOnLane.
func quicLaneForFrame(clusterwire.StreamFrameType) string {
	return "control"
}
