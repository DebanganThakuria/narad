package cluster

import (
	"context"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The fakes below gain the per-call budget forms of their interfaces.
// fakePeerClient turns a budget into a context deadline, which is how
// the real transport's budget looks from the far side (a call that ends
// with context.DeadlineExceeded), so its scripted functions keep seeing
// a bounded context exactly as before.

func zzWP9Within(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (f fakePeerClient) ConsumeWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.Consume(ctx, addr, req)
}

func (f fakePeerClient) AckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.Ack(ctx, addr, req)
}

func (f fakePeerClient) ExtendAckWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.ExtendAck(ctx, addr, req)
}

func (f fakePeerClient) NackWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.Nack(ctx, addr, req)
}

func (f fakePeerClient) NotifyTokenWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.TokenNotifyRequest) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.NotifyToken(ctx, addr, req)
}

func (f fakePeerClient) RegisterTokensWithin(ctx context.Context, addr string, timeout time.Duration, delta nodewire.TokenDelta) (nodewire.Response, error) {
	ctx, cancel := zzWP9Within(ctx, timeout)
	defer cancel()
	return f.RegisterTokens(ctx, addr, delta)
}

func (r *laneRecorder) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return r.RequestOnLane(ctx, addr, lane, frameType, payload)
}

func (s scriptedTransport) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return s.RequestOnLane(ctx, addr, lane, frameType, payload)
}

func (f *fallbackTransport) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLane(ctx, addr, lane, frameType, payload)
}
