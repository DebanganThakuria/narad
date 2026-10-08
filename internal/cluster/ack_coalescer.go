package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Ack coalescing.
//
// Every forwarded ack used to be an RPC of its own: a request frame, a
// reply frame, a server goroutine and a messaging slot, for a few bytes
// of in-memory bookkeeping on the owner. At the throughput ceiling most
// cluster RPCs are acks, one per consumed message.
//
// A forwarded ack still goes out on its own, at once, whenever fewer
// than ackInFlightLimit acks to its owner are in flight: the same OpAck,
// sent as soon as it arrives. Only when every one of those slots is
// busy does a further ack wait, and the acks that pile up behind one
// owner leave together as one OpAckBatch the moment a slot frees. That
// is Nagle's algorithm with a wide window: batches form only when many
// acks to one owner overlap, and grow with how much they overlap.
//
// The window has to be wide because a queued ack waits for some other
// ack's round trip to end, a wait that shrinks as the window grows. A
// window of 4 made every ack past the 4th in flight wait: with a 1 ms
// round trip, forwarded acks at 5 to 64 in flight were 38% to 93% slower
// than one RPC each. At twice the owner's handler bound (128 on a small node)
// the queue forms only far past where most gateways run, and there the
// batch saves more than the wait costs.
//
// Acks are idempotent by nonce, so sharing an RPC changes nothing about
// what they do. Each record keeps its own outcome: the owner answers
// every record of a batch separately, and each HTTP ack gets the status
// and body its own single RPC would have got.

// ackCoalesceMax caps one coalesced batch. A burst larger than this
// forms several batches, which leave one per freed slot.
const ackCoalesceMax = 64

// ackInFlightLimit is how many ack RPCs to one owner may be in flight
// before further acks queue for a shared batch: twice the
// messaging-handler bound every node serves with (see
// defaultMessagingConcurrency), since an ack holds an owner's handler
// slot only while it runs there, not while it crosses the network. It
// is read once per owner, when the owner's state is created, because
// runtime.GOMAXPROCS takes a scheduler lock.
func ackInFlightLimit() int { return 2 * defaultMessagingConcurrency() }

// ackCoalescer is the router's per-owner ack state. The zero value is
// ready to use.
type ackCoalescer struct {
	owners sync.Map // addr -> *ackOwner
	legacy ackBatchLegacy
}

func (c *ackCoalescer) owner(addr string) *ackOwner {
	if v, ok := c.owners.Load(addr); ok {
		return v.(*ackOwner)
	}
	v, _ := c.owners.LoadOrStore(addr, &ackOwner{limit: ackInFlightLimit()})
	return v.(*ackOwner)
}

// ackOwner is one owner's slots and queue. While queue is not empty
// every slot is busy: a freed slot passes straight to the queue's head.
type ackOwner struct {
	mu       sync.Mutex
	limit    int // slots; see ackInFlightLimit
	inflight int
	queue    []*ackBatch
}

// ackBatch is acks waiting to leave for one owner together.
type ackBatch struct {
	items []nodewire.AckBatchItem
	// gone marks callers that stopped waiting before the batch left;
	// their records are not sent. Guarded by the owner's mu.
	gone []bool
	// sent is set when the batch takes a slot and leaves. From then on
	// its records are on their way to the owner, which may apply them,
	// so a caller whose queue wait runs out keeps waiting for the
	// batch's answer instead of reporting a failure. Guarded by the
	// owner's mu.
	sent bool
	done chan struct{}
	// Set before done is closed.
	outs   []ackOut
	legacy bool // the owner refused the op: each caller sends its own
}

// ackOut is one caller's share of a batch's round trip.
type ackOut struct {
	res nodewire.Response
	err error
}

// forwardAck sends one ack-shaped record to its owner at addr, at once
// when a slot is free and otherwise as part of the next batch, and
// returns what a single RPC for it would have: the owner's reply, or
// the transport error. See the comment at the top of this file.
func (rt *Router) forwardAck(ctx context.Context, addr string, item nodewire.AckBatchItem) (nodewire.Response, error) {
	if !rt.peerBatches() || rt.acks.legacy.is(addr) {
		return rt.sendSingleAck(ctx, addr, ackForwardTimeout, item)
	}
	o := rt.acks.owner(addr)
	o.mu.Lock()
	if o.inflight < o.limit {
		o.inflight++
		o.mu.Unlock()
		res, err := rt.sendSingleAck(ctx, addr, ackForwardTimeout, item)
		rt.releaseAckSlot(addr, o)
		return res, err
	}
	b, idx := o.join(item)
	o.mu.Unlock()
	return rt.awaitAck(ctx, addr, o, b, idx, item)
}

// peerBatches reports whether the peer client can send an OpAckBatch.
// The production client is checked by its concrete type, which costs a
// pointer compare rather than an interface lookup on every forwarded ack.
func (rt *Router) peerBatches() bool {
	if _, ok := rt.peer.(*PeerClient); ok {
		return true
	}
	_, ok := rt.peer.(ackBatcher)
	return ok
}

// join adds item to the batch at the tail of the queue, opening a new
// one when there is none or it is full. Must hold o.mu.
func (o *ackOwner) join(item nodewire.AckBatchItem) (*ackBatch, int) {
	var b *ackBatch
	if n := len(o.queue); n > 0 && len(o.queue[n-1].items) < ackCoalesceMax {
		b = o.queue[n-1]
	} else {
		b = &ackBatch{done: make(chan struct{})}
		o.queue = append(o.queue, b)
	}
	b.items = append(b.items, item)
	b.gone = append(b.gone, false)
	return b, len(b.items) - 1
}

// releaseAckSlot frees a slot after an RPC to the owner returns, or
// passes it to the batch at the head of the queue, which then leaves on
// a goroutine of its own: the caller has its reply and must not wait
// for somebody else's.
func (rt *Router) releaseAckSlot(addr string, o *ackOwner) {
	o.mu.Lock()
	if len(o.queue) == 0 {
		o.inflight--
		o.mu.Unlock()
		return
	}
	b := o.queue[0]
	o.queue[0] = nil
	o.queue = o.queue[1:]
	b.sent = true
	live := make([]int, 0, len(b.items))
	for i, gone := range b.gone {
		if !gone {
			live = append(live, i)
		}
	}
	o.mu.Unlock()
	go rt.sendAckBatch(addr, o, b, live)
}

// sendAckBatch sends a batch's live records, hands every caller its
// outcome, and frees the slot the batch held. It runs under no caller's
// context: the batch serves every caller in it, and any one of them
// leaving must not cancel it for the rest. The round trip gets the full
// ackForwardTimeout however long the batch queued: a batch sent with
// only the rest of its callers' budget would time out on the wire, after
// the owner may have applied it, which is an unknown outcome by
// construction.
func (rt *Router) sendAckBatch(addr string, o *ackOwner, b *ackBatch, live []int) {
	b.outs = make([]ackOut, len(b.items))
	switch len(live) {
	case 0:
		// Everyone left before the batch did.
	case 1:
		// One record is an OpAck, which every owner speaks.
		i := live[0]
		b.outs[i].res, b.outs[i].err = rt.sendSingleAck(context.Background(), addr, ackForwardTimeout, b.items[i])
	default:
		rt.sendLiveBatch(addr, b, live, ackForwardTimeout)
	}
	close(b.done)
	rt.releaseAckSlot(addr, o)
}

// sendLiveBatch is sendAckBatch's OpAckBatch round trip.
func (rt *Router) sendLiveBatch(addr string, b *ackBatch, live []int, timeout time.Duration) {
	req := nodewire.AckBatchRequest{Items: make([]nodewire.AckBatchItem, len(live))}
	for k, i := range live {
		req.Items[k] = b.items[i]
	}
	res, err := rt.peer.(ackBatcher).AckBatchWithin(context.Background(), addr, timeout, req)
	if err == nil && isUnsupportedOp(res) {
		rt.acks.legacy.note(addr)
		b.legacy = true
		return
	}
	var results []nodewire.AckResult
	if err == nil && res.Status == http.StatusOK {
		results, err = nodewire.DecodeAckBatchReply(res.Body, nil)
		if err == nil && len(results) != len(live) {
			err = fmt.Errorf("ack batch reply carries %d results for %d records", len(results), len(live))
		}
		if err != nil {
			err = fmt.Errorf("invalid ack batch reply: %w", err)
		}
	}
	for k, i := range live {
		switch {
		case err != nil:
			b.outs[i].err = err
		case results == nil:
			// The batch as a whole was refused (an owner that gave up on
			// it while it waited for a slot, say): every record shares
			// the reply.
			b.outs[i].res = res
		default:
			b.outs[i].res = ackResultResponse(results[k])
		}
	}
}

// awaitAck waits for the batch holding a queued ack and returns the
// ack's share of it. The wait for a slot is bounded by
// ackForwardTimeout; an ack still queued then gives up without having
// been sent. Once its batch has left, the ack waits for the batch's
// answer, which the batch's own round-trip budget bounds: the record is
// on its way to the owner, and giving up then would report a failure for
// an ack the owner may apply. The longest wait is therefore twice
// ackForwardTimeout.
func (rt *Router) awaitAck(ctx context.Context, addr string, o *ackOwner, b *ackBatch, idx int, item nodewire.AckBatchItem) (nodewire.Response, error) {
	timer := time.NewTimer(ackForwardTimeout)
	defer timer.Stop()
	select {
	case <-b.done:
	case <-ctx.Done():
		// The client left. Its record stays out of a batch that has not
		// left yet; one that has may still be applied.
		o.leave(b, idx)
		return nodewire.Response{}, ctx.Err()
	case <-timer.C:
		if o.leave(b, idx) {
			return nodewire.Response{}, ackQueueTimeout(addr)
		}
		select {
		case <-b.done:
		case <-ctx.Done():
			return nodewire.Response{}, ctx.Err()
		}
	}
	if b.legacy {
		// The owner predates OpAckBatch and refused the batch, so nothing
		// in it was applied. Send this record on its own with a round
		// trip of its own; the owner is remembered, so later acks to it
		// skip the queue altogether.
		return rt.sendSingleAck(ctx, addr, ackForwardTimeout, item)
	}
	out := b.outs[idx]
	return out.res, out.err
}

// leave marks a queued caller gone, so a batch that has not left yet
// does not send its record: the requester answers its client with an
// error, and a retry is applied on its own (as the owner's own slot wait
// does, see handleAckFamily). It reports false, and marks nothing, when
// the batch has already left: the record is then on its way to the
// owner and cannot be pulled back.
func (o *ackOwner) leave(b *ackBatch, idx int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if b.sent {
		return false
	}
	b.gone[idx] = true
	return true
}

// ackQueueTimeout is a queued ack that found no slot to leave on within
// ackForwardTimeout: a deadline failure, like a single forwarded ack's,
// and one that never left this node, so the owner applied nothing.
func ackQueueTimeout(addr string) error {
	return fmt.Errorf("ack to %s: %w after %s: %w: %w", addr, errAckQueued, ackForwardTimeout, clusterrpc.ErrNotSent, context.DeadlineExceeded)
}

// ackResultResponse is the reply a single ack-shaped RPC would have
// carried for one record's result: a bare status, or the owner's error
// body, byte for byte (see RPCServer.brokerError).
func ackResultResponse(r nodewire.AckResult) nodewire.Response {
	if r.Status < http.StatusMultipleChoices {
		return nodewire.Response{Status: r.Status}
	}
	return errorResponse(r.Status, r.Error)
}
