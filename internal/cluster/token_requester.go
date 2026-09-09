package cluster

import (
	"context"
	"sync"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The requester half of the token protocol.
//
// When a consumer parks here for a topic whose partitions live on other
// nodes, this node leaves a token with every one of those owners. A
// token reserves nothing: it says only "I am here, tell me if records
// show up". An owner spends one by calling back, this node wakes one
// parked consumer, and that consumer claims with an ordinary consume.
//
// Registration and retirement both go out to every owner CONCURRENTLY.
// Doing them one owner at a time would put a round trip per owner in
// front of the very consumer they exist to serve.

// tokenTTLFloor is the shortest remaining budget worth registering. A
// token that expires before a notification and a claim could complete
// is guaranteed waste, so it is never sent.
const tokenTTLFloor = 50 * time.Millisecond

// localWaiter is one consumer parked here, waiting to be told where to
// claim.
//
// The signal and the payload are separate on purpose: ch is a plain
// struct{} channel so it can be selected on inside the broker's local
// wait, which lets one goroutine cover both halves of the race instead
// of two. The owner address rides alongside it in from, published
// before the signal and read after, so the claim can be aimed at
// exactly the node that has the record.
type localWaiter struct {
	ch   chan struct{}
	mu   sync.Mutex
	from string
}

// take returns the address of the owner that woke this waiter.
func (w *localWaiter) take() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.from
}

// offer publishes the owner address and signals, reporting whether the
// waiter took it. The signal is non-blocking: a waiter already woken by
// another owner simply is not available.
func (w *localWaiter) offer(from string) bool {
	w.mu.Lock()
	w.from = from
	w.mu.Unlock()
	select {
	case w.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// topicDemand is the set of consumers parked on one topic.
type topicDemand struct {
	mu      sync.Mutex
	waiters []*localWaiter
}

// tokenRequester tracks what this node is waiting for and keeps the
// matching tokens alive at the owners.
type tokenRequester struct {
	router *Router
	// selfAddr is what this node advertises so owners can call back. An
	// empty value disables the protocol: without a return address a token
	// can never be spent, so registering one would strand it.
	selfAddr string

	mu     sync.RWMutex
	topics map[string]*topicDemand
}

func newTokenRequester(rt *Router, selfAddr string) *tokenRequester {
	return &tokenRequester{
		router:   rt,
		selfAddr: selfAddr,
		topics:   make(map[string]*topicDemand),
	}
}

// enabled reports whether this node can take part: it needs a return
// address for owners to notify.
func (q *tokenRequester) enabled() bool { return q != nil && q.selfAddr != "" }

func (q *tokenRequester) demandFor(topicName string) *topicDemand {
	q.mu.RLock()
	d, ok := q.topics[topicName]
	q.mu.RUnlock()
	if ok {
		return d
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if d, ok := q.topics[topicName]; ok {
		return d
	}
	d = &topicDemand{}
	q.topics[topicName] = d
	return d
}

// park adds a waiter and returns it with a release func that removes
// it. The caller selects on the waiter's channel.
func (q *tokenRequester) park(topicName string) (*localWaiter, func()) {
	d := q.demandFor(topicName)
	w := &localWaiter{ch: make(chan struct{}, 1)}
	d.mu.Lock()
	d.waiters = append(d.waiters, w)
	d.mu.Unlock()
	return w, func() {
		d.mu.Lock()
		for i, other := range d.waiters {
			if other == w {
				d.waiters = append(d.waiters[:i], d.waiters[i+1:]...)
				break
			}
		}
		d.mu.Unlock()
	}
}

// repark returns a waiter to the queue after its claim lost the race.
// Waking removes a waiter permanently, so without this the consumer
// would re-register its token with nothing left listening for the next
// offer, and would sit out the rest of its budget.
func (q *tokenRequester) repark(topicName string, w *localWaiter) {
	d := q.demandFor(topicName)
	d.mu.Lock()
	d.waiters = append(d.waiters, w)
	d.mu.Unlock()
}

// WakeOneWaiter hands the owner's address to one parked consumer and
// reports whether anyone took it. False is the "pass" verdict: nobody
// here wants the record any more, so the owner should offer it to the
// next peer at once instead of waiting out its deadline.
//
// The waiter is removed before the send, so its buffered channel gets
// exactly one address and the send can never block the RPC handler.
func (q *tokenRequester) WakeOneWaiter(topicName, from string) bool {
	if q == nil {
		return false
	}
	d := q.demandFor(topicName)
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.waiters) > 0 {
		w := d.waiters[0]
		d.waiters = d.waiters[1:]
		if w.offer(from) {
			return true
		}
		// Already woken by another owner; try the next one.
	}
	return false
}

// register leaves a token with every remote owner of the topic, all at
// once. Failures are ignored: a token that never lands costs this
// consumer its notification, and the wait budget is the backstop.
func (q *tokenRequester) register(ctx context.Context, topicName string, remaining time.Duration) {
	if !q.enabled() || remaining < tokenTTLFloor {
		return
	}
	owners := q.router.remoteConsumeCandidates(topicName)
	if len(owners) == 0 {
		return
	}
	delta := nodewire.TokenDelta{
		From: q.selfAddr,
		Add:  []nodewire.TokenRegistration{{Topic: topicName, TTLNanos: int64(remaining), MinRecords: 1}},
	}
	q.broadcast(ctx, owners, delta)
}

// drop retires this node's token at every owner except the one that
// served us. Best effort and fire-and-forget: a lost drop costs one
// notification the owner's next record wastes on us, never correctness.
func (q *tokenRequester) drop(ctx context.Context, topicName, servedBy string) {
	if !q.enabled() {
		return
	}
	owners := q.router.remoteConsumeCandidates(topicName)
	targets := owners[:0:0]
	for _, addr := range owners {
		if addr != servedBy {
			targets = append(targets, addr)
		}
	}
	if len(targets) == 0 {
		return
	}
	q.broadcast(ctx, targets, nodewire.TokenDelta{From: q.selfAddr, Drop: []string{topicName}})
}

// broadcast sends one delta to every address concurrently and does NOT
// wait for them. Blocking here would put a round trip to every owner in
// front of the consumer that triggered it, on every single consume,
// which is the cost this protocol exists to avoid.
//
// Not waiting is safe because a token landing late is self-correcting: a
// record produced before the token arrives is still sitting there when
// it does, and registering marks the topic dirty, so the owner's pump
// notifies on the spot. The window costs a moment of latency, never a
// missed record.
//
// The context is detached from the request that triggered it for the
// same reason: a drop must still land after the consumer it belongs to
// has been served, and a register must not be cancelled by the consumer
// parking.
func (q *tokenRequester) broadcast(ctx context.Context, addrs []string, delta nodewire.TokenDelta) {
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tokenSendTimeout)
	var pending sync.WaitGroup
	for _, addr := range addrs {
		pending.Go(func() {
			_, _ = q.router.peer.RegisterTokens(sendCtx, addr, delta)
		})
	}
	// Release the timeout once the last send finishes, without holding
	// the caller: cancel() must outlive the sends, not the request.
	go func() {
		pending.Wait()
		cancel()
	}()
}

// tokenSendTimeout bounds one register or drop. Tokens are advisory, so
// a slow owner is dropped from this round rather than holding up the
// consumer that triggered it.
const tokenSendTimeout = 2 * time.Second
