package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
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
	ch chan struct{}
	// deadline is when this consumer's wait budget ends, so a token
	// re-registered on behalf of the consumers still parked can carry the
	// longest remaining budget among them.
	deadline time.Time
	mu       sync.Mutex
	from     string
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
	// owners records, per remote owner address, until when this node
	// believes that owner holds its live token for the topic: stamped
	// when a registration is sent, cleared when the send fails. The
	// keeper (Run) registers wherever an entry is missing or past.
	owners map[string]time.Time
}

// keepAliveInterval is how often the keeper checks that every remote
// owner of a topic with parked consumers holds a live token from here.
const keepAliveInterval = 500 * time.Millisecond

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

	// legacyMu guards legacyPeers, the set of peers that answered a
	// registration with "unsupported rpc operation": nodes too old to
	// speak this protocol. See noteRegisterResult.
	legacyMu    sync.Mutex
	legacyPeers map[string]bool
}

func newTokenRequester(rt *Router, selfAddr string) *tokenRequester {
	return &tokenRequester{
		router:      rt,
		selfAddr:    selfAddr,
		topics:      make(map[string]*topicDemand),
		legacyPeers: make(map[string]bool),
	}
}

// noteRegisterResult records whether a peer understood a registration,
// and logs each change of state exactly once per peer.
//
// A node that predates this protocol answers OpTokenRegister with 400
// "unsupported rpc operation", so during a rolling upgrade an upgraded
// node leaves tokens that some owners silently discard. The consequence
// is bounded and self-healing: the opening probe is an ordinary consume
// that old nodes serve, so a record on an old owner is still found, just
// on the consumer's NEXT poll rather than by notification. The cost is
// up to one wait budget of extra latency per record on a not-yet-
// upgraded owner, for the length of the rollout.
//
// What is not acceptable is that being invisible. Consume latency rising
// during a rollout with nothing in the logs to explain it is the kind of
// thing that gets diagnosed as a mystery. So the transition is logged in
// BOTH directions: once when a peer is first seen refusing, and again
// when that same peer starts accepting, which is how an operator watches
// the rollout drain to zero.
//
// Registrations are still sent to peers on the legacy list. Skipping
// them would save an RPC and cost correctness: nothing else tells this
// node the peer has been upgraded, so a peer written off once would
// never be offered a token again until this process restarted.
func (q *tokenRequester) noteRegisterResult(addr string, res nodewire.Response, err error) {
	if err != nil {
		// A transport failure says nothing about what the peer speaks.
		return
	}
	legacy := res.Status == http.StatusBadRequest &&
		bytes.Contains(res.Body, []byte("unsupported rpc operation"))

	q.legacyMu.Lock()
	was := q.legacyPeers[addr]
	if legacy == was {
		q.legacyMu.Unlock()
		return
	}
	if legacy {
		q.legacyPeers[addr] = true
	} else {
		delete(q.legacyPeers, addr)
	}
	remaining := len(q.legacyPeers)
	q.legacyMu.Unlock()

	if legacy {
		slog.Default().Warn("peer does not speak the consume token protocol; "+
			"records on its partitions reach consumers here on the next poll "+
			"rather than by notification, until it is upgraded",
			"peer", addr, "legacy_peers", remaining)
		return
	}
	slog.Default().Info("peer now speaks the consume token protocol",
		"peer", addr, "legacy_peers", remaining)
}

// legacyPeerCount is how many owners are currently known not to speak
// the token protocol. Zero on a fully upgraded cluster.
func (q *tokenRequester) legacyPeerCount() int {
	q.legacyMu.Lock()
	defer q.legacyMu.Unlock()
	return len(q.legacyPeers)
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
func (q *tokenRequester) park(topicName string, deadline time.Time) (*localWaiter, func()) {
	d := q.demandFor(topicName)
	w := &localWaiter{ch: make(chan struct{}, 1), deadline: deadline}
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

// othersParked reports whether any consumer other than self is still
// parked on the topic, and the longest wait budget among them. The
// owners hold ONE token per (this node, topic), so the consumer that is
// leaving must know whether that token still has takers: dropping it
// while others are parked stranded every one of them until their wait
// ran out, and spending it (a claim) without registering again did the
// same.
func (q *tokenRequester) othersParked(topicName string, self *localWaiter) (remaining time.Duration, ok bool) {
	d := q.demandFor(topicName)
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, w := range d.waiters {
		if w == self {
			continue
		}
		ok = true
		if left := w.deadline.Sub(now); left > remaining {
			remaining = left
		}
	}
	return remaining, ok
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
	q.send(ctx, topicName, owners, remaining)
}

// send leaves a token with each of addrs and records the attempt in the
// topic's owners table, optimistically: a send that fails clears its
// entry when the failure is known, and the keeper tries that owner
// again on its next pass.
func (q *tokenRequester) send(ctx context.Context, topicName string, addrs []string, remaining time.Duration) {
	until := time.Now().Add(remaining)
	d := q.demandFor(topicName)
	d.mu.Lock()
	if d.owners == nil {
		d.owners = make(map[string]time.Time)
	}
	for _, addr := range addrs {
		d.owners[addr] = until
	}
	d.mu.Unlock()
	delta := nodewire.TokenDelta{
		From: q.selfAddr,
		Add:  []nodewire.TokenRegistration{{Topic: topicName, TTLNanos: int64(remaining)}},
	}
	q.broadcast(ctx, addrs, delta, func(addr string, err error) {
		if err == nil {
			return
		}
		d.mu.Lock()
		if d.owners[addr].Equal(until) {
			delete(d.owners, addr)
		}
		d.mu.Unlock()
	})
}

// Run is the keeper: for as long as consumers are parked on a topic, it
// registers with any remote owner of that topic which does not hold a
// live token from this node. That is how an owner that was unreachable
// when the consumers parked (its registration failed) or that became an
// owner afterwards (a restart, a partition move) learns of the demand;
// nothing else would tell it, and its records sat until the consumers'
// waits ran out. One cheap pass per keepAliveInterval per node, and no
// network at all unless an owner is missing a token.
func (q *tokenRequester) Run(ctx context.Context) {
	if !q.enabled() {
		return
	}
	t := time.NewTicker(keepAliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			q.keepAlive(ctx)
		}
	}
}

func (q *tokenRequester) keepAlive(ctx context.Context) {
	q.mu.RLock()
	topics := make([]string, 0, len(q.topics))
	for name := range q.topics {
		topics = append(topics, name)
	}
	q.mu.RUnlock()
	for _, topicName := range topics {
		d := q.demandFor(topicName)
		now := time.Now()
		d.mu.Lock()
		var remaining time.Duration
		for _, w := range d.waiters {
			if left := w.deadline.Sub(now); left > remaining {
				remaining = left
			}
		}
		d.mu.Unlock()
		if remaining < tokenTTLFloor {
			continue
		}
		var missing []string
		for _, addr := range q.router.remoteConsumeCandidates(topicName) {
			if q.isLegacyPeer(addr) {
				// It would refuse the registration; the next consumer to
				// park re-tests it (see noteRegisterResult).
				continue
			}
			d.mu.Lock()
			live := d.owners[addr].After(now)
			d.mu.Unlock()
			if !live {
				missing = append(missing, addr)
			}
		}
		if len(missing) > 0 {
			q.send(ctx, topicName, missing, remaining)
		}
	}
}

func (q *tokenRequester) isLegacyPeer(addr string) bool {
	q.legacyMu.Lock()
	defer q.legacyMu.Unlock()
	return q.legacyPeers[addr]
}

// registerAt leaves a fresh token with one owner: the one that just
// spent ours on a consumer here, when other consumers are still parked
// for the topic. The tokens at the other owners are untouched; they were
// never spent.
func (q *tokenRequester) registerAt(ctx context.Context, topicName, addr string, remaining time.Duration) {
	if !q.enabled() || remaining < tokenTTLFloor {
		return
	}
	q.send(ctx, topicName, []string{addr}, remaining)
}

// drop retires this node's token at every owner except the one that
// served us. Only called once nobody else is parked here for the topic:
// the token is shared by every consumer on this node, so retiring it
// early would strand the rest. Best effort and fire-and-forget: a lost
// drop costs one notification the owner's next record wastes on us,
// never correctness.
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
	d := q.demandFor(topicName)
	d.mu.Lock()
	for _, addr := range targets {
		delete(d.owners, addr)
	}
	d.mu.Unlock()
	q.broadcast(ctx, targets, nodewire.TokenDelta{From: q.selfAddr, Drop: []string{topicName}}, nil)
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
//
// done, when set, is told how each send ended (nil for any reply, the
// transport error otherwise), so a registration that never reached its
// owner can be tried again.
func (q *tokenRequester) broadcast(ctx context.Context, addrs []string, delta nodewire.TokenDelta, done func(addr string, err error)) {
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tokenSendTimeout)
	var pending sync.WaitGroup
	for _, addr := range addrs {
		pending.Go(func() {
			res, err := q.router.peer.RegisterTokens(sendCtx, addr, delta)
			// The send stays fire-and-forget; the REPLY is still worth
			// reading, because it is the only place a peer tells us it
			// does not speak this protocol.
			q.noteRegisterResult(addr, res, err)
			if done != nil {
				done(addr, err)
			}
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
