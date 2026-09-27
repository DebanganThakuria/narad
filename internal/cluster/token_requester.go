package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"slices"
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
// Registration goes out to every owner CONCURRENTLY. Doing it one owner
// at a time would put a round trip per owner in front of the very
// consumer it exists to serve. It also goes out only where it is
// needed: consumers parking in quick succession share the token the
// first of them left (see registerShareWindow), and a token nobody here
// wants any more is left to lapse rather than retired with a frame of
// its own (see register).

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
	// from is the owner of an offer this waiter accepted and has not yet
	// acted on: set by offer, cleared by take.
	from string
}

// take returns the address of the owner that woke this waiter, or ""
// if none did since the last take.
func (w *localWaiter) take() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	from := w.from
	w.from = ""
	return from
}

// offer publishes the owner address and signals, reporting whether the
// waiter took it. The signal is non-blocking: a waiter already woken by
// another owner simply is not available. The address is set under the
// same lock hold as the signal, so take sees it once the signal is
// received, and a refused offer leaves no address behind.
func (w *localWaiter) offer(from string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case w.ch <- struct{}{}:
		w.from = from
		return true
	default:
		return false
	}
}

// topicDemand is the set of consumers parked on one topic.
type topicDemand struct {
	mu      sync.Mutex
	waiters []*localWaiter
	// owners records, per remote owner address, the token this node
	// believes that owner holds for the topic: stamped when a
	// registration is sent, cleared when the send fails or the owner
	// spends the token on a notification. The keeper (Run) registers
	// wherever an entry is missing or due for a refresh.
	owners map[string]ownerToken
}

// ownerToken is this node's record of one registration at one owner.
type ownerToken struct {
	// sentAt is when the registration went out.
	sentAt time.Time
	// refreshAt is when the keeper sends it again: the registered budget
	// or registrationRefresh after sentAt, whichever is sooner.
	refreshAt time.Time
	// expiresAt is when the owner lets the token lapse: sentAt plus the
	// TTL the registration carried.
	expiresAt time.Time
}

// registerShareWindow is how long a registration is shared by consumers
// parking after it. A consumer parking here for a topic whose owners
// already hold a token from this node, sent within the window and
// lasting past its deadline, sends nothing: the owner holds one token
// per (node, topic) and a new registration would only replace it. With
// hundreds of consumers re-parking on a sparse topic that was one
// registration per consumer per owner, each replacing the last.
//
// The window is short on purpose. The owner's reply only says the frame
// arrived; the token may have been discarded (its assignment view
// lagged ours) or lost to a restart. Re-registering on park repairs
// that at once, so sharing is trusted only briefly, and a consumer
// parking after the window re-sends. A token spent on a notification is
// forgotten outright (see WakeOneWaiter).
//
// Registrations carry the window on top of the budget, so a consumer
// parking inside the window with the same wait as the one that
// registered is still covered.
const registerShareWindow = 250 * time.Millisecond

// keepAliveInterval is how often the keeper checks that every remote
// owner of a topic with parked consumers holds a live token from here.
const keepAliveInterval = 500 * time.Millisecond

// registrationRefresh bounds how long a registration is trusted before
// the keeper sends it again. The owner's reply says only that the frame
// arrived: it may have discarded the token (its assignment view lagged
// ours), or it may restart and lose it. Re-registering replaces the
// token, so repeating it every few seconds while consumers are parked
// repairs both at the cost of one small frame per owner per interval.
const registrationRefresh = 5 * time.Second

// longestRemainingLocked returns the longest wait budget among the
// parked consumers other than exclude (nil to count them all), and
// whether there was any. Must hold mu.
func (d *topicDemand) longestRemainingLocked(now time.Time, exclude *localWaiter) (remaining time.Duration, any bool) {
	for _, w := range d.waiters {
		if w == exclude {
			continue
		}
		any = true
		if left := w.deadline.Sub(now); left > remaining {
			remaining = left
		}
	}
	return remaining, any
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

	// legacyMu guards legacyPeers, the peers that answered a
	// registration with "unsupported rpc operation" (nodes too old to
	// speak this protocol), each with when it last did. See
	// noteRegisterResult.
	legacyMu    sync.Mutex
	legacyPeers map[string]time.Time
}

func newTokenRequester(rt *Router, selfAddr string) *tokenRequester {
	return &tokenRequester{
		router:      rt,
		selfAddr:    selfAddr,
		topics:      make(map[string]*topicDemand),
		legacyPeers: make(map[string]time.Time),
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
// Registrations from a parking consumer are still sent to peers on the
// legacy list. Skipping them would save an RPC and cost correctness:
// nothing else tells this node the peer has been upgraded, so a peer
// written off once would never be offered a token again until this
// process restarted. Only the keeper's periodic refresh skips them, so a
// not-yet-upgraded owner is not asked twice a second for the length of
// the roll; the next consumer to park re-tests it.
func (q *tokenRequester) noteRegisterResult(addr string, res nodewire.Response, err error) {
	if err != nil {
		// A transport failure says nothing about what the peer speaks.
		return
	}
	legacy := res.Status == http.StatusBadRequest &&
		bytes.Contains(res.Body, []byte("unsupported rpc operation"))

	q.legacyMu.Lock()
	_, was := q.legacyPeers[addr]
	if legacy {
		// Refreshed on every refusal: a gateway polls a topic for
		// legacyRetest after its owner last refused (see pollingOwner).
		q.legacyPeers[addr] = time.Now()
	}
	if legacy == was {
		q.legacyMu.Unlock()
		return
	}
	if !legacy {
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
// it. The caller selects on the waiter's channel. The release func is
// for the caller's goroutine only.
func (q *tokenRequester) park(topicName string, deadline time.Time) (*localWaiter, func()) {
	d := q.demandFor(topicName)
	w := &localWaiter{ch: make(chan struct{}, 1), deadline: deadline}
	d.mu.Lock()
	d.waiters = append(d.waiters, w)
	d.mu.Unlock()
	released := false
	return w, func() {
		// Idempotent: a consumer leaves the queue as soon as it stops
		// waiting and again, as a no-op, when it returns.
		if released {
			return
		}
		released = true
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
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.longestRemainingLocked(time.Now(), self)
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
//
// A notification spends the owner's token whatever the verdict, so the
// owner is forgotten here: the keeper then puts a token back within
// keepAliveInterval if anyone is still parked, and the next consumer to
// park registers there. Without it a consumer woken as it was leaving
// (served locally at that instant, or at the end of its budget) took
// the token with it, and the keeper, still trusting the old stamp, left
// the consumers parked behind it with no token at that owner for up to
// registrationRefresh.
func (q *tokenRequester) WakeOneWaiter(topicName, from string) bool {
	if q == nil {
		return false
	}
	d := q.demandFor(topicName)
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.owners, from)
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

// register leaves a token with every remote owner of the topic that
// does not already hold one covering this consumer, all at once.
// Failures are ignored: a token that never lands costs this consumer
// its notification, and the wait budget is the backstop.
//
// Nothing retires the tokens when the last consumer leaves. A token
// nobody here wants any more costs at most one notification, answered
// "pass", before it lapses at its TTL, where retiring it cost a frame
// to every owner each time the parked count touched zero, and the next
// consumer to park had to register all over again.
func (q *tokenRequester) register(ctx context.Context, topicName string, remaining time.Duration) {
	if !q.enabled() || remaining < tokenTTLFloor {
		return
	}
	// The plain owner list, not the rotated one the probe path uses:
	// registering must not advance the probe cursor.
	owners := q.router.remoteOwnerAddrsForTopic(topicName)
	if len(owners) == 0 {
		return
	}
	d := q.demandFor(topicName)
	now := time.Now()
	d.mu.Lock()
	// The token is shared by every consumer parked here, so it carries
	// the longest budget among them: a short poll registering after a
	// long one must not shorten the token the long one relies on.
	if longest, _ := d.longestRemainingLocked(now, nil); longest > remaining {
		remaining = longest
	}
	need := now.Add(remaining)
	targets := owners[:0]
	for _, addr := range owners {
		if tok, ok := d.owners[addr]; ok && now.Sub(tok.sentAt) < registerShareWindow && !tok.expiresAt.Before(need) {
			continue
		}
		targets = append(targets, addr)
	}
	stamp := d.stampLocked(targets, now, remaining)
	d.mu.Unlock()
	q.dispatch(ctx, topicName, d, targets, remaining, stamp)
}

// send leaves a token with each of addrs.
func (q *tokenRequester) send(ctx context.Context, topicName string, addrs []string, remaining time.Duration) {
	d := q.demandFor(topicName)
	d.mu.Lock()
	stamp := d.stampLocked(addrs, time.Now(), remaining)
	d.mu.Unlock()
	q.dispatch(ctx, topicName, d, addrs, remaining, stamp)
}

// stampLocked records a registration of remaining to each of addrs,
// optimistically: dispatch clears an entry whose send is known to have
// failed, and the keeper tries that owner again on its next pass. Must
// hold mu.
func (d *topicDemand) stampLocked(addrs []string, now time.Time, remaining time.Duration) ownerToken {
	stamp := ownerToken{
		sentAt:    now,
		refreshAt: now.Add(min(remaining, registrationRefresh)),
		expiresAt: now.Add(remaining + registerShareWindow),
	}
	if len(addrs) == 0 {
		return stamp
	}
	if d.owners == nil {
		d.owners = make(map[string]ownerToken)
	}
	for _, addr := range addrs {
		d.owners[addr] = stamp
	}
	return stamp
}

// dispatch sends the registration stamped for addrs. Its TTL carries
// registerShareWindow on top of the budget, matching the stamp's expiry.
func (q *tokenRequester) dispatch(ctx context.Context, topicName string, d *topicDemand, addrs []string, remaining time.Duration, stamp ownerToken) {
	if len(addrs) == 0 {
		return
	}
	delta := nodewire.TokenDelta{
		From: q.selfAddr,
		Add:  []nodewire.TokenRegistration{{Topic: topicName, TTLNanos: int64(remaining + registerShareWindow)}},
	}
	q.broadcast(ctx, addrs, delta, func(addr string, err error) {
		if err == nil {
			return
		}
		d.mu.Lock()
		if tok, ok := d.owners[addr]; ok && tok.sentAt.Equal(stamp.sentAt) {
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
		// The plain owner list, not the rotated one the probe path uses:
		// a keeper pass must not advance the probe cursor.
		owners := q.router.remoteOwnerAddrsForTopic(topicName)
		if len(owners) == 0 {
			continue
		}
		d := q.demandFor(topicName)
		now := time.Now()
		var missing []string
		d.mu.Lock()
		remaining, _ := d.longestRemainingLocked(now, nil)
		if remaining >= tokenTTLFloor {
			for _, addr := range owners {
				if !d.owners[addr].refreshAt.After(now) {
					missing = append(missing, addr)
				}
			}
		}
		d.mu.Unlock()
		if len(missing) == 0 {
			continue
		}
		missing = slices.DeleteFunc(missing, q.isLegacyPeer)
		if len(missing) > 0 {
			q.send(ctx, topicName, missing, remaining)
		}
	}
}

func (q *tokenRequester) isLegacyPeer(addr string) bool {
	q.legacyMu.Lock()
	defer q.legacyMu.Unlock()
	_, legacy := q.legacyPeers[addr]
	return legacy
}

// legacyRetest is how long a node that owns none of a topic's
// partitions keeps polling it after one of its owners refused a
// registration. Past it the node parks its consumers on tokens again,
// and the registration that sends is what tests the owner once more, so
// an owner upgraded mid-roll is back on notifications within this
// interval.
const legacyRetest = 2 * time.Minute

// pollingOwner reports whether any live remote owner of the topic
// refused a registration within legacyRetest. A consumer on a node that
// owns none of the topic's partitions then polls instead of parking on
// tokens: a token left with that owner is discarded, and a record on it
// would wait out the consumer's whole budget.
func (q *tokenRequester) pollingOwner(topicName string) bool {
	q.legacyMu.Lock()
	none := len(q.legacyPeers) == 0
	q.legacyMu.Unlock()
	if none {
		return false
	}
	routes, ok := q.router.routesForTopic(topicName)
	if !ok {
		return false
	}
	now := time.Now()
	q.legacyMu.Lock()
	defer q.legacyMu.Unlock()
	for _, entry := range routes.remoteEntries {
		addr := q.router.consumeOwnerAddr(entry)
		if at, legacy := q.legacyPeers[addr]; legacy && addr != "" && now.Sub(at) < legacyRetest {
			return true
		}
	}
	return false
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
// same reason: a register must not be cancelled by the consumer parking
// or being served.
//
// done, when set, is told how each send ended (nil for any reply, the
// transport error otherwise), so a registration that never reached its
// owner can be tried again.
func (q *tokenRequester) broadcast(ctx context.Context, addrs []string, delta nodewire.TokenDelta, done func(addr string, err error)) {
	// Each send carries its own budget, so nothing has to outlive the
	// sends just to release a shared timeout.
	sendCtx := context.WithoutCancel(ctx)
	for _, addr := range addrs {
		go func() {
			res, err := q.router.peer.RegisterTokensWithin(sendCtx, addr, tokenSendTimeout, delta)
			// The send stays fire-and-forget; the REPLY is still worth
			// reading, because it is the only place a peer tells us it
			// does not speak this protocol.
			q.noteRegisterResult(addr, res, err)
			if done != nil {
				done(addr, err)
			}
		}()
	}
}

// tokenSendTimeout bounds one registration. Tokens are advisory, so a
// slow owner is dropped from this round rather than holding up the
// consumer that triggered it.
const tokenSendTimeout = 2 * time.Second
