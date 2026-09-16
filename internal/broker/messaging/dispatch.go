package messaging

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Queue-style consume delivery.
//
// The old shape woke every parked consumer on a partition and let them
// race: one broadcast closed the log's notify channel, every waiter
// rebuilt a reflect.SelectCase slice, took the partition shard mutex,
// scanned for a free offset, and all but one found nothing. Cost per
// commit scaled with the number of waiting consumers.
//
// This inverts it. Consumers do not scan and do not race. A consumer
// enqueues itself and blocks on one buffered channel; a single pump
// goroutine does the reservation once and hands the record to exactly
// one waiter. Cost per commit scales with messages delivered, not with
// consumers waiting.
//
// Two conditions gate a delivery: a record must be reservable AND
// someone must be waiting for it. Because the gate is a conjunction,
// the pump has to be woken by a change to EITHER side:
//
//   - a waiter arrived  -> enqueue kicks the pump
//   - data became available -> the log's wake notifier marks the topic
//     dirty and kicks (fires on a high-watermark advance, a nack, a
//     lease expiry, or a close)
//
// Waking on only one of them is the classic bug: a consumer that
// arrives after the data would wait for the next record, which may
// never come.
//
// The reservation happens only once a waiter has been taken off the
// queue, so nothing is ever reserved speculatively and there is no
// give-back path on this side.

// waiterDelivery is what the pump hands a parked consumer.
type waiterDelivery struct {
	msg topic.Message
}

// waiter is one parked queue-style consume. ch is buffered so the pump
// never blocks handing over, and receives at most one delivery because
// the waiter is removed from the queue before the send.
//
// abandoned covers the window in which the pump has taken this waiter
// off the queue but has not finished reserving for it: the waiter is in
// neither the queue nor the channel, so a consumer that gives up in
// that instant cannot see the record it is about to be handed. The flag
// is guarded by the topic's mutex, and the pump reads it under that
// same lock immediately before the send, so exactly one of the two ends
// up owning the record: either the pump delivers it, or it learns
// nobody is left to read it and gives it back. Getting this wrong
// leaves a reserved record invisible until its visibility timeout, and
// under load that treadmill is indistinguishable from a stall.
type waiter struct {
	cw *ConsumeWaiter
	ch chan waiterDelivery

	// guarded by the owning topicDispatch's mu.
	abandoned bool
}

// RemoteDemand is a peer's standing interest in a topic, registered by
// the cluster layer. It sits in the same queue as local consumers and
// the pump cannot tell the two apart, except in one decisive way: a
// local waiter is handed a reserved record, whereas remote demand is
// only *told* that records may be available and claims them itself with
// an ordinary consume. Nothing is ever reserved on a peer's behalf, so
// there is no give-back if that peer never comes back.
type RemoteDemand interface {
	// Notify asks the cluster layer to tell the peer records may be
	// available. It must not block on the network: queue the frame and
	// return. done is invoked later with whether the peer said it would
	// claim; a false verdict frees the record for someone else at once.
	//
	// Reporting false means nothing was spent (the peer's outbound queue
	// was full, say), and the pump leaves this entry in place and stops
	// rather than dropping the interest on the floor.
	Notify(topicName string, done func(claiming bool)) bool

	// Expired reports that this interest is finished — spent, timed out,
	// or its connection is gone — and should leave the queue.
	Expired() bool
}

// queueEntry is one unit of demand: exactly one field is set.
type queueEntry struct {
	waiter *waiter
	remote RemoteDemand
}

// entryQueue is the per-topic demand FIFO. Local waiters are popped
// when served; remote demand rotates to the back so a peer with a lot
// of outstanding interest cannot take every turn ahead of one with a
// little. head lets a pop be undone in O(1) when the reservation that
// followed it failed.
type entryQueue struct {
	items []queueEntry
	head  int
}

func (q *entryQueue) len() int { return len(q.items) - q.head }

func (q *entryQueue) push(e queueEntry) {
	q.items = append(q.items, e)
	q.compact()
}

func (q *entryQueue) peek() (queueEntry, bool) {
	if q.head >= len(q.items) {
		return queueEntry{}, false
	}
	return q.items[q.head], true
}

func (q *entryQueue) pop() (queueEntry, bool) {
	if q.head >= len(q.items) {
		return queueEntry{}, false
	}
	e := q.items[q.head]
	q.items[q.head] = queueEntry{}
	q.head++
	if q.head == len(q.items) {
		q.items, q.head = q.items[:0], 0
	}
	return e, true
}

// pushFront returns a popped entry to the head. Valid only immediately
// after a pop, which guarantees the slot is free.
func (q *entryQueue) pushFront(e queueEntry) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = e
		return
	}
	q.items = append([]queueEntry{e}, q.items...)
}

// rotate moves the head entry to the back. Remote demand rotates rather
// than being consumed so turns spread across peers.
func (q *entryQueue) rotate() {
	if e, ok := q.pop(); ok {
		q.push(e)
	}
}

// compact reclaims the popped prefix once it dominates the slice, so a
// long-lived rotating entry cannot grow the backing array without
// bound.
func (q *entryQueue) compact() {
	if q.head == 0 || q.head < len(q.items)/2 {
		return
	}
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = queueEntry{}
	}
	q.items, q.head = q.items[:n], 0
}

// removeWaiter drops a local waiter if it is still queued, reporting
// whether it was found. One the pump already took is gone from the
// queue and its delivery is drained by the caller.
func (q *entryQueue) removeWaiter(w *waiter) bool {
	return q.removeMatching(func(e queueEntry) bool { return e.waiter == w })
}

// removeRemote drops a peer's interest, used when its connection dies
// or it tells us it no longer wants the topic.
func (q *entryQueue) removeRemote(d RemoteDemand) bool {
	return q.removeMatching(func(e queueEntry) bool { return e.remote == d })
}

func (q *entryQueue) removeMatching(match func(queueEntry) bool) bool {
	for i := q.head; i < len(q.items); i++ {
		if !match(q.items[i]) {
			continue
		}
		copy(q.items[i:], q.items[i+1:])
		q.items[len(q.items)-1] = queueEntry{}
		q.items = q.items[:len(q.items)-1]
		return true
	}
	return false
}

// topicDispatch is the per-topic waiter set. hasWaiters is read on the
// produce hot path (through the log's wake notifier) so it must stay an
// atomic load, never a mutex acquisition.
type topicDispatch struct {
	hasWaiters atomic.Bool

	mu    sync.Mutex
	queue entryQueue
	// outstanding counts notifications sent to peers that have not yet
	// resolved. Each one is a claim on one record, so the pump will not
	// promise the same record to a second peer.
	outstanding int
	// scan is the topic's locally owned partitions, captured when demand
	// is registered so the pump can size consumable() without a metadata
	// lookup on every visit.
	scan []int
	// holds are the notifications a peer said it would claim, oldest
	// first, each with its deadline timer. A claim arriving retires the
	// oldest one at once instead of letting it run out (see claimArrived).
	holds []*claimHold
	// gen counts releases of this topic's state. Entries are never removed
	// from the map (an open log's wake notifier holds a pointer to one),
	// so a release resets the state in place and bumps gen; a pump that
	// took a waiter off the queue before the release sees the change and
	// wakes the waiter empty instead of re-queuing it on dead state.
	gen uint64
	// pinned holds the waiters of partition-pinned long-polls, one FIFO
	// per partition, apart from queue. The pump stops a FIFO at the first
	// waiter it cannot serve, which is right only when every waiter in it
	// scans the same partitions: a pinned waiter at the head of the shared
	// queue stalled every unpinned waiter behind it for as long as its one
	// partition stayed empty. Queues are never removed from the map (a
	// pump may hold one across the topic lock); a topic has at most one
	// per partition.
	pinned map[int]*entryQueue
	// lastReadErrLog is the unix time in nanoseconds of the last log line
	// about a read the pump could not complete, so a broken disk under a
	// busy topic logs once every few seconds rather than once per commit.
	lastReadErrLog atomic.Int64
}

// queueFor returns the FIFO a waiter belongs on: the per-partition one
// for a pinned consume, the shared one otherwise. With create false a
// missing pinned FIFO reads as nil. Must hold mu.
func (st *topicDispatch) queueFor(w *waiter, create bool) *entryQueue {
	if w.cw.pinned == nil {
		return &st.queue
	}
	p := *w.cw.pinned
	q, ok := st.pinned[p]
	if !ok && create {
		if st.pinned == nil {
			st.pinned = make(map[int]*entryQueue)
		}
		q = &entryQueue{}
		st.pinned[p] = q
	}
	return q
}

// anyDemandLocked reports whether any FIFO of the topic still holds an
// entry. Must hold mu.
func (st *topicDispatch) anyDemandLocked() bool {
	if st.queue.len() > 0 {
		return true
	}
	for _, q := range st.pinned {
		if q.len() > 0 {
			return true
		}
	}
	return false
}

// drainLocked empties every FIFO, closing each local waiter's channel so
// its ConsumeWait answers empty at once. Must hold mu.
func (st *topicDispatch) drainLocked() {
	drain := func(q *entryQueue) {
		for {
			e, ok := q.pop()
			if !ok {
				return
			}
			if e.waiter != nil {
				close(e.waiter.ch)
			}
		}
	}
	drain(&st.queue)
	for _, q := range st.pinned {
		drain(q)
	}
}

// claimHold is one notification a peer promised to claim. Its identity
// is what the deadline callback and claimArrived agree on; the timer is
// only ever touched under the topic's mutex.
type claimHold struct {
	timer *time.Timer
}

// dispatcher owns every topic's waiter set and the single pump
// goroutine that feeds them.
type dispatcher struct {
	engine *Engine

	mu     sync.RWMutex
	topics map[string]*topicDispatch

	dirtyMu sync.Mutex
	dirty   map[string]struct{}

	kick      chan struct{}
	stop      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
}

// maxPumpBatch bounds how many topics one pump wake services before it
// re-checks its wake sources, so a burst that dirties thousands of
// topics cannot delay a newly-hot one behind all of them.
const maxPumpBatch = 64

func newDispatcher(e *Engine) *dispatcher {
	return &dispatcher{
		engine: e,
		topics: make(map[string]*topicDispatch),
		dirty:  make(map[string]struct{}),
		kick:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// start launches the pump. Safe to call more than once.
func (d *dispatcher) start() {
	d.startOnce.Do(func() { go d.run() })
}

// close stops the pump and releases every parked waiter. Deliveries
// already handed out are unaffected. Idempotent: the engine's Close and
// a lifecycle hook may both reach it.
func (d *dispatcher) close() {
	d.stopOnce.Do(func() { close(d.stop) })
	<-d.done
}

// stateFor returns the topic's waiter set, creating it on first use.
// Entries are never removed: one empty struct per topic name this node
// has served a long-poll consume for is cheap, and dropping them would
// race the wake notifier holding a pointer to one.
// peekState returns the topic's dispatch state without creating one.
// Paths driven by peers (a claim arriving, a hold expiring, a retire
// after a release) use it so an RPC naming a topic this node has no
// interest in cannot grow the map: with no state there is nothing to
// retire.
func (d *dispatcher) peekState(topicName string) *topicDispatch {
	d.mu.RLock()
	st := d.topics[topicName]
	d.mu.RUnlock()
	return st
}

func (d *dispatcher) stateFor(topicName string) *topicDispatch {
	d.mu.RLock()
	st, ok := d.topics[topicName]
	d.mu.RUnlock()
	if ok {
		return st
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if st, ok := d.topics[topicName]; ok {
		return st
	}
	st = &topicDispatch{}
	d.topics[topicName] = st
	return st
}

// markDirty records that a topic may have deliverable records and wakes
// the pump. The kick channel has capacity one and the send is
// non-blocking: either it lands, or the buffer was already full, which
// means a wake is already pending. A kick carries no information beyond
// "look again", so dropping one is always safe — unlike a channel
// carrying topic names, where a dropped send would lose work.
func (d *dispatcher) markDirty(topicName string) {
	d.dirtyMu.Lock()
	d.dirty[topicName] = struct{}{}
	d.dirtyMu.Unlock()
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// drainDirty removes up to maxPumpBatch topics from the dirty set and
// reports whether any were left behind, so the caller can re-kick
// itself instead of holding the pump on one burst.
func (d *dispatcher) drainDirty(dst []string) ([]string, bool) {
	d.dirtyMu.Lock()
	defer d.dirtyMu.Unlock()
	dst = dst[:0]
	for name := range d.dirty {
		delete(d.dirty, name)
		dst = append(dst, name)
		if len(dst) >= maxPumpBatch {
			break
		}
	}
	return dst, len(d.dirty) > 0
}

// run is the pump: one goroutine per broker. It sleeps whenever there
// is nothing to hand out, so an idle topic costs nothing at all.
func (d *dispatcher) run() {
	defer close(d.done)
	var batch []string
	for {
		select {
		case <-d.kick:
		case <-d.stop:
			d.releaseAll()
			return
		}
		for {
			var more bool
			batch, more = d.drainDirty(batch)
			for _, name := range batch {
				d.pumpTopic(name)
			}
			if !more {
				break
			}
			select {
			case <-d.stop:
				d.releaseAll()
				return
			default:
			}
		}
	}
}

// consumable estimates how many records on the topic's local partitions
// are free to hand out: everything between the reservation frontier and
// the visible tail. Both halves are O(1), so the pump can gate on this
// without scanning. It is an estimate on purpose and may over-report
// (a partition paused for handoff, or one at its in-flight cap), which
// costs at most a notification the peer answers empty.
func (d *dispatcher) consumable(topicName string, scan []int) int {
	total := 0
	for _, p := range scan {
		// Live when the log is open, persisted when it is not: a backlog
		// in a log nobody has opened since a restart or an idle eviction
		// is exactly what a token holder needs to hear about.
		tail, ok := d.engine.logs.PeekHighWatermark(topicName, p)
		if !ok {
			continue
		}
		// Next is the ack frontier, so records handed out but not yet
		// acked (in flight) and records acked ahead of a gap both sit
		// above it and are not free. Counting them offered records that
		// were already taken, and every such offer cost the peer a claim
		// that came back empty and this topic a claimDeadline of silence.
		next, inFlight, ackedAhead, ok := d.engine.offsets.Reservable(topicName, p)
		if !ok {
			// No shard yet (nothing touched the partition since the
			// process started): the frontier is the persisted one. Without
			// it a fully drained partition reads as its whole history.
			if committed, found, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(d.engine.logs.DataDir(), topicName, p)); err == nil && found {
				next = committed + 1
			}
		}
		free := tail - next - int64(inFlight+ackedAhead)
		if free > 0 {
			total += int(free)
		}
	}
	return total
}

// pumpTopic hands out as many records as this topic has demand and
// reservable records, then returns. Pinned waiters go first: each can
// take from one partition only, so serving them first costs the
// unpinned waiters nothing they could not get elsewhere, and it keeps a
// pinned consumer from starving behind consumers that scan every
// partition. The shared queue (unpinned waiters and peers' tokens) is
// pumped last.
func (d *dispatcher) pumpTopic(topicName string) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	pinned := make([]*entryQueue, 0, len(st.pinned))
	for _, q := range st.pinned {
		pinned = append(pinned, q)
	}
	st.mu.Unlock()
	for _, q := range pinned {
		d.pumpQueue(topicName, st, q)
	}
	d.pumpQueue(topicName, st, &st.queue)
}

// pumpQueue is pumpTopic for one FIFO. It stops on the first reservation
// that finds nothing, which is also what makes "a record someone else
// already took" free: the queue is never disturbed. Every waiter in one
// FIFO scans the same partitions, so a miss for the head is a miss for
// all of them.
func (d *dispatcher) pumpQueue(topicName string, st *topicDispatch, q *entryQueue) {
	for {
		st.mu.Lock()
		e, ok := q.peek()
		if !ok {
			st.hasWaiters.Store(st.anyDemandLocked())
			st.mu.Unlock()
			return
		}

		if e.remote != nil {
			if e.remote.Expired() {
				q.pop()
				st.mu.Unlock()
				continue
			}
			// Never promise the same record to two peers: every
			// notification in flight is a claim on one of them.
			if d.consumable(topicName, st.scan) <= st.outstanding {
				st.mu.Unlock()
				return
			}
			st.outstanding++
			// The hold exists BEFORE the notification goes out: the peer's
			// claim can reach this node before the notify round trip has
			// even returned, and it must find the hold it resolves.
			h := d.addHoldLocked(topicName, st)
			q.rotate()
			st.mu.Unlock()
			if !e.remote.Notify(topicName, func(claiming bool) {
				if !claiming {
					// The peer passed: nothing is coming to claim, so free
					// the record for the next peer at once.
					d.retireHold(topicName, h)
				}
			}) {
				// Nothing was spent, so take the claim back and stop.
				// Backpressure, not a lost interest.
				d.retireHold(topicName, h)
				return
			}
			continue
		}

		w := e.waiter
		q.pop()
		gen := st.gen
		st.mu.Unlock()

		// The reservation runs with a waiter already in hand and outside
		// the topic lock, so a slow log read never blocks arriving
		// consumers and nothing is ever reserved speculatively.
		msg, found, err := d.engine.readForWaiter(w.cw)
		if err != nil || !found {
			dead := err != nil && unservable(err)
			st.mu.Lock()
			// A consumer that gave up while the read was running is no
			// longer waiting for anything, so it does not go back on the
			// queue. Nothing was reserved, so there is nothing to release.
			if !w.abandoned {
				switch {
				case st.gen != gen:
					// The topic was released (deleted) while the read ran:
					// the queue this waiter came from was drained and its
					// siblings woken, so wake it empty now rather than
					// leaving it to sleep out its wait on a fresh queue.
					close(w.ch)
				case dead:
					// Nothing on this node can serve it any more (its
					// partition moved away, or the topic is gone). Wake it
					// empty; the client's next poll is routed afresh.
					close(w.ch)
				default:
					q.pushFront(queueEntry{waiter: w})
				}
			}
			st.hasWaiters.Store(st.anyDemandLocked())
			st.mu.Unlock()
			if err != nil && !dead {
				d.logReadError(topicName, st, err)
			}
			return
		}

		// Resolve against a consumer that may have given up during the
		// read. Both this check and dequeue's flag write happen under
		// st.mu, so the send is ordered before any observation of the
		// queue that could conclude the record was never handed over.
		st.mu.Lock()
		abandoned := w.abandoned
		if !abandoned && st.gen != gen {
			// The topic was released (deleted) while the read ran: the
			// record belongs to state being torn down and the consumer
			// must not be handed it. Wake it empty and give the record
			// back.
			close(w.ch)
			abandoned = true
		}
		if !abandoned {
			// Buffered, and this waiter is off the queue, so exactly one
			// delivery can ever be sent and the send cannot block.
			w.ch <- waiterDelivery{msg: msg}
		}
		st.mu.Unlock()

		if abandoned {
			// Reserved for someone who is gone. Give it back now rather
			// than leaving it invisible for a whole visibility timeout.
			d.engine.releaseUndelivered(topicName, msg)
			continue
		}
		d.engine.recordConsumed(topicName, msg.Partition, len(msg.Payload))
	}
}

// unservable reports a read error that no later wake can fix on this
// node: the waiter's partition is not owned here, or its topic is gone.
func unservable(err error) bool {
	return errors.Is(err, ErrNotPartitionOwner) || errors.Is(err, ErrTopicNotFound)
}

// readErrLogInterval bounds how often one topic logs a failed pump read.
const readErrLogInterval = 5 * time.Second

// logReadError reports a read the pump could not complete for a parked
// consumer, at most once per readErrLogInterval per topic. The consumer
// stays parked and is retried on the next wake; without a line here a
// broken disk looked exactly like an idle topic.
func (d *dispatcher) logReadError(topicName string, st *topicDispatch, err error) {
	now := time.Now().UnixNano()
	last := st.lastReadErrLog.Load()
	if now-last < int64(readErrLogInterval) || !st.lastReadErrLog.CompareAndSwap(last, now) {
		return
	}
	d.engine.logger.Warn("dispatcher could not read for a parked consumer; it stays parked until the next wake",
		"topic", topicName, "err", err)
}

// claimDeadline is how long a peer that said it would claim holds its
// record. It can be aggressive, because being wrong costs one wasted
// round trip and never a double delivery: two peers claiming the same
// record both call ReserveNext, which is atomic, so one wins and the
// other gets an empty answer and re-registers. Contrast the visibility
// timeout, which must stay conservative precisely because being wrong
// there hands one record to two consumers.
//
// TODO: derive this per peer from observed claim latency, so a slow but
// healthy peer is not written off on every notification.
const claimDeadline = time.Second

// addHoldLocked records a notification a peer is about to be sent, with
// the deadline after which the record is offered elsewhere if no claim
// arrived. Called with st.mu held, before the notification leaves.
//
// A peer that DECLINES frees its record at once (retireHold from the
// notify callback), so the pump is kicked to offer it to whoever is
// next. A peer that said it would claim keeps its hold until the claim
// arrives (claimArrived) or the deadline fires (expireHold): retiring it
// earlier would let the pump promise the very same record to a second
// peer while the first is still on its way to collect it.
func (d *dispatcher) addHoldLocked(topicName string, st *topicDispatch) *claimHold {
	h := &claimHold{}
	// The callback captures h, allocated before the timer exists, so it
	// never reads a field written after AfterFunc returned.
	h.timer = time.AfterFunc(claimDeadline, func() { d.expireHold(topicName, h) })
	st.holds = append(st.holds, h)
	return h
}

// retireHold gives back one specific hold, if it is still held: the
// peer declined, the notification was never spent, or the deadline
// fired. A hold that claimArrived already retired is no longer in the
// list and is skipped, so each hold is retired exactly once.
func (d *dispatcher) retireHold(topicName string, h *claimHold) {
	st := d.peekState(topicName)
	if st == nil {
		// Released with the topic; its holds were cancelled there.
		return
	}
	st.mu.Lock()
	found := false
	for i, held := range st.holds {
		if held == h {
			st.holds = append(st.holds[:i], st.holds[i+1:]...)
			found = true
			break
		}
	}
	if found && st.outstanding > 0 {
		// Same critical section as the removal, so a release in between
		// cannot leave the next generation's count one below its holds.
		st.outstanding--
	}
	st.mu.Unlock()
	if found {
		h.timer.Stop()
		// The wake is the load-bearing half: retiring frees capacity the
		// gate `consumable() <= outstanding` was withholding, and the
		// pump only ever looks at a topic it has been told about.
		d.markDirty(topicName)
	}
}

// expireHold is the deadline path: the peer never came to claim (or its
// claim lost the race and it re-registered), so the hold is given back.
func (d *dispatcher) expireHold(topicName string, h *claimHold) {
	d.retireHold(topicName, h)
}

// claimArrived retires the oldest hold the moment a peer's claim reaches
// this node. The hold only ever existed to keep the pump from promising
// the claimant's record to a second peer while the claim was in flight;
// once the claim is here that record is either reserved or lost, and
// either way the count of free records has moved on. Left to the
// deadline, the hold gated every later record on this topic for up to
// claimDeadline: a consumer parked on another node saw each message of
// a sparse stream arrive a full second late, since the hold from the
// previous message was still counted against the next one.
//
// Only a request flagged as a claim reaches here (the wire's Claim
// field); a probe never does, so it cannot release a hold promised to
// another peer. A claim for a topic with no state or no holds does
// nothing.
func (d *dispatcher) claimArrived(topicName string) {
	st := d.peekState(topicName)
	if st == nil {
		return
	}
	st.mu.Lock()
	if len(st.holds) == 0 {
		st.mu.Unlock()
		return
	}
	// Oldest first: notifications and their claims travel in order, so
	// the oldest hold is the one this claim answers. A claim that arrives
	// after its own hold already expired retires the next peer's live
	// hold instead; the pump may then offer that record to a third peer,
	// which costs one empty claim and never a double delivery, since
	// ReserveNext is atomic.
	h := st.holds[0]
	st.holds = st.holds[1:]
	if st.outstanding > 0 {
		st.outstanding--
	}
	timer := h.timer
	st.mu.Unlock()
	// Stopping may fail because the deadline is firing right now; that
	// callback will not find the hold in the list and skips, so the
	// single retirement here is still the only one.
	timer.Stop()
	d.markDirty(topicName)
}

// registerRemote adds a peer's interest to the topic's demand queue.
// scan is the topic's locally owned partitions, kept so the pump can
// size consumable() without a metadata lookup per visit.
func (d *dispatcher) registerRemote(topicName string, scan []int, rd RemoteDemand) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	st.scan = scan
	st.queue.push(queueEntry{remote: rd})
	st.hasWaiters.Store(true)
	st.mu.Unlock()
	d.markDirty(topicName)
}

// dropRemote removes a peer's interest, for a connection that died or a
// peer that said it no longer wants the topic.
func (d *dispatcher) dropRemote(topicName string, rd RemoteDemand) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	st.queue.removeRemote(rd)
	st.hasWaiters.Store(st.anyDemandLocked())
	st.mu.Unlock()
}

// enqueue parks a waiter and wakes the pump, because a new waiter can
// satisfy the gate just as new data can.
func (d *dispatcher) enqueue(topicName string, w *waiter) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	st.queueFor(w, true).push(queueEntry{waiter: w})
	if st.scan == nil && w.cw.pinned == nil {
		st.scan = w.cw.scan
	}
	st.hasWaiters.Store(true)
	st.mu.Unlock()
	d.markDirty(topicName)
}

// dequeue removes a waiter that gave up. It returns any delivery the
// pump handed over in the meantime so the caller can give the message
// back: that record is reserved, and dropping it here would leave it
// invisible until its visibility timeout.
func (d *dispatcher) dequeue(topicName string, w *waiter) (waiterDelivery, bool) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	removed := false
	if q := st.queueFor(w, false); q != nil {
		removed = q.removeWaiter(w)
	}
	if !removed {
		// The pump has this waiter off the queue and may be reserving on
		// its behalf right now. Say so before releasing the lock: from
		// here the pump either has already put a delivery in the buffer
		// (the receive below finds it) or will see this flag and give the
		// record back itself.
		w.abandoned = true
	}
	st.hasWaiters.Store(st.anyDemandLocked())
	st.mu.Unlock()
	if removed {
		// Still queued, so the pump never took it and no delivery exists.
		return waiterDelivery{}, false
	}
	select {
	case dl, ok := <-w.ch:
		if !ok {
			// Closed by a release (the topic was deleted): nothing was
			// handed over, and the zero value must never be served as a
			// record.
			return waiterDelivery{}, false
		}
		return dl, true
	default:
		return waiterDelivery{}, false
	}
}

// releaseAll wakes every parked waiter on shutdown so no consume hangs
// past the engine's lifetime. Waiters see a closed channel and answer
// empty.
func (d *dispatcher) releaseAll() {
	d.mu.RLock()
	states := make([]*topicDispatch, 0, len(d.topics))
	for _, st := range d.topics {
		states = append(states, st)
	}
	d.mu.RUnlock()
	for _, st := range states {
		st.mu.Lock()
		st.drainLocked()
		st.hasWaiters.Store(false)
		st.mu.Unlock()
	}
}

// releaseTopic wakes every consumer parked on a topic that no longer
// exists and resets the topic's dispatch state in place. Each local
// waiter's channel is closed, which ConsumeWait reads as "the dispatcher
// shut down under us" and turns into an empty answer, so a consumer
// parked on a deleted topic learns at once instead of sleeping out its
// wait. Peers' tokens are dropped from the queue and their holds
// cancelled; the peers re-register if they still care, and find the
// topic gone.
//
// The map entry itself stays: every open partition log carries a wake
// notifier that captured a pointer to this state at open time, and a
// same-name recreate opens its logs before the old incarnation's state
// is retired, so removing the entry would leave those notifiers marking
// a state nobody pumps. Resetting in place and bumping gen keeps every
// pointer valid.
func (d *dispatcher) releaseTopic(topicName string) {
	st := d.peekState(topicName)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.drainLocked()
	for _, h := range st.holds {
		h.timer.Stop()
	}
	st.holds = nil
	st.outstanding = 0
	st.hasWaiters.Store(false)
	st.gen++
	st.mu.Unlock()
}

// wakeNotifier returns the callback installed on a partition log for
// (topicName, _). It runs on the committing goroutine, so it does the
// least possible work: one atomic load, and nothing at all unless this
// topic actually has a parked consumer.
func (d *dispatcher) wakeNotifier(topicName string) func() {
	st := d.stateFor(topicName)
	return func() {
		if !st.hasWaiters.Load() {
			return
		}
		d.markDirty(topicName)
	}
}
