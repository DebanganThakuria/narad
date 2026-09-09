package messaging

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
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
type waiter struct {
	cw *ConsumeWaiter
	ch chan waiterDelivery
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
		log, ok := d.engine.logs.Peek(topicName, p)
		if !ok {
			continue
		}
		if free := log.HighWatermark() - d.engine.offsets.Next(topicName, p); free > 0 {
			total += int(free)
		}
	}
	return total
}

// pumpTopic hands out as many records as this topic has demand and
// reservable records, then returns. It stops on the first reservation
// that finds nothing, which is also what makes "a record someone else
// already took" free: the queue is never disturbed.
func (d *dispatcher) pumpTopic(topicName string) {
	st := d.stateFor(topicName)
	for {
		st.mu.Lock()
		e, ok := st.queue.peek()
		if !ok {
			st.hasWaiters.Store(false)
			st.mu.Unlock()
			return
		}

		if e.remote != nil {
			if e.remote.Expired() {
				st.queue.pop()
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
			st.queue.rotate()
			st.mu.Unlock()
			if !e.remote.Notify(topicName, func(claiming bool) {
				d.resolveOutstanding(topicName, claiming)
			}) {
				// Nothing was spent, so take the claim back and stop.
				// Backpressure, not a lost interest.
				d.resolveOutstanding(topicName, false)
				return
			}
			continue
		}

		w := e.waiter
		st.queue.pop()
		st.mu.Unlock()

		// The reservation runs with a waiter already in hand and outside
		// the topic lock, so a slow log read never blocks arriving
		// consumers and nothing is ever reserved speculatively.
		msg, found, err := d.engine.tryQueueRead(context.Background(), topicName,
			w.cw.scan, w.cw.scanStart, w.cw.visibilityTimeout)
		if err != nil || !found {
			st.mu.Lock()
			st.queue.pushFront(queueEntry{waiter: w})
			st.mu.Unlock()
			return
		}
		d.engine.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		// Buffered, and this waiter is off the queue, so exactly one
		// delivery can ever be sent and the send cannot block.
		w.ch <- waiterDelivery{msg: msg}
	}
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

// resolveOutstanding retires one in-flight notification.
//
// A peer that DECLINED frees its record at once, so the pump is kicked
// to offer it to whoever is next. A peer that said it would claim keeps
// its hold until the deadline: retiring it immediately would let the
// pump promise the very same record to a second peer while the first is
// still on its way to collect it.
func (d *dispatcher) resolveOutstanding(topicName string, claiming bool) {
	if claiming {
		time.AfterFunc(claimDeadline, func() { d.retireOutstanding(topicName) })
		return
	}
	d.retireOutstanding(topicName)
	d.markDirty(topicName)
}

func (d *dispatcher) retireOutstanding(topicName string) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	if st.outstanding > 0 {
		st.outstanding--
	}
	st.mu.Unlock()
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
	if st.queue.len() == 0 {
		st.hasWaiters.Store(false)
	}
	st.mu.Unlock()
}

// enqueue parks a waiter and wakes the pump, because a new waiter can
// satisfy the gate just as new data can.
func (d *dispatcher) enqueue(topicName string, w *waiter) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	st.queue.push(queueEntry{waiter: w})
	if st.scan == nil {
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
	removed := st.queue.removeWaiter(w)
	if st.queue.len() == 0 {
		st.hasWaiters.Store(false)
	}
	st.mu.Unlock()
	if removed {
		// Still queued, so the pump never took it and no delivery exists.
		return waiterDelivery{}, false
	}
	select {
	case dl := <-w.ch:
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
		for {
			e, ok := st.queue.pop()
			if !ok {
				break
			}
			if e.waiter != nil {
				close(e.waiter.ch)
			}
		}
		st.hasWaiters.Store(false)
		st.mu.Unlock()
	}
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
