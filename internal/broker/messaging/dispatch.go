package messaging

import (
	"context"
	"sync"
	"sync/atomic"

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

// waiterQueue is a FIFO of parked consumers for one topic. head lets a
// pop be undone in O(1) when the reservation that followed it failed;
// pushFront is only ever called to undo the pop immediately above it.
type waiterQueue struct {
	items []*waiter
	head  int
}

func (q *waiterQueue) len() int { return len(q.items) - q.head }

func (q *waiterQueue) push(w *waiter) { q.items = append(q.items, w) }

func (q *waiterQueue) pop() *waiter {
	if q.head >= len(q.items) {
		return nil
	}
	w := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	if q.head == len(q.items) {
		q.items, q.head = q.items[:0], 0
	}
	return w
}

// pushFront returns a popped waiter to the head. Valid only immediately
// after a pop, which guarantees the slot is free.
func (q *waiterQueue) pushFront(w *waiter) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = w
		return
	}
	q.items = append([]*waiter{w}, q.items...)
}

// remove drops w from the queue if it is still there, reporting whether
// it was found. A waiter the pump already took is gone from the queue
// and its delivery (if any) is drained by the caller.
func (q *waiterQueue) remove(w *waiter) bool {
	for i := q.head; i < len(q.items); i++ {
		if q.items[i] != w {
			continue
		}
		copy(q.items[i:], q.items[i+1:])
		q.items[len(q.items)-1] = nil
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
	queue waiterQueue
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

// pumpTopic hands out as many records as this topic has waiters and
// reservable records, then returns. It stops on the first reservation
// that finds nothing, which is also what makes "a record someone else
// already took" free: the waiters are never disturbed.
func (d *dispatcher) pumpTopic(topicName string) {
	st := d.stateFor(topicName)
	for {
		st.mu.Lock()
		w := st.queue.pop()
		if w == nil {
			st.hasWaiters.Store(false)
			st.mu.Unlock()
			return
		}
		st.mu.Unlock()

		// The reservation runs with a waiter already in hand and outside
		// the topic lock, so a slow log read never blocks arriving
		// consumers and nothing is ever reserved speculatively.
		msg, found, err := d.engine.tryQueueRead(context.Background(), topicName,
			w.cw.scan, w.cw.scanStart, w.cw.visibilityTimeout)
		if err != nil || !found {
			st.mu.Lock()
			st.queue.pushFront(w)
			st.mu.Unlock()
			return
		}
		d.engine.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		// Buffered, and this waiter is off the queue, so exactly one
		// delivery can ever be sent and the send cannot block.
		w.ch <- waiterDelivery{msg: msg}
	}
}

// enqueue parks a waiter and wakes the pump, because a new waiter can
// satisfy the gate just as new data can.
func (d *dispatcher) enqueue(topicName string, w *waiter) {
	st := d.stateFor(topicName)
	st.mu.Lock()
	st.queue.push(w)
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
	removed := st.queue.remove(w)
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
			w := st.queue.pop()
			if w == nil {
				break
			}
			close(w.ch)
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
