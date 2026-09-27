package messaging

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// MaxConsumeBatch is the largest max a batch consume may ask for, and
// the most receipt handles one batch ack may carry.
const MaxConsumeBatch = 100

// ConsumeWeight is how many records a consume request may hold reserved
// when it returns: its max for a batch consume (GET /consume?max=N),
// else 1. The per-identity in-flight limiter counts a request by it, so
// a batch of N takes N of the identity's budget. A max the handler will
// refuse counts 1.
//
// A plain consume costs two substring checks: a query without "max" (or
// an escape that could spell it) is not parsed a second time.
func ConsumeWeight(r *http.Request) int {
	raw := r.URL.RawQuery
	if !strings.Contains(raw, "max") && !strings.Contains(raw, "%") {
		return 1
	}
	n, err := strconv.Atoi(consumeQueryFromRawQuery(raw).max)
	if err != nil || n < 1 || n > MaxConsumeBatch {
		return 1
	}
	return n
}

// consumeBatch serves GET /consume?max=N: up to N records in one
// response, {"messages":[...]}, each encoded exactly as a single consume
// encodes it and each with its own receipt handle and visibility window.
// It answers 204 when none materializes within wait, as a single consume
// does.
//
// The records come from one non-blocking scan of this node's partitions
// (Engine.ConsumeBatch). Only when that finds nothing does the request
// fall back on the single-record machinery (remote owners, the long-poll
// and the token protocol), which delivers one record; a record that
// arrives by a wait is topped up with another local scan. The request is
// never held to fill N: a caller that wants N records soon asks with a
// wait and gets what is there when the first one lands.
func consumeBatch(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts, localOnly bool, max int) {
	bc, ok := s.Deps.Broker.(broker.BatchConsumer)
	if !ok {
		// A broker without the batch surface: serve one record, through
		// exactly the single-record flow.
		c := &captureWriter{}
		consumeSingle(s, c, r, topicName, opts, localOnly)
		writeCapturedBatch(w, c, nil)
		return
	}
	b := &batchConsume{s: s, bc: bc, w: w, r: r, topic: topicName, max: max}

	// A peer's fan-out probe: strictly local and non-blocking.
	if localOnly && isQueueConsume(opts) {
		opts.Wait = 0
		b.local(opts)
		return
	}
	if s.Deps.Router != nil {
		c := &captureWriter{}
		forwarded, localPartition := s.Deps.Router.RouteConsume(r.Context(), c, r, topicName, opts.Partition)
		if forwarded {
			// Served by another node (a pinned partition it owns, or the
			// remote owners of a topic this node holds none of).
			writeCapturedBatch(w, c, nil)
			return
		}
		if localPartition != nil && isQueueConsume(opts) {
			b.withLocalOwner(opts, *localPartition)
			return
		}
	}
	b.local(opts)
}

// batchConsume is one batch consume request in flight.
type batchConsume struct {
	s     *handlers.Set
	bc    broker.BatchConsumer
	w     http.ResponseWriter
	r     *http.Request
	topic string
	max   int
}

// local serves a batch from this node alone: a single node, a pinned
// partition this node owns, or a peer's local-only probe. It mirrors
// consumeOnce: one scan, then the local wait.
func (b *batchConsume) local(opts brokermsg.ConsumeOpts) {
	queue := isQueueConsume(opts)
	msgs, waiter, err := b.bc.ConsumeBatch(b.r.Context(), b.topic, opts, b.max, nil)
	if queue && errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		// Ownership just moved: no message here.
		b.w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		b.s.WriteBrokerError(b.w, "consume", err)
		return
	}
	if len(msgs) > 0 {
		writeMessages(b.w, nil, msgs)
		return
	}
	if opts.Wait <= 0 || waiter == nil {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}
	msg, found, _, err := b.s.Deps.Broker.ConsumeWait(b.r.Context(), waiter, opts.Wait, nil)
	if queue && errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		b.s.WriteBrokerError(b.w, "consume", err)
		return
	}
	if !found {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}
	writeMessages(b.w, nil, b.topUp(opts, append(msgs, msg), 1))
}

// withLocalOwner is queueConsumeWithLocalOwner for a batch: one scan of
// the local partitions starting at the router's pick, then the remote
// owners, then the wait raced against the token protocol.
func (b *batchConsume) withLocalOwner(opts brokermsg.ConsumeOpts, localPartition int) {
	ctx := b.r.Context()
	router := b.s.Deps.Router
	probe := opts
	probe.Partition = nil
	probe.ScanStart = &localPartition
	probe.Wait = 0
	msgs, waiter, err := b.bc.ConsumeBatch(ctx, b.topic, probe, b.max, nil)
	if err != nil && !errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		b.s.WriteBrokerError(b.w, "consume", err)
		return
	}
	if len(msgs) > 0 {
		writeMessages(b.w, nil, msgs)
		return
	}

	c := &captureWriter{}
	if forwarded, _ := router.RouteConsumeRemote(ctx, c, b.r, b.topic); forwarded {
		writeCapturedBatch(b.w, c, nil)
		return
	}
	if opts.Wait <= 0 || waiter == nil {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}

	local := &localConsumeWaiter{s: b.s, topic: b.topic, waiter: waiter}
	c = &captureWriter{}
	if router.RouteConsumeWait(ctx, c, b.r, b.topic, opts.Wait, local) {
		var more []topic.Message
		if _, ok := c.message(); ok {
			more = b.topUp(probe, nil, 1)
		}
		writeCapturedBatch(b.w, c, more)
		return
	}

	msg, found, _, err := b.s.Deps.Broker.ConsumeWait(ctx, waiter, opts.Wait, nil)
	if err != nil && !errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		b.s.WriteBrokerError(b.w, "consume", err)
		return
	}
	if !found {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}
	writeMessages(b.w, nil, b.topUp(probe, []topic.Message{msg}, 1))
}

// topUp adds what one more non-blocking local scan finds to the held
// records a wait delivered (msgs, and any already encoded), up to the
// batch size: a wait wakes on the first record of a burst, and the rest
// are usually right behind it. A failure only ends the top-up; the
// records in hand are reserved and go out.
func (b *batchConsume) topUp(opts brokermsg.ConsumeOpts, msgs []topic.Message, held int) []topic.Message {
	room := b.max - held
	if room <= 0 {
		return msgs
	}
	opts.Wait = 0
	more, _, err := b.bc.ConsumeBatch(b.r.Context(), b.topic, opts, room, msgs)
	if err != nil {
		return msgs
	}
	return more
}

// writeMessages answers 200 with {"messages":[...]}: first (an already
// encoded message, may be nil) and then msgs, each as a single consume
// encodes it.
func writeMessages(w http.ResponseWriter, first []byte, msgs []topic.Message) {
	size := len(first) + 16
	for i := range msgs {
		size += len(msgs[i].Payload) + 160
	}
	body := make([]byte, 0, size)
	body = append(body, `{"messages":[`...)
	body = append(body, first...)
	for i := range msgs {
		if i > 0 || len(first) > 0 {
			body = append(body, ',')
		}
		body = msgs[i].AppendJSON(body)
	}
	body = append(body, "]}\n"...)
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// writeCapturedBatch answers a batch consume from what the single-record
// machinery wrote: a delivered record becomes a one-record batch (plus
// msgs, a top-up), and anything else (204, an error) is passed through
// unchanged.
func writeCapturedBatch(w http.ResponseWriter, c *captureWriter, msgs []topic.Message) {
	if first, ok := c.message(); ok {
		writeMessages(w, first, msgs)
		return
	}
	c.replay(w)
}
