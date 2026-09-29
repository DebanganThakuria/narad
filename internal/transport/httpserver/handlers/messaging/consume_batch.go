package messaging

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// MaxConsumeBatch is the largest max a batch consume may ask for, and
// the most receipt handles one batch ack may carry.
const MaxConsumeBatch = 100

// consumeBatchReplyBytes bounds the encoded records of one batch consume
// response: the records go out in the order taken until the next one
// would take the body past it, and the rest are given back at once (see
// batchConsume.write). The first record always goes, as a single consume
// would send it. A record is up to 1 MiB of key and payload, and text
// that is not JSON goes out as a JSON string, where a control byte or
// one of < > & takes six bytes, so 100 of them came to 600 MiB in one
// buffer. 8 MiB is the bound a batch forwarded from an owner already
// has (half the cluster RPC frame), so a batch is the same size
// whichever node serves it, and it holds a full reserve
// (consumeBatchReserveBytes) of JSON or binary records.
const consumeBatchReplyBytes = 8 << 20

// consumeBatchReserveBytes bounds the key and payload bytes one scan of a
// batch consume reserves (ConsumeOpts.MaxBytes), as the owner of a
// forwarded batch bounds it. JSON goes out as it is and binary grows by
// a third, so their records all fit consumeBatchReplyBytes; the reply
// bound only cuts records that escaping inflates, and without this one a
// batch of large records would reserve and read up to 100 MiB only to
// give most of it back.
const consumeBatchReserveBytes = 4 << 20

// BatchConsumeRequested reports whether a consume's raw query asks for
// a batch: a non-empty max, found the way the handler finds it (so no
// parser difference, such as url.ParseQuery giving up on a query of more
// than 10000 parameters, can hide it from a check the handler then
// ignores). A query without "max" or an escape that could spell it is
// not parsed.
func BatchConsumeRequested(rawQuery string) bool {
	if !strings.Contains(rawQuery, "max") && !strings.Contains(rawQuery, "%") {
		return false
	}
	return consumeQueryFromRawQuery(rawQuery).max != ""
}

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
// Where the records come from depends on what this node owns:
//
//   - A single node, a pinned partition this node owns, or a peer's
//     local-only probe (local): one non-blocking scan of this node's
//     partitions (Engine.ConsumeBatch), then the local wait, whose record
//     is topped up with another scan.
//   - A node that owns some of the topic's partitions (withLocalOwner):
//     one scan of them first. Only when that finds nothing does the
//     request fall back on the single-record machinery: a probe of the
//     remote owners, whose one record goes out alone, then the long-poll
//     raced against the token protocol, whose one record is topped up
//     with another local scan.
//   - A node that owns none of the topic's partitions, or not the pinned
//     one: the router forwards the request (RouteConsumeBatch) and asks
//     the owner for up to N, in the opening probes and in the wait phase
//     alike (the token claim, the re-probe and the polling fallback). The
//     owner builds the batch, and its {"messages":[...]} body goes out as
//     it is, without touching this node's broker. So such a node's batch,
//     parked or not, holds up to N records and up to the owner's reply
//     bound (8 MiB, as consumeBatchReplyBytes here), not one record.
//
// The request is never held to fill N, nor filled from several owners: a
// caller that wants N records soon asks with a wait and gets what is
// there when the first one lands. A scan reserves at most
// consumeBatchReserveBytes, and the response carries at most
// consumeBatchReplyBytes of encoded records.
func consumeBatch(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts, localOnly bool, max int) {
	bc, ok := s.Deps.Broker.(broker.BatchConsumer)
	if !ok {
		// A broker without the batch surface: serve one record, through
		// exactly the single-record flow.
		c := &captureWriter{}
		consumeSingle(s, c, r, topicName, opts, localOnly)
		writeCapturedBatch(w, c)
		return
	}
	b := &batchConsume{s: s, bc: bc, w: w, r: r, topic: topicName, max: max}
	opts.MaxBytes = consumeBatchReserveBytes

	// A peer's fan-out probe: strictly local and non-blocking.
	if localOnly && isQueueConsume(opts) {
		opts.Wait = 0
		b.local(opts)
		return
	}
	if s.Deps.Router != nil {
		c := &captureWriter{}
		var forwarded, batch bool
		var localPartition *int
		if br, ok := s.Deps.Router.(batchConsumeRouter); ok {
			forwarded, batch, localPartition = br.RouteConsumeBatch(r.Context(), c, r, topicName, opts.Partition, max)
		} else {
			forwarded, localPartition = s.Deps.Router.RouteConsume(r.Context(), c, r, topicName, opts.Partition)
		}
		if forwarded {
			// Served by another node (a pinned partition it owns, or the
			// remote owners of a topic this node holds none of): a batch
			// the owner built goes out as it is, and anything else as a
			// single-record flow's outcome.
			if batch && c.code() == http.StatusOK {
				writeBatchBody(w, c.body)
				return
			}
			writeCapturedBatch(w, c)
			return
		}
		if localPartition != nil && isQueueConsume(opts) {
			b.withLocalOwner(opts, *localPartition)
			return
		}
	}
	b.local(opts)
}

// batchConsumeRouter is the batch form of the router's RouteConsume;
// the cluster router implements it. Its forwards ask the owner for up
// to max records instead of one, and batch reports that a 200 it wrote
// is already a {"messages":[...]} body rather than a single message for
// the caller to wrap. forwarded and localPartition mean what they mean
// for RouteConsume.
type batchConsumeRouter interface {
	RouteConsumeBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int, max int) (forwarded, batch bool, localPartition *int)
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
		b.write(nil, msgs)
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
	b.write(nil, b.topUp(opts, append(msgs, msg), 1, len(msg.Key)+len(msg.Payload)))
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
		b.write(nil, msgs)
		return
	}

	c := &captureWriter{}
	if forwarded, _ := router.RouteConsumeRemote(ctx, c, b.r, b.topic); forwarded {
		writeCapturedBatch(b.w, c)
		return
	}
	if opts.Wait <= 0 || waiter == nil {
		b.w.WriteHeader(http.StatusNoContent)
		return
	}

	local := &localConsumeWaiter{s: b.s, topic: b.topic, waiter: waiter}
	c = &captureWriter{}
	if router.RouteConsumeWait(ctx, c, b.r, b.topic, opts.Wait, local) {
		if first, ok := c.message(); ok {
			b.write(first, b.topUp(probe, nil, 1, len(first)))
			return
		}
		c.replay(b.w)
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
	b.write(nil, b.topUp(probe, []topic.Message{msg}, 1, len(msg.Key)+len(msg.Payload)))
}

// topUp adds what one more non-blocking local scan finds to the held
// records a wait delivered (msgs, and any already encoded), up to the
// batch size: a wait wakes on the first record of a burst, and the rest
// are usually right behind it. heldBytes is what the held records
// carry, taken off the scan's byte bound. A failure only ends the
// top-up; the records in hand are reserved and go out.
func (b *batchConsume) topUp(opts brokermsg.ConsumeOpts, msgs []topic.Message, held, heldBytes int) []topic.Message {
	room := b.max - held
	if room <= 0 {
		return msgs
	}
	if opts.MaxBytes > 0 {
		if opts.MaxBytes -= heldBytes; opts.MaxBytes <= 0 {
			return msgs
		}
	}
	opts.Wait = 0
	more, _, err := b.bc.ConsumeBatch(b.r.Context(), b.topic, opts, room, msgs)
	if err != nil {
		return msgs
	}
	return more
}

// write answers with first (an already encoded message, may be nil) and
// msgs, up to consumeBatchReplyBytes (see appendMessages). The records
// the bound leaves out are reserved for a response that will not carry
// them, so they are given back before the response goes out: the next
// consume takes them at once rather than when their leases lapse.
func (b *batchConsume) write(first []byte, msgs []topic.Message) {
	body, sent := appendMessages(first, msgs)
	b.release(msgs[sent:])
	writeBatchBody(b.w, body)
}

// release gives back reserved records the response does not carry. It
// runs whether or not the client is still there, so it does not take
// the request's cancellation. A handle that is already stale was
// resolved some other way, which is what giving it back is for.
func (b *batchConsume) release(msgs []topic.Message) {
	if len(msgs) == 0 {
		return
	}
	ctx := context.WithoutCancel(b.r.Context())
	for i := range msgs {
		h, err := consumer.DecodeHandle(msgs[i].ReceiptHandle)
		if err != nil {
			continue
		}
		if err := b.s.Deps.Broker.Nack(ctx, b.topic, h); err != nil && !errors.Is(err, consumer.ErrHandleStale) && b.s.Deps.Logger != nil {
			b.s.Deps.Logger.Warn("release a record left out of a batch consume", "topic", b.topic, "err", err)
		}
	}
}

// appendMessages encodes {"messages":[...]}: first (an already encoded
// message, may be nil) and then msgs, each as a single consume encodes
// it, in order until the next record would take the body past
// consumeBatchReplyBytes. The first record always goes. sent is how many
// of msgs the body carries. The buffer is sized for each key and payload
// base64-encoded, plus the topic, the receipt handle and 192 bytes for
// the field names, both encoding flags and the widest numbers, so a
// batch of binary records is built without growing it; JSON records
// over-reserve by a third, within the reply bound.
func appendMessages(first []byte, msgs []topic.Message) (body []byte, sent int) {
	size := len(first) + 16
	for i := range msgs {
		size += base64.StdEncoding.EncodedLen(len(msgs[i].Key)) + base64.StdEncoding.EncodedLen(len(msgs[i].Payload)) +
			len(msgs[i].Topic) + len(msgs[i].ReceiptHandle) + 192
	}
	body = make([]byte, 0, min(size, consumeBatchReplyBytes))
	body = append(body, `{"messages":[`...)
	body = append(body, first...)
	sent = len(msgs)
	for i := range msgs {
		mark := len(body)
		later := i > 0 || len(first) > 0
		if later {
			body = append(body, ',')
		}
		body = msgs[i].AppendJSON(body)
		if later && len(body)+len("]}\n") > consumeBatchReplyBytes {
			// Encoded, this record would take the body past the bound:
			// it and the rest go back for the next consume.
			body = body[:mark]
			sent = i
			break
		}
	}
	return append(body, "]}\n"...), sent
}

// writeBatchBody answers 200 with a batch body: one appendMessages built,
// or one another node built.
func writeBatchBody(w http.ResponseWriter, body []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// writeCapturedBatch answers a batch consume from what the single-record
// machinery wrote: a delivered record becomes a one-record batch, and
// anything else (204, an error) is passed through unchanged.
func writeCapturedBatch(w http.ResponseWriter, c *captureWriter) {
	if first, ok := c.message(); ok {
		body, _ := appendMessages(first, nil)
		writeBatchBody(w, body)
		return
	}
	c.replay(w)
}
