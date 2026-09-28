package cluster

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (s *RPCServer) handleProduce(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid produce request: "+err.Error())
	}
	if len(req.Payload) == 0 {
		return errorResponse(http.StatusBadRequest, "message required")
	}
	offset, partition, err := s.broker.Produce(ctx, req.Topic, req.Key, req.Payload, req.Partition)
	if err != nil {
		return s.brokerError("produce", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": partition,
	})
}

// handleCommitProduce commits one accepted record. No release's
// dispatcher sends this op (they commit through the batch op); it stays
// so a peer that does is still served, and it hands the owner the
// record's incarnation as the batch op does.
func (s *RPCServer) handleCommitProduce(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce request: "+err.Error())
	}
	offset, err := s.broker.CommitAcceptedProduce(ctx, ingress.ProduceRecord{
		Topic:           req.Topic,
		TopicID:         req.TopicID,
		Key:             req.Key,
		TargetPartition: req.TargetPartition,
		Payload:         req.Payload,
		CreatedAtUnixMs: req.CreatedAtUnixMs,
	})
	if err != nil {
		return s.brokerError("commit produce", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": req.TargetPartition,
	})
}

func (s *RPCServer) handleCommitProduceBatch(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceBatchRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce batch request: "+err.Error())
	}
	records := make([]ingress.ProduceRecord, 0, len(req.Records))
	for _, record := range req.Records {
		records = append(records, ingress.ProduceRecord{
			Topic:           record.Topic,
			TopicID:         record.TopicID,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	offsets, err := s.broker.CommitAcceptedProduceBatch(ctx, records)
	if err != nil {
		return s.brokerError("commit produce batch", err)
	}
	return commitBatchResponse(offsets)
}

// commitBatchResponse is the commit_produce_batch reply body. Callers
// (the produce dispatcher and the fan-out runner) act on the status only,
// so the body is a small summary rather than every committed offset:
// {"count":N,"first_offset":F}, with first_offset present when N > 0.
// (A durable_next field would need the partition high watermark, which
// the Broker surface does not expose cheaply; it is omitted.)
func commitBatchResponse(offsets []int64) nodewire.Response {
	body := make([]byte, 0, 48)
	body = append(body, `{"count":`...)
	body = strconv.AppendInt(body, int64(len(offsets)), 10)
	if len(offsets) > 0 {
		body = append(body, `,"first_offset":`...)
		body = strconv.AppendInt(body, offsets[0], 10)
	}
	body = append(body, '}', '\n')
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}
}

func (s *RPCServer) handleConsume(ctx context.Context, key requestKey, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeConsumeRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid consume request: "+err.Error())
	}
	// Clamp the wire-supplied wait: the request context ends when the
	// client cancels or its stream dies, but a peer that skipped the
	// router-side clamp must still not park this server for however long
	// it asked; defense in depth.
	wait := max(time.Duration(req.WaitNanos), 0)
	if ceiling := s.consumeWaitCeiling(); wait > ceiling {
		wait = ceiling
	}
	if wait > 0 {
		// A long-poll parks inside the broker for up to the wait ceiling
		// while consuming no CPU. Holding a messaging slot for that long
		// would let a handful of forwarded long-polls starve acks and
		// probes, so only non-blocking scans are gated.
		return s.consume(ctx, key, &req, wait)
	}
	// A non-blocking consume whose requester already gave up (its probe
	// or claim budget ran out, or its client left), typically while it
	// waited for a messaging slot, is answered without reserving a
	// record for nobody, reading it and releasing it again.
	if !s.acquireMessagingSlot(ctx) {
		return s.abandonConsume(&req)
	}
	defer s.releaseMessagingSlot()
	if ctx.Err() != nil {
		return s.abandonConsume(&req)
	}
	return s.consume(ctx, key, &req, wait)
}

// abandonConsume answers a non-blocking consume whose requester gave up
// before it ran. A claim still retires the hold its notification put on
// a record, so the record goes to somebody else now rather than at the
// hold's deadline; that is what reserving and releasing it did before.
func (s *RPCServer) abandonConsume(req *nodewire.ConsumeRequest) nodewire.Response {
	if req.Claim && req.LocalOnly {
		s.broker.NoteRemoteClaim(req.Topic)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// consume runs a decoded consume against the broker, wait already
// clamped.
func (s *RPCServer) consume(ctx context.Context, key requestKey, req *nodewire.ConsumeRequest, wait time.Duration) nodewire.Response {
	if req.Max > 1 && !req.HasOffset {
		return s.consumeBatch(ctx, key, req, wait)
	}
	opts := brokermsg.ConsumeOpts{Wait: wait}
	if req.HasPartition {
		partition := req.Partition
		opts.Partition = &partition
	}
	if req.HasOffset {
		offset := req.Offset
		opts.Offset = &offset
	}
	msg, found, err := s.broker.Consume(ctx, req.Topic, opts)
	if req.Claim && req.LocalOnly && wait == 0 && err == nil && found {
		// A claim that WON its record resolves the notification it
		// answers, so the owner releases that hold now rather than at
		// the claim deadline. An empty claim keeps the hold to its
		// deadline on purpose: the pump's free-record estimate can be
		// wrong (a partition at its in-flight cap, or paused for a
		// handoff), and the deadline is the backoff that keeps a wrong
		// estimate from becoming a notify/claim loop at RPC speed. A
		// plain probe is not a claim and never releases a hold.
		s.broker.NoteRemoteClaim(req.Topic)
	}
	if errors.Is(err, brokermsg.ErrNotPartitionOwner) && req.LocalOnly {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	if err != nil {
		if ctx.Err() != nil {
			return nodewire.Response{Status: http.StatusNoContent}
		}
		return s.brokerError("consume", err)
	}
	if !found {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	// The message is reserved for a client that may already be gone.
	// Remember the handle first, then check the request context: if
	// the client cancelled, give the message back right away instead
	// of leaving it invisible until its lease expires. A cancel that
	// lands after the reply is written is handled by HandleStreamCancel
	// through the same record.
	if h, herr := consumer.DecodeHandle(msg.ReceiptHandle); herr == nil {
		s.rememberDelivery(key, req.Topic, h)
		if ctx.Err() != nil {
			if d, ok := s.takeDelivery(key); ok {
				s.releaseDelivery(d)
			}
			return nodewire.Response{Status: http.StatusNoContent}
		}
	}
	// Encode the message the same way the local HTTP path does: one
	// append-style pass with the payload embedded verbatim. Routing it
	// through json.Marshal re-validated and compacted the payload (so
	// a forwarded consume returned different bytes than a local one)
	// and copied the body three times.
	body := msg.AppendJSON(make([]byte, 0, len(msg.Payload)+128))
	body = append(body, '\n')
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}
}

// maxForwardedConsumeBatch caps the records one forwarded batch consume
// takes, whatever the requester asked for. It is the HTTP layer's own
// cap on ?max= (messaging.MaxConsumeBatch there), restated because this
// package sits below the transport.
const maxForwardedConsumeBatch = 100

// forwardedConsumeBatchBytes bounds the key and payload bytes a
// forwarded batch reserves (ConsumeOpts.MaxBytes). It keeps the reserving
// in step with what the reply can carry for the usual records: JSON goes
// out as it is and binary grows by a third, so their reply stays under
// forwardedConsumeReplyBytes. It does not bound the reply itself: text
// that is not JSON goes out as a JSON string, where a control byte or one
// of < > & takes six bytes, and keys are escaped the same way.
const forwardedConsumeBatchBytes = 4 << 20

// forwardedConsumeReplyBytes bounds the encoded records of a forwarded
// batch's reply, which must fit one cluster RPC frame
// (clusterwire.MaxStreamFramePayloadBytes). A reply over the frame
// cannot be written: the stream aborts, the requester sees an error and
// moves on, and every record in the reply stays reserved until its lease
// lapses. The first record always goes, as a single consume's reply
// would carry it; half the frame, as for a remote produce commit, leaves
// the rest for its framing.
const forwardedConsumeReplyBytes = clusterwire.MaxStreamFramePayloadBytes / 2

// consumeBatch answers a consume that asked for up to req.Max records: a
// batch consume forwarded by a node that owns none of the topic's
// partitions (or not the pinned one). It reserves up to that many in one
// non-blocking scan; with none there and a wait it parks as a single
// consume does and tops up the record the wait delivers with another
// scan. The reply is 200 {"messages":[...]}, each record encoded as a
// single consume's reply encodes it, or 204 with none; failures are
// answered as for a single consume.
//
// The reply carries the records in the order taken until the next one
// would take it past forwardedConsumeReplyBytes. Those left out are
// given back at once (a nack each), so the next consume takes them. In
// practice only records whose escaping makes them several times their
// size are left out: the reserving stops near forwardedConsumeBatchBytes
// of raw bytes, which JSON and binary records keep well inside the bound.
//
// Every record the reply carries is remembered for a cancel that races
// it (rememberDeliveries), so such a cancel gives them all back at once,
// as does one that arrives before the reply is written.
func (s *RPCServer) consumeBatch(ctx context.Context, key requestKey, req *nodewire.ConsumeRequest, wait time.Duration) nodewire.Response {
	bc, ok := s.broker.(broker.BatchConsumer)
	if !ok {
		// A broker without the batch surface serves one record, answered
		// in the batch shape the requester asked for.
		single := *req
		single.Max = 0
		res := s.consume(ctx, key, &single, wait)
		if res.Status != http.StatusOK {
			return res
		}
		body := make([]byte, 0, len(res.Body)+16)
		body = append(body, `{"messages":[`...)
		body = append(body, bytes.TrimRight(res.Body, "\n")...)
		body = append(body, "]}\n"...)
		return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}
	}

	opts := brokermsg.ConsumeOpts{MaxBytes: forwardedConsumeBatchBytes}
	if req.HasPartition {
		partition := req.Partition
		opts.Partition = &partition
	}
	max := min(req.Max, maxForwardedConsumeBatch)
	msgs, waiter, err := bc.ConsumeBatch(ctx, req.Topic, opts, max, nil)
	if err == nil && len(msgs) == 0 && wait > 0 && waiter != nil {
		var msg topic.Message
		var found bool
		msg, found, _, err = s.broker.ConsumeWait(ctx, waiter, wait, nil)
		if err == nil && found {
			msgs = append(msgs, msg)
			// The wait wakes on the first record of a burst; the rest are
			// usually right behind it. A failed top-up only ends the batch.
			top := opts
			top.MaxBytes -= len(msg.Key) + len(msg.Payload)
			if max > 1 && top.MaxBytes > 0 {
				if more, _, terr := bc.ConsumeBatch(ctx, req.Topic, top, max-1, msgs); terr == nil {
					msgs = more
				}
			}
		}
	}
	if req.Claim && req.LocalOnly && wait == 0 && err == nil && len(msgs) > 0 {
		// A claim that won records resolves its notification, as a single
		// claim does (see consume).
		s.broker.NoteRemoteClaim(req.Topic)
	}
	if errors.Is(err, brokermsg.ErrNotPartitionOwner) && req.LocalOnly {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	if err != nil {
		if ctx.Err() != nil {
			return nodewire.Response{Status: http.StatusNoContent}
		}
		return s.brokerError("consume", err)
	}
	if len(msgs) == 0 {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	size := 16
	for i := range msgs {
		size += len(msgs[i].Key) + len(msgs[i].Payload) + 160
	}
	body := make([]byte, 0, min(size, forwardedConsumeReplyBytes))
	body = append(body, `{"messages":[`...)
	sent := len(msgs)
	for i := range msgs {
		mark := len(body)
		if i > 0 {
			body = append(body, ',')
		}
		body = msgs[i].AppendJSON(body)
		if i > 0 && len(body)+len("]}\n") > forwardedConsumeReplyBytes {
			// Encoded, this record would take the reply past what one
			// frame carries: it and the rest go back for the next consume.
			body = body[:mark]
			sent = i
			break
		}
	}
	for _, m := range msgs[sent:] {
		if h, herr := consumer.DecodeHandle(m.ReceiptHandle); herr == nil {
			s.releaseHandle(req.Topic, h)
		}
	}
	// The records are reserved for a requester that may already be gone.
	// Remember them all first, then check the request context, as a
	// single consume does: a cancel that lands after the check finds the
	// record (HandleStreamCancel).
	if h, herr := consumer.DecodeHandle(msgs[0].ReceiptHandle); herr == nil {
		var rest []string
		if sent > 1 {
			rest = make([]string, sent-1)
			for i := range rest {
				rest[i] = msgs[i+1].ReceiptHandle
			}
		}
		s.rememberDeliveries(key, req.Topic, h, rest)
	}
	if ctx.Err() != nil {
		// The requester is gone: give every record back now rather than
		// leave them hidden until their leases lapse.
		if d, ok := s.takeDelivery(key); ok {
			s.releaseDelivery(d)
		}
		return nodewire.Response{Status: http.StatusNoContent}
	}
	body = append(body, "]}\n"...)
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}
}

func (s *RPCServer) handleAck(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeAckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid ack request: "+err.Error())
	}
	if err := s.broker.Ack(ctx, req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("ack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleExtendAck(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeExtendAckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid extend ack request: "+err.Error())
	}
	if err := s.broker.ExtendAck(ctx, req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("extend ack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleNack(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeNackRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid nack request: "+err.Error())
	}
	if err := s.broker.Nack(ctx, req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("nack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}
