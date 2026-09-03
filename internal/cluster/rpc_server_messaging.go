package cluster

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (s *RPCServer) handleProduce(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid produce request: "+err.Error())
	}
	if len(req.Payload) == 0 {
		return errorResponse(http.StatusBadRequest, "message required")
	}
	offset, partition, err := s.broker.Produce(rpcRequestContext(), req.Topic, req.Key, req.Payload, req.Partition)
	if err != nil {
		return s.brokerError("produce", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": partition,
	})
}

func (s *RPCServer) handleCommitProduce(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce request: "+err.Error())
	}
	offset, err := s.broker.CommitAcceptedProduce(rpcRequestContext(), ingress.ProduceRecord{
		Topic:           req.Topic,
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

func (s *RPCServer) handleCommitProduceBatch(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceBatchRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce batch request: "+err.Error())
	}
	records := make([]ingress.ProduceRecord, 0, len(req.Records))
	for _, record := range req.Records {
		records = append(records, ingress.ProduceRecord{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	offsets, err := s.broker.CommitAcceptedProduceBatch(rpcRequestContext(), records)
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

func (s *RPCServer) handleConsume(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeConsumeRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid consume request: "+err.Error())
	}
	// Clamp the wire-supplied wait: broker.Consume runs under a Background
	// context here (RPC frames carry no caller deadline), so an unclamped
	// wait would park this server for however long the peer asked;
	// defense in depth against peers that skipped the router-side clamp.
	wait := max(time.Duration(req.WaitNanos), 0)
	if ceiling := s.consumeWaitCeiling(); wait > ceiling {
		wait = ceiling
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
	consume := func() nodewire.Response {
		msg, found, err := s.broker.Consume(rpcRequestContext(), req.Topic, opts)
		if errors.Is(err, brokermsg.ErrNotPartitionOwner) && req.LocalOnly {
			return nodewire.Response{Status: http.StatusNoContent}
		}
		if err != nil {
			return s.brokerError("consume", err)
		}
		if !found {
			return nodewire.Response{Status: http.StatusNoContent}
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
	if wait > 0 {
		// A long-poll parks inside the broker for up to the wait ceiling
		// while consuming no CPU. Holding a messaging slot for that long
		// would let a handful of forwarded long-polls starve produce
		// commits and acks, so only non-blocking scans are gated.
		return consume()
	}
	return s.withMessagingSlot(consume)
}

func (s *RPCServer) handleAck(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeAckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid ack request: "+err.Error())
	}
	if err := s.broker.Ack(rpcRequestContext(), req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("ack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleExtendAck(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeExtendAckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid extend ack request: "+err.Error())
	}
	if err := s.broker.ExtendAck(rpcRequestContext(), req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("extend ack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleNack(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeNackRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid nack request: "+err.Error())
	}
	if err := s.broker.Nack(rpcRequestContext(), req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("nack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}
