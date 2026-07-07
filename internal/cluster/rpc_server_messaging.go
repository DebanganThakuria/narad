package cluster

import (
	"errors"
	"net/http"
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
	return jsonResponse(http.StatusOK, map[string]any{
		"offsets": offsets,
		"count":   len(offsets),
	})
}

func (s *RPCServer) handleConsume(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeConsumeRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid consume request: "+err.Error())
	}
	// Clamp the wire-supplied wait: broker.Consume runs under a Background
	// context here (RPC frames carry no caller deadline), so an unclamped
	// wait would park this server for however long the peer asked —
	// defense in depth against peers that skipped the router-side clamp.
	wait := max(time.Duration(req.WaitNanos), 0)
	if wait > defaultMaxConsumeWait {
		wait = defaultMaxConsumeWait
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
	return jsonResponse(http.StatusOK, msg)
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
