package cluster

import (
	"context"
	"net/http"

	"github.com/debanganthakuria/narad/internal/consumer"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleAckBatch applies every record of an OpAckBatch under ONE
// messaging slot and answers each on its own: the reply body carries,
// in request order, the status and message the single-record op would
// have answered, so a stale handle in the batch is a 410 for that record
// and nothing else. Records are applied in order; a batch never holds
// two records of one reservation in practice, and if it did the second
// would see what the first left, exactly as two single acks would.
//
// A batch whose requester gave up while it waited for the slot is
// answered 503 unapplied, as a single ack is (see handleAckFamily).
func (s *RPCServer) handleAckBatch(ctx context.Context, payload []byte) nodewire.Response {
	req, err := nodewire.DecodeAckBatchRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid ack batch request: "+err.Error())
	}
	if !s.acquireMessagingSlot(ctx) {
		return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot")
	}
	// Each record's result is appended to the reply as it is applied, so
	// the common all-success batch costs the body and nothing more.
	body, _ := nodewire.AppendAckBatchReplyHeader(make([]byte, 0, 4+6*len(req.Items)), len(req.Items))
	for i := range req.Items {
		item := &req.Items[i]
		h := consumer.Handle{Partition: item.Partition, Offset: item.Offset, Nonce: item.Nonce}
		var op string
		switch item.Mode {
		case nodewire.AckModeExtend:
			op, err = "extend ack", s.broker.ExtendAck(ctx, item.Topic, h)
		case nodewire.AckModeNack:
			op, err = "nack", s.broker.Nack(ctx, item.Topic, h)
		default:
			op, err = "ack", s.broker.Ack(ctx, item.Topic, h)
		}
		res := nodewire.AckResult{Status: http.StatusNoContent}
		if err != nil {
			res.Status, res.Error = s.brokerErrorStatus(op, err)
		}
		body, err = nodewire.AppendAckResult(body, res)
		if err != nil {
			s.releaseMessagingSlot()
			return errorResponse(http.StatusInternalServerError, "encode ack batch reply failed")
		}
	}
	s.releaseMessagingSlot()
	return nodewire.Response{Status: http.StatusOK, ContentType: "application/octet-stream", Body: body}
}
