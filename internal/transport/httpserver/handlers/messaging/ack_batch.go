package messaging

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// maxAckBatchBodyBytes bounds a batch ack body: MaxConsumeBatch handles
// of a few dozen bytes each, with room to spare for whitespace.
const maxAckBatchBodyBytes = 64 << 10

// tooManyReceiptHandles answers a batch ack past MaxConsumeBatch
// handles. The decode stops at the first handle past the bound, so the
// answer does not count the rest.
var tooManyReceiptHandles = "too many receipt_handles: more than " + strconv.Itoa(MaxConsumeBatch) + " (max " + strconv.Itoa(MaxConsumeBatch) + ")"

// batchAckRouter is the batch form of the router's RouteAck family; the
// cluster router implements it. It settles the handles whose partitions
// other nodes own, one RPC per owner, and fills in statuses[i] and
// msgs[i] with the status and error message a single ack of handles[i]
// would have answered. It skips entries whose status is already set and
// leaves the handles this node owns at 0 for the caller. op is "ack",
// "extend" or "nack".
type batchAckRouter interface {
	RouteAckBatch(ctx context.Context, topicName, op string, handles []consumer.Handle, statuses []int, msgs []string)
}

// ackOpNames are the op names batchAckRouter takes, by ackMode.
var ackOpNames = [...]string{ackCommit: "ack", ackExtend: "extend", ackNack: "nack"}

// ackBatch serves POST /ack with a JSON body {"receipt_handles":[...]}
// and no receipt_handle parameter: every handle is settled on its own,
// with the mode the extend parameter selects for all of them, and the
// reply is 200 with one result per handle in request order,
// {"results":[{"status":204},{"status":410,"error":"..."}]}. Each
// status is the one a single ack of that handle would have answered, so
// one stale or malformed handle never fails the rest. Handles owned by
// other nodes are forwarded with one RPC per owner.
func ackBatch(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, mode ackMode) {
	body, ok := s.ReadBody(w, r, maxAckBatchBodyBytes)
	if !ok {
		return
	}
	// Decoded one handle at a time and refused at handle
	// MaxConsumeBatch+1, so a body of anything but handles costs no more
	// than a full batch (see decodeBoundedList).
	raws, err := decodeBoundedList[string](body, "receipt_handles", MaxConsumeBatch)
	if errors.Is(err, errListTooLong) {
		s.WriteError(w, http.StatusBadRequest, tooManyReceiptHandles)
		return
	}
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	n := len(raws)
	if n == 0 {
		s.WriteError(w, http.StatusBadRequest, "receipt_handles required")
		return
	}

	ctx := r.Context()
	handles := make([]consumer.Handle, n)
	statuses := make([]int, n)
	msgs := make([]string, n)
	for i, raw := range raws {
		h, err := consumer.DecodeHandle(raw)
		if err != nil {
			statuses[i], msgs[i] = brokerOutcome(s, "ack", err)
			continue
		}
		handles[i] = h
	}

	if router := s.Deps.Router; router != nil {
		if br, ok := router.(batchAckRouter); ok {
			br.RouteAckBatch(ctx, topicName, ackOpNames[mode], handles, statuses, msgs)
		} else {
			// A router without the batch form: route each handle as a
			// single ack would be.
			for i := range handles {
				if statuses[i] != 0 {
					continue
				}
				c := &captureWriter{}
				if routeAckOne(ctx, router, c, r, topicName, handles[i], mode) {
					statuses[i], msgs[i] = c.outcome()
				}
			}
		}
	}

	for i := range handles {
		if statuses[i] != 0 {
			continue
		}
		var op string
		var err error
		switch mode {
		case ackExtend:
			op, err = "extend ack", s.Deps.Broker.ExtendAck(ctx, topicName, handles[i])
		case ackNack:
			op, err = "nack", s.Deps.Broker.Nack(ctx, topicName, handles[i])
		default:
			op, err = "ack", s.Deps.Broker.Ack(ctx, topicName, handles[i])
		}
		if err != nil {
			statuses[i], msgs[i] = brokerOutcome(s, op, err)
			continue
		}
		statuses[i] = http.StatusNoContent
	}
	writeAckResults(w, statuses, msgs)
}

// routeAckOne is the single-ack routing of the Ack handler.
func routeAckOne(ctx context.Context, router handlers.Router, w http.ResponseWriter, r *http.Request, topicName string, h consumer.Handle, mode ackMode) bool {
	switch mode {
	case ackExtend:
		return router.RouteExtendAck(ctx, w, r, topicName, h)
	case ackNack:
		return router.RouteNack(ctx, w, r, topicName, h)
	default:
		return router.RouteAck(ctx, w, r, topicName, h)
	}
}

// brokerOutcome is the status and message WriteBrokerError answers err
// with (and it logs a 5xx just the same).
func brokerOutcome(s *handlers.Set, op string, err error) (int, string) {
	c := &captureWriter{}
	s.WriteBrokerError(c, op, err)
	return c.outcome()
}

// writeAckResults answers a batch ack: 200 with one {"status":N} (and
// "error" for a failure) per handle, in request order.
func writeAckResults(w http.ResponseWriter, statuses []int, msgs []string) {
	body := make([]byte, 0, 16+len(statuses)*16)
	body = append(body, `{"results":[`...)
	for i, status := range statuses {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, `{"status":`...)
		body = strconv.AppendInt(body, int64(status), 10)
		if msgs[i] != "" {
			body = append(body, `,"error":`...)
			body = topic.AppendJSONQuoted(body, msgs[i])
		}
		body = append(body, '}')
	}
	body = append(body, "]}\n"...)
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
