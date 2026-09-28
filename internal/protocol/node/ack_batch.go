package node

import (
	"fmt"
	"math"
)

// MaxAckBatch bounds how many records one OpAckBatch may carry, so a
// peer cannot make this node allocate an unbounded slice by lying about
// the count. Senders stay far below it: the HTTP batch ack takes at most
// a hundred handles and the ack coalescer sends at most a few dozen.
const MaxAckBatch = 1024

// AckMode is what an ack-shaped record asks of its reservation.
type AckMode uint8

// Ack modes. Values are stable on the wire.
const (
	// AckModeAck commits the record, as OpAck does.
	AckModeAck AckMode = iota
	// AckModeExtend renews the visibility window, as OpExtendAck does.
	AckModeExtend
	// AckModeNack releases the reservation now, as OpNack does.
	AckModeNack
)

// AckBatchItem is one record of an OpAckBatch: the coordinates an
// AckRequest carries plus what to do with them.
type AckBatchItem struct {
	Topic     string
	Partition int
	Offset    int64
	Nonce     int64
	Mode      AckMode
}

// AckBatchRequest carries several ack-shaped records bound for one
// owner in a single RPC. The records are independent: the owner applies
// each on its own and answers each on its own (see AckResult), so one
// stale handle never fails the rest.
type AckBatchRequest struct {
	Items []AckBatchItem
}

// AckResult is the owner's answer for one record of an OpAckBatch: the
// status the single-record op would have answered (204, 410, 400, 421,
// ...) and, for a failure, the message its error body would have carried.
type AckResult struct {
	Status int
	Error  string
}

// ackBatchItemMin is the smallest encoding of one item: mode, an empty
// topic's length prefix, partition, offset and nonce.
const ackBatchItemMin = 1 + 4 + 4 + 8 + 8

// EncodeAckBatchRequest encodes an OpAckBatch payload.
func EncodeAckBatchRequest(req AckBatchRequest) ([]byte, error) {
	if len(req.Items) > MaxAckBatch {
		return nil, fmt.Errorf("ack batch too large: %d records (max %d)", len(req.Items), MaxAckBatch)
	}
	size := 4
	for i := range req.Items {
		size += ackBatchItemMin + len(req.Items[i].Topic)
	}
	w := opWriter(OpAckBatch, size)
	w.i32(int32(len(req.Items)))
	for i := range req.Items {
		item := &req.Items[i]
		if item.Mode > AckModeNack {
			return nil, fmt.Errorf("invalid ack mode %d", item.Mode)
		}
		partition, err := partitionField(item.Partition)
		if err != nil {
			return nil, err
		}
		w.u8(uint8(item.Mode))
		if err := w.string(item.Topic); err != nil {
			return nil, err
		}
		w.i32(partition)
		w.i64(item.Offset)
		w.i64(item.Nonce)
	}
	return w.finish(), nil
}

// DecodeAckBatchRequest decodes an OpAckBatch payload. Consecutive
// records for the same topic share one decoded string, so a batch for a
// single topic allocates the topic once rather than once per record.
func DecodeAckBatchRequest(payload []byte) (AckBatchRequest, error) {
	r, err := opReader(payload, OpAckBatch)
	if err != nil {
		return AckBatchRequest{}, err
	}
	count, err := r.i32()
	if err != nil {
		return AckBatchRequest{}, err
	}
	if count < 0 || count > MaxAckBatch || int(count) > r.remaining()/ackBatchItemMin {
		return AckBatchRequest{}, fmt.Errorf("invalid ack batch count %d", count)
	}
	req := AckBatchRequest{Items: make([]AckBatchItem, count)}
	var topic string
	for i := range req.Items {
		mode, err := r.u8()
		if err != nil {
			return AckBatchRequest{}, err
		}
		if AckMode(mode) > AckModeNack {
			return AckBatchRequest{}, fmt.Errorf("invalid ack mode %d", mode)
		}
		name, err := r.bytes()
		if err != nil {
			return AckBatchRequest{}, err
		}
		if i == 0 || string(name) != topic {
			topic = string(name)
		}
		partition, err := r.i32()
		if err != nil {
			return AckBatchRequest{}, err
		}
		offset, err := r.i64()
		if err != nil {
			return AckBatchRequest{}, err
		}
		nonce, err := r.i64()
		if err != nil {
			return AckBatchRequest{}, err
		}
		req.Items[i] = AckBatchItem{Topic: topic, Partition: int(partition), Offset: offset, Nonce: nonce, Mode: AckMode(mode)}
	}
	if err := r.done(); err != nil {
		return AckBatchRequest{}, err
	}
	return req, nil
}

// AppendAckBatchReply appends the body of an OpAckBatch reply to dst:
// the count, then each record's status and error message in request
// order. A success carries an empty message, so the common reply is six
// bytes a record.
func AppendAckBatchReply(dst []byte, results []AckResult) ([]byte, error) {
	out, err := AppendAckBatchReplyHeader(dst, len(results))
	if err != nil {
		return dst, err
	}
	for _, res := range results {
		if out, err = AppendAckResult(out, res); err != nil {
			return dst, err
		}
	}
	return out, nil
}

// AppendAckBatchReplyHeader appends the record count that opens an
// OpAckBatch reply. The n results follow, each appended in request
// order with AppendAckResult; together they encode exactly what
// AppendAckBatchReply does, without a results slice.
func AppendAckBatchReplyHeader(dst []byte, n int) ([]byte, error) {
	if n < 0 || n > MaxAckBatch {
		return dst, fmt.Errorf("ack batch reply too large: %d records (max %d)", n, MaxAckBatch)
	}
	w := writer{buf: dst}
	w.i32(int32(n))
	return w.finish(), nil
}

// AppendAckResult appends one record's result to an OpAckBatch reply.
func AppendAckResult(dst []byte, res AckResult) ([]byte, error) {
	if res.Status < 0 || res.Status > math.MaxUint16 {
		return dst, fmt.Errorf("invalid ack status %d", res.Status)
	}
	w := writer{buf: dst}
	w.u16(uint16(res.Status))
	if err := w.string(res.Error); err != nil {
		return dst, err
	}
	return w.finish(), nil
}

// DecodeAckBatchReply decodes an OpAckBatch reply body into dst's
// storage (dst may be nil) and returns the results.
func DecodeAckBatchReply(body []byte, dst []AckResult) ([]AckResult, error) {
	r := reader{payload: body}
	count, err := r.i32()
	if err != nil {
		return nil, err
	}
	if count < 0 || count > MaxAckBatch || int(count) > r.remaining()/6 {
		return nil, fmt.Errorf("invalid ack batch reply count %d", count)
	}
	out := dst[:0]
	for range count {
		status, err := r.u16()
		if err != nil {
			return nil, err
		}
		msg, err := r.string()
		if err != nil {
			return nil, err
		}
		out = append(out, AckResult{Status: int(status), Error: msg})
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return out, nil
}
