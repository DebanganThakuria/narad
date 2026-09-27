package node

import (
	"fmt"
	"math"
)

// EncodeConsumeRequest encodes an OpConsume payload.
func EncodeConsumeRequest(req ConsumeRequest) ([]byte, error) {
	partition, err := partitionField(req.Partition)
	if err != nil {
		return nil, err
	}
	if req.Max < 0 || req.Max > math.MaxInt32 {
		return nil, fmt.Errorf("consume max out of range: %d", req.Max)
	}
	w := opWriter(OpConsume, fieldLen(req.Topic)+4+1+8+1+8+1+1+4)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(partition)
	w.bool(req.HasPartition)
	w.i64(req.Offset)
	w.bool(req.HasOffset)
	w.i64(req.WaitNanos)
	w.bool(req.LocalOnly)
	batch := req.Max > 1
	if req.Claim || batch {
		// Written only when set (or when Max follows it), so an owner on
		// an older release still decodes every probe we send it. It
		// refuses a flagged claim with 400; the requester then falls back
		// to a plain probe for that owner (cluster.Router.claimFrom).
		w.bool(req.Claim)
	}
	if batch {
		// The same rule for Max, one field further on: an owner that
		// predates it refuses the request with 400 (trailing payload), and
		// the requester asks it for one record instead.
		w.i32(int32(req.Max))
	}
	return w.finish(), nil
}

// DecodeConsumeRequest decodes an OpConsume payload.
func DecodeConsumeRequest(payload []byte) (ConsumeRequest, error) {
	r, err := opReader(payload, OpConsume)
	if err != nil {
		return ConsumeRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return ConsumeRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return ConsumeRequest{}, err
	}
	hasPartition, err := r.bool()
	if err != nil {
		return ConsumeRequest{}, err
	}
	offset, err := r.i64()
	if err != nil {
		return ConsumeRequest{}, err
	}
	hasOffset, err := r.bool()
	if err != nil {
		return ConsumeRequest{}, err
	}
	waitNanos, err := r.i64()
	if err != nil {
		return ConsumeRequest{}, err
	}
	localOnly, err := r.bool()
	if err != nil {
		return ConsumeRequest{}, err
	}
	claim := false
	if r.pos < len(r.payload) {
		// Optional trailing field; absent from older peers' requests.
		if claim, err = r.bool(); err != nil {
			return ConsumeRequest{}, err
		}
	}
	batchMax := 0
	if r.pos < len(r.payload) {
		// The second optional trailing field, present only in a batch.
		v, err := r.i32()
		if err != nil {
			return ConsumeRequest{}, err
		}
		if v < 0 {
			return ConsumeRequest{}, fmt.Errorf("consume max out of range: %d", v)
		}
		if v > 1 {
			// 0 and 1 both ask for one record; decoding them alike keeps
			// the encoding canonical.
			batchMax = int(v)
		}
	}
	if err := r.done(); err != nil {
		return ConsumeRequest{}, err
	}
	return ConsumeRequest{
		Topic:        topic,
		Partition:    int(partition),
		HasPartition: hasPartition,
		Offset:       offset,
		HasOffset:    hasOffset,
		WaitNanos:    waitNanos,
		Claim:        claim,
		LocalOnly:    localOnly,
		Max:          batchMax,
	}, nil
}
