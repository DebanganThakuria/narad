package node

import "fmt"

// EncodeAckRequest encodes an OpAck payload.
func EncodeAckRequest(req AckRequest) ([]byte, error) {
	if int(int32(req.Partition)) != req.Partition {
		return nil, fmt.Errorf("partition out of int32 range: %d", req.Partition)
	}
	w := opWriter(OpAck, fieldLen(req.Topic)+4+8+8)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	w.i64(req.Offset)
	w.i64(req.Nonce)
	return w.finish(), nil
}

// DecodeAckRequest decodes an OpAck payload.
func DecodeAckRequest(payload []byte) (AckRequest, error) {
	r, err := opReader(payload, OpAck)
	if err != nil {
		return AckRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return AckRequest{}, err
	}
	partition, err := r.i32()
	if err != nil {
		return AckRequest{}, err
	}
	offset, err := r.i64()
	if err != nil {
		return AckRequest{}, err
	}
	nonce, err := r.i64()
	if err != nil {
		return AckRequest{}, err
	}
	if err := r.done(); err != nil {
		return AckRequest{}, err
	}
	return AckRequest{
		Topic:     topic,
		Partition: int(partition),
		Offset:    offset,
		Nonce:     nonce,
	}, nil
}
