package node

// EncodeConsumeRequest encodes an OpConsume payload.
func EncodeConsumeRequest(req ConsumeRequest) ([]byte, error) {
	w := opWriter(OpConsume, fieldLen(req.Topic)+4+1+8+1+8+1)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	w.bool(req.HasPartition)
	w.i64(req.Offset)
	w.bool(req.HasOffset)
	w.i64(req.WaitNanos)
	w.bool(req.LocalOnly)
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
		LocalOnly:    localOnly,
	}, nil
}
