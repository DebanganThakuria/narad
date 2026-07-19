package node

// EncodeCompleteMoveRequest encodes an OpCompleteMove payload.
func EncodeCompleteMoveRequest(req CompleteMoveRequest) ([]byte, error) {
	w := opWriter(OpCompleteMove, fieldLen(req.Topic)+4+fieldLen(req.ExpectedOwner)+fieldLen(req.TargetID))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	if err := w.string(req.ExpectedOwner); err != nil {
		return nil, err
	}
	if err := w.string(req.TargetID); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeCompleteMoveRequest decodes an OpCompleteMove payload.
func DecodeCompleteMoveRequest(payload []byte) (CompleteMoveRequest, error) {
	r, err := opReader(payload, OpCompleteMove)
	if err != nil {
		return CompleteMoveRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return CompleteMoveRequest{}, err
	}
	part, err := r.i32()
	if err != nil {
		return CompleteMoveRequest{}, err
	}
	owner, err := r.string()
	if err != nil {
		return CompleteMoveRequest{}, err
	}
	target, err := r.string()
	if err != nil {
		return CompleteMoveRequest{}, err
	}
	if err := r.done(); err != nil {
		return CompleteMoveRequest{}, err
	}
	return CompleteMoveRequest{Topic: topic, Partition: int(part), ExpectedOwner: owner, TargetID: target}, nil
}

// EncodeAbortMoveRequest encodes an OpAbortMove payload.
func EncodeAbortMoveRequest(req AbortMoveRequest) ([]byte, error) {
	w := opWriter(OpAbortMove, fieldLen(req.Topic)+4+fieldLen(req.ExpectedTarget))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	if err := w.string(req.ExpectedTarget); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeAbortMoveRequest decodes an OpAbortMove payload.
func DecodeAbortMoveRequest(payload []byte) (AbortMoveRequest, error) {
	r, err := opReader(payload, OpAbortMove)
	if err != nil {
		return AbortMoveRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return AbortMoveRequest{}, err
	}
	part, err := r.i32()
	if err != nil {
		return AbortMoveRequest{}, err
	}
	target, err := r.string()
	if err != nil {
		return AbortMoveRequest{}, err
	}
	if err := r.done(); err != nil {
		return AbortMoveRequest{}, err
	}
	return AbortMoveRequest{Topic: topic, Partition: int(part), ExpectedTarget: target}, nil
}
