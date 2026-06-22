package node

func EncodeAckRequest(req AckRequest) ([]byte, error) {
	w := opWriter(OpAck, fieldLen(req.Topic)+fieldLen(req.ReceiptHandle))
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	if err := w.string(req.ReceiptHandle); err != nil {
		return nil, err
	}
	return w.bytesOut(), nil
}

func DecodeAckRequest(payload []byte) (AckRequest, error) {
	r, err := opReader(payload, OpAck)
	if err != nil {
		return AckRequest{}, err
	}
	topicName, err := r.string()
	if err != nil {
		return AckRequest{}, err
	}
	handle, err := r.string()
	if err != nil {
		return AckRequest{}, err
	}
	if err := r.done(); err != nil {
		return AckRequest{}, err
	}
	return AckRequest{Topic: topicName, ReceiptHandle: handle}, nil
}
