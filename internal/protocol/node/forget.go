package node

// EncodeForgetServerRequest encodes an OpForgetServer payload: the Raft
// server ID.
func EncodeForgetServerRequest(req ForgetServerRequest) ([]byte, error) {
	w := opWriter(OpForgetServer, fieldLen(req.ID))
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeForgetServerRequest decodes an OpForgetServer payload.
func DecodeForgetServerRequest(payload []byte) (ForgetServerRequest, error) {
	r, err := opReader(payload, OpForgetServer)
	if err != nil {
		return ForgetServerRequest{}, err
	}
	id, err := r.string()
	if err != nil {
		return ForgetServerRequest{}, err
	}
	if err := r.done(); err != nil {
		return ForgetServerRequest{}, err
	}
	return ForgetServerRequest{ID: id}, nil
}
