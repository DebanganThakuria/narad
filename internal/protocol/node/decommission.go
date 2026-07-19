package node

// EncodeDecommissionRequest encodes an OpDecommissionMember payload: the
// member ID and whether this cancels (clears) an in-progress drain.
func EncodeDecommissionRequest(req DecommissionRequest) ([]byte, error) {
	w := opWriter(OpDecommissionMember, fieldLen(req.ID)+1)
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	w.bool(req.Cancel)
	return w.finish(), nil
}

// DecodeDecommissionRequest decodes an OpDecommissionMember payload.
func DecodeDecommissionRequest(payload []byte) (DecommissionRequest, error) {
	r, err := opReader(payload, OpDecommissionMember)
	if err != nil {
		return DecommissionRequest{}, err
	}
	id, err := r.string()
	if err != nil {
		return DecommissionRequest{}, err
	}
	cancel, err := r.bool()
	if err != nil {
		return DecommissionRequest{}, err
	}
	if err := r.done(); err != nil {
		return DecommissionRequest{}, err
	}
	return DecommissionRequest{ID: id, Cancel: cancel}, nil
}
