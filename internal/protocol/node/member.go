package node

// EncodeMemberRequest encodes an OpRegisterMember payload.
func EncodeMemberRequest(req MemberRequest) ([]byte, error) {
	w := opWriter(OpRegisterMember, fieldLen(req.ID)+fieldLen(req.Addr)+fieldLen(req.ClusterAddr)+fieldLen(req.Status)+8)
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	if err := w.string(req.Addr); err != nil {
		return nil, err
	}
	if err := w.string(req.ClusterAddr); err != nil {
		return nil, err
	}
	if err := w.string(req.Status); err != nil {
		return nil, err
	}
	w.i64(req.LastHeartbeat)
	return w.finish(), nil
}

// DecodeMemberRequest decodes an OpRegisterMember payload.
func DecodeMemberRequest(payload []byte) (MemberRequest, error) {
	r, err := opReader(payload, OpRegisterMember)
	if err != nil {
		return MemberRequest{}, err
	}
	id, err := r.string()
	if err != nil {
		return MemberRequest{}, err
	}
	addr, err := r.string()
	if err != nil {
		return MemberRequest{}, err
	}
	clusterAddr, err := r.string()
	if err != nil {
		return MemberRequest{}, err
	}
	status, err := r.string()
	if err != nil {
		return MemberRequest{}, err
	}
	lastHeartbeat, err := r.i64()
	if err != nil {
		return MemberRequest{}, err
	}
	if err := r.done(); err != nil {
		return MemberRequest{}, err
	}
	return MemberRequest{
		ID:            id,
		Addr:          addr,
		ClusterAddr:   clusterAddr,
		Status:        status,
		LastHeartbeat: lastHeartbeat,
	}, nil
}
