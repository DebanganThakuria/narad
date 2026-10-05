package node

// EncodeMemberRequest encodes an OpRegisterMember payload. Build and
// EntryTypes trail the v3.0.1 fields and are written only when either is
// set, so a request without them is exactly the frame v3.0.1 sends.
func EncodeMemberRequest(req MemberRequest) ([]byte, error) {
	size := fieldLen(req.ID) + fieldLen(req.Addr) + fieldLen(req.ClusterAddr) + fieldLen(req.Status) + 8
	withVersion := req.Build != "" || req.EntryTypes != 0
	if withVersion {
		size += fieldLen(req.Build) + 4
	}
	w := opWriter(OpRegisterMember, size)
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
	if withVersion {
		if err := w.string(req.Build); err != nil {
			return nil, err
		}
		w.u32(req.EntryTypes)
	}
	return w.finish(), nil
}

// DecodeMemberRequest decodes an OpRegisterMember payload. A frame from
// a sender that predates Build and EntryTypes decodes with both empty:
// it reports nothing.
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
	req := MemberRequest{
		ID:            id,
		Addr:          addr,
		ClusterAddr:   clusterAddr,
		Status:        status,
		LastHeartbeat: lastHeartbeat,
	}
	if r.remaining() > 0 {
		if req.Build, err = r.string(); err != nil {
			return MemberRequest{}, err
		}
		if req.EntryTypes, err = r.u32(); err != nil {
			return MemberRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return MemberRequest{}, err
	}
	return req, nil
}
