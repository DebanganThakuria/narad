package node

// EncodeJoinClusterRequest encodes an OpJoinCluster payload. EntryTypes
// trails Fresh and is written only when non-zero, so a request without
// it is exactly the frame v3.0.1 sends.
func EncodeJoinClusterRequest(req JoinClusterRequest) ([]byte, error) {
	size := fieldLen(req.ID) + fieldLen(req.ClusterAddr) + 1
	if req.EntryTypes != 0 {
		size += 4
	}
	w := opWriter(OpJoinCluster, size)
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	if err := w.string(req.ClusterAddr); err != nil {
		return nil, err
	}
	w.bool(req.Fresh)
	if req.EntryTypes != 0 {
		w.u32(req.EntryTypes)
	}
	return w.finish(), nil
}

// DecodeJoinClusterRequest decodes an OpJoinCluster payload. Fresh and
// EntryTypes are trailing optional fields: a request from a node that
// predates Fresh decodes with Fresh=false, the conservative reading (its
// state is unknown, so a tombstoned ID stays refused), and one that
// predates EntryTypes with 0 (it reports nothing).
func DecodeJoinClusterRequest(payload []byte) (JoinClusterRequest, error) {
	r, err := opReader(payload, OpJoinCluster)
	if err != nil {
		return JoinClusterRequest{}, err
	}
	id, err := r.string()
	if err != nil {
		return JoinClusterRequest{}, err
	}
	clusterAddr, err := r.string()
	if err != nil {
		return JoinClusterRequest{}, err
	}
	req := JoinClusterRequest{ID: id, ClusterAddr: clusterAddr}
	if r.remaining() > 0 {
		if req.Fresh, err = r.bool(); err != nil {
			return JoinClusterRequest{}, err
		}
	}
	if r.remaining() > 0 {
		if req.EntryTypes, err = r.u32(); err != nil {
			return JoinClusterRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return JoinClusterRequest{}, err
	}
	return req, nil
}
