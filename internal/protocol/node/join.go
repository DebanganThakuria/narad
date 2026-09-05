package node

// EncodeJoinClusterRequest encodes an OpJoinCluster payload.
func EncodeJoinClusterRequest(req JoinClusterRequest) ([]byte, error) {
	w := opWriter(OpJoinCluster, fieldLen(req.ID)+fieldLen(req.ClusterAddr)+1)
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	if err := w.string(req.ClusterAddr); err != nil {
		return nil, err
	}
	w.bool(req.Fresh)
	return w.finish(), nil
}

// DecodeJoinClusterRequest decodes an OpJoinCluster payload. The Fresh
// flag is a trailing optional field: a request from a node that
// predates it decodes with Fresh=false, the conservative reading (its
// state is unknown, so a tombstoned ID stays refused).
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
	fresh := false
	if r.remaining() > 0 {
		if fresh, err = r.bool(); err != nil {
			return JoinClusterRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return JoinClusterRequest{}, err
	}
	return JoinClusterRequest{ID: id, ClusterAddr: clusterAddr, Fresh: fresh}, nil
}
