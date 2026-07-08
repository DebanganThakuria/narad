package node

// EncodeJoinClusterRequest encodes an OpJoinCluster payload.
func EncodeJoinClusterRequest(req JoinClusterRequest) ([]byte, error) {
	w := opWriter(OpJoinCluster, fieldLen(req.ID)+fieldLen(req.ClusterAddr))
	if err := w.string(req.ID); err != nil {
		return nil, err
	}
	if err := w.string(req.ClusterAddr); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeJoinClusterRequest decodes an OpJoinCluster payload.
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
	if err := r.done(); err != nil {
		return JoinClusterRequest{}, err
	}
	return JoinClusterRequest{ID: id, ClusterAddr: clusterAddr}, nil
}
