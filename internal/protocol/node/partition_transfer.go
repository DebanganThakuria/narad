package node

// Codecs for the partition-transfer RPCs (OpListPartitionSegments,
// OpFetchSegmentChunk) used by rebalance/decommission to copy a
// partition's segments verbatim to a new owner.

// EncodePartitionSegmentsRequest encodes an OpListPartitionSegments payload.
func EncodePartitionSegmentsRequest(req PartitionSegmentsRequest) ([]byte, error) {
	w := opWriter(OpListPartitionSegments, fieldLen(req.Topic)+4)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	return w.finish(), nil
}

// DecodePartitionSegmentsRequest decodes an OpListPartitionSegments payload.
func DecodePartitionSegmentsRequest(payload []byte) (PartitionSegmentsRequest, error) {
	r, err := opReader(payload, OpListPartitionSegments)
	if err != nil {
		return PartitionSegmentsRequest{}, err
	}
	t, err := r.string()
	if err != nil {
		return PartitionSegmentsRequest{}, err
	}
	p, err := r.i32()
	if err != nil {
		return PartitionSegmentsRequest{}, err
	}
	if err := r.done(); err != nil {
		return PartitionSegmentsRequest{}, err
	}
	return PartitionSegmentsRequest{Topic: t, Partition: int(p)}, nil
}

// EncodeFetchSegmentChunkRequest encodes an OpFetchSegmentChunk payload.
func EncodeFetchSegmentChunkRequest(req FetchSegmentChunkRequest) ([]byte, error) {
	w := opWriter(OpFetchSegmentChunk, fieldLen(req.Topic)+4+8+8+8)
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(int32(req.Partition))
	w.i64(req.BaseOffset)
	w.i64(req.At)
	w.i64(req.Length)
	return w.finish(), nil
}

// DecodeFetchSegmentChunkRequest decodes an OpFetchSegmentChunk payload.
func DecodeFetchSegmentChunkRequest(payload []byte) (FetchSegmentChunkRequest, error) {
	r, err := opReader(payload, OpFetchSegmentChunk)
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	t, err := r.string()
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	p, err := r.i32()
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	base, err := r.i64()
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	at, err := r.i64()
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	length, err := r.i64()
	if err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	if err := r.done(); err != nil {
		return FetchSegmentChunkRequest{}, err
	}
	return FetchSegmentChunkRequest{Topic: t, Partition: int(p), BaseOffset: base, At: at, Length: length}, nil
}
