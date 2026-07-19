package cluster

// Serve-side handlers for the partition-transfer RPCs: a node that owns
// a partition answers a destination's list/fetch requests during a
// rebalance copy. Read-only — no ownership or state change here.

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (s *RPCServer) handleListPartitionSegments(payload []byte) nodewire.Response {
	req, err := nodewire.DecodePartitionSegmentsRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid partition-segments request: "+err.Error())
	}
	info, err := s.broker.PartitionTransferInfo(rpcRequestContext(), req.Topic, req.Partition)
	if err != nil {
		return s.brokerError("partition segments", err)
	}
	return jsonResponse(http.StatusOK, info)
}

func (s *RPCServer) handleFetchSegmentChunk(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeFetchSegmentChunkRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid fetch-segment request: "+err.Error())
	}
	data, err := s.broker.ReadPartitionSegment(rpcRequestContext(), req.Topic, req.Partition, req.BaseOffset, req.At, req.Length)
	if err != nil {
		return s.brokerError("fetch segment", err)
	}
	return nodewire.Response{Status: http.StatusOK, ContentType: "application/octet-stream", Body: data}
}
