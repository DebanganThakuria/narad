package cluster

// Serve-side handlers for the partition-transfer RPCs: a node that owns
// a partition answers a destination's list/fetch requests during a
// rebalance copy. Read-only — no ownership or state change here.

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handoffConfirmer is the fenced re-arm a broker exposes when it fences
// its handoff freeze with a token (*messaging.Engine does). A broker
// without it cannot honor a token-bearing request, which is refused so
// the destination never mistakes an unfenced extend for a fence.
type handoffConfirmer interface {
	ConfirmHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration, token string) (messaging.PartitionTransferInfo, error)
}

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

func (s *RPCServer) handlePrepareHandoff(payload []byte) nodewire.Response {
	req, err := nodewire.DecodePrepareHandoffRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid prepare-handoff request: "+err.Error())
	}
	var info messaging.PartitionTransferInfo
	if req.FreezeToken != "" {
		confirmer, ok := s.broker.(handoffConfirmer)
		if !ok {
			return errorResponse(http.StatusConflict, "prepare handoff: this node does not fence handoff freezes")
		}
		info, err = confirmer.ConfirmHandoff(rpcRequestContext(), req.Topic, req.Partition, time.Duration(req.FreezeTTLNanos), req.FreezeToken)
	} else {
		info, err = s.broker.PrepareHandoff(rpcRequestContext(), req.Topic, req.Partition, time.Duration(req.FreezeTTLNanos))
	}
	if errors.Is(err, messaging.ErrHandoffFreezeLapsed) {
		// Distinct from every other failure: the destination must NOT
		// retry the flip on this token, it must re-freeze and re-drain.
		return errorResponse(http.StatusConflict, err.Error())
	}
	if err != nil {
		return s.brokerError("prepare handoff", err)
	}
	return jsonResponse(http.StatusOK, info)
}
