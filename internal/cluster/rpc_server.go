package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// purgeBroadcaster fans a topic purge out to the other cluster members.
// *Router implements it; the RPC server holds it so a delete that was
// forwarded to the leader over RPC still triggers the same owner-pod purge
// the HTTP handler does for a leader-direct delete.
type purgeBroadcaster interface {
	BroadcastDeleteTopic(ctx context.Context, topicName string) error
}

// RPCServer serves node-to-node RPC frames: it decodes each request payload,
// invokes the local broker, and encodes the response frame. It is the peer
// side of PeerClient.
type RPCServer struct {
	broker      broker.Broker
	store       *metastore.Store
	logger      *slog.Logger
	broadcaster purgeBroadcaster
}

// NewRPCServer constructs an RPCServer around the local broker.
func NewRPCServer(br broker.Broker, store *metastore.Store, logger *slog.Logger) *RPCServer {
	return &RPCServer{broker: br, store: store, logger: logger}
}

// SetBroadcaster wires the purge fan-out used when a topic delete is
// forwarded to this node as the leader. Without it, a delete that arrives
// over RPC (i.e. from a follower) deletes the metastore record and purges
// only this node's files, leaving the owner pods' partition directories
// orphaned until the next startup sweep.
func (s *RPCServer) SetBroadcaster(b purgeBroadcaster) {
	s.broadcaster = b
}

// HandleStreamFrame serves a node RPC frame, replying asynchronously via
// respond. It reports whether the frame was one this server handles.
func (s *RPCServer) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	go func() {
		res := s.dispatch(frame.Payload)
		payload, err := nodewire.EncodeResponse(res)
		if err != nil {
			payload, _ = nodewire.EncodeResponse(errorResponse(http.StatusInternalServerError, "encode rpc response failed"))
		}
		respond(clusterwire.StreamFrame{
			Type:      clusterwire.StreamFrameNodeReply,
			RequestID: frame.RequestID,
			Payload:   payload,
		})
	}()
	return true
}

func (s *RPCServer) dispatch(payload []byte) nodewire.Response {
	op, err := nodewire.OperationOf(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid rpc request")
	}
	var res nodewire.Response
	switch op {
	case nodewire.OpProduce:
		res = s.handleProduce(payload)
	case nodewire.OpCommitProduce:
		res = s.handleCommitProduce(payload)
	case nodewire.OpCommitProduceBatch:
		res = s.handleCommitProduceBatch(payload)
	case nodewire.OpConsume:
		res = s.handleConsume(payload)
	case nodewire.OpAck:
		res = s.handleAck(payload)
	case nodewire.OpCreateTopic:
		res = s.handleCreateTopic(payload)
	case nodewire.OpAlterTopic:
		res = s.handleAlterTopic(payload)
	case nodewire.OpDeleteTopic:
		res = s.handleDeleteTopic(payload)
	case nodewire.OpPurgeTopic:
		res = s.handlePurgeTopic(payload)
	case nodewire.OpTopicPartitionStats:
		res = s.handleTopicPartitionStats(payload)
	case nodewire.OpRegisterMember:
		res = s.handleRegisterMember(payload)
	case nodewire.OpCreateUser:
		res = s.handleCreateUser(payload)
	case nodewire.OpUpdateUser:
		res = s.handleUpdateUser(payload)
	case nodewire.OpDeleteUser:
		res = s.handleDeleteUser(payload)
	default:
		res = errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", op))
	}
	return res
}

// brokerError maps a broker failure onto the RPC status vocabulary shared
// with the HTTP layer. Unrecognized errors are logged and reported as opaque
// 500s so internal details never cross the wire.
func (s *RPCServer) brokerError(op string, err error) nodewire.Response {
	switch {
	case errors.Is(err, errs.ErrTopicNotFound):
		return errorResponse(http.StatusNotFound, "topic not found")
	case errors.Is(err, errs.ErrTopicAlreadyExists):
		return errorResponse(http.StatusConflict, "topic already exists")
	case errors.Is(err, errs.ErrHandleMalformed):
		return errorResponse(http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrHandleStale):
		return errorResponse(http.StatusGone, err.Error())
	case errors.Is(err, errs.ErrAckedAheadFull):
		return errorResponse(http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, errs.ErrInvalidArgument),
		errors.Is(err, errs.ErrPartitionRequired):
		return errorResponse(http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrNotPartitionOwner):
		return errorResponse(http.StatusMisdirectedRequest, err.Error())
	default:
		if s.logger != nil {
			s.logger.Error(op, "err", err)
		}
		return errorResponse(http.StatusInternalServerError, op+" failed")
	}
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func jsonResponse(status int, v any) nodewire.Response {
	body, err := json.Marshal(v)
	if err != nil {
		return errorResponse(http.StatusInternalServerError, "encode response failed")
	}
	body = append(body, '\n')
	return nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON, Body: body}
}

func errorResponse(status int, msg string) nodewire.Response {
	body, _ := json.Marshal(map[string]string{"error": msg})
	body = append(body, '\n')
	return nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON, Body: body}
}

// RPC frames do not carry a caller context. The transport layer owns request
// timeouts, so broker operations run under a fresh internal context here.
func rpcRequestContext() context.Context {
	return context.Background()
}
