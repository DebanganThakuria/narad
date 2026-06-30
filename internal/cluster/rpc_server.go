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
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
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

type RPCServer struct {
	broker      broker.Broker
	store       *metastore.Store
	logger      *slog.Logger
	broadcaster purgeBroadcaster
}

type rpcCreateTopicBody struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

type rpcAlterTopicBody struct {
	Partitions                int             `json:"partitions"`
	RetentionMs               *int64          `json:"retention_ms,omitempty"`
	MaxInFlightPerPartition   *int64          `json:"max_in_flight_per_partition,omitempty"`
	MaxAckedAheadPerPartition *int64          `json:"max_acked_ahead_per_partition,omitempty"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

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
	default:
		res = errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", op))
	}
	return res
}

func (s *RPCServer) handleCommitProduce(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce request: "+err.Error())
	}
	offset, err := s.broker.CommitAcceptedProduce(rpcRequestContext(), ingress.ProduceRecord{
		Topic:           req.Topic,
		Key:             req.Key,
		TargetPartition: req.TargetPartition,
		Payload:         req.Payload,
		CreatedAtUnixMs: req.CreatedAtUnixMs,
	})
	if err != nil {
		return s.brokerError("commit produce", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": req.TargetPartition,
	})
}

func (s *RPCServer) handleCommitProduceBatch(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCommitProduceBatchRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid commit produce batch request: "+err.Error())
	}
	records := make([]ingress.ProduceRecord, 0, len(req.Records))
	for _, record := range req.Records {
		records = append(records, ingress.ProduceRecord{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	offsets, err := s.broker.CommitAcceptedProduceBatch(rpcRequestContext(), records)
	if err != nil {
		return s.brokerError("commit produce batch", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offsets": offsets,
		"count":   len(offsets),
	})
}

func (s *RPCServer) handleProduce(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeProduceRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid produce request: "+err.Error())
	}
	if len(req.Payload) == 0 {
		return errorResponse(http.StatusBadRequest, "message required")
	}
	offset, partition, err := s.broker.Produce(rpcRequestContext(), req.Topic, req.Key, req.Payload, req.Partition)
	if err != nil {
		return s.brokerError("produce", err)
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": partition,
	})
}

func (s *RPCServer) handleConsume(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeConsumeRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid consume request: "+err.Error())
	}
	opts := brokermsg.ConsumeOpts{Wait: time.Duration(req.WaitNanos)}
	if req.HasPartition {
		partition := req.Partition
		opts.Partition = &partition
	}
	if req.HasOffset {
		offset := req.Offset
		opts.Offset = &offset
	}
	msg, found, err := s.broker.Consume(rpcRequestContext(), req.Topic, opts)
	if errors.Is(err, brokermsg.ErrNotPartitionOwner) && req.LocalOnly {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	if err != nil {
		return s.brokerError("consume", err)
	}
	if !found {
		return nodewire.Response{Status: http.StatusNoContent}
	}
	return jsonResponse(http.StatusOK, msg)
}

func (s *RPCServer) handleAck(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeAckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid ack request: "+err.Error())
	}
	if err := s.broker.Ack(rpcRequestContext(), req.Topic, consumer.Handle{
		Partition: req.Partition,
		Offset:    req.Offset,
		Nonce:     req.Nonce,
	}); err != nil {
		return s.brokerError("ack", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleCreateTopic(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicBodyRequest(payload, nodewire.OpCreateTopic)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid create topic request: "+err.Error())
	}
	var body rpcCreateTopicBody
	if err := decodeStrictJSON(req.Body, &body); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid json: "+err.Error())
	}
	t, err := s.broker.CreateTopic(rpcRequestContext(), brokertopics.CreateOpts{
		Name:                      body.Name,
		Partitions:                body.Partitions,
		RetentionMs:               body.RetentionMs,
		VisibilityTimeoutMs:       body.VisibilityTimeoutMs,
		MaxInFlightPerPartition:   body.MaxInFlightPerPartition,
		MaxAckedAheadPerPartition: body.MaxAckedAheadPerPartition,
		Schema:                    body.Schema,
	})
	if err != nil {
		return s.brokerError("create topic", err)
	}
	return jsonResponse(http.StatusCreated, t)
}

func (s *RPCServer) handleAlterTopic(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicBodyRequest(payload, nodewire.OpAlterTopic)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid alter topic request: "+err.Error())
	}
	if req.Topic == "" {
		return errorResponse(http.StatusBadRequest, "topic required")
	}
	var body rpcAlterTopicBody
	if err := decodeStrictJSON(req.Body, &body); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid json: "+err.Error())
	}
	if err := body.validate(); err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}

	var (
		t        topic.Topic
		alterErr error
	)
	if body.RetentionMs != nil {
		t, alterErr = s.broker.UpdateTopicRetention(rpcRequestContext(), req.Topic, *body.RetentionMs)
		if alterErr != nil {
			return s.brokerError("alter topic", alterErr)
		}
	}
	if body.MaxInFlightPerPartition != nil || body.MaxAckedAheadPerPartition != nil {
		current := t
		if current.Name == "" {
			current, alterErr = s.broker.GetTopic(rpcRequestContext(), req.Topic)
			if alterErr != nil {
				return s.brokerError("alter topic", alterErr)
			}
		}
		newIF := current.MaxInFlightPerPartition
		if body.MaxInFlightPerPartition != nil {
			newIF = *body.MaxInFlightPerPartition
		}
		newAA := current.MaxAckedAheadPerPartition
		if body.MaxAckedAheadPerPartition != nil {
			newAA = *body.MaxAckedAheadPerPartition
		}
		t, alterErr = s.broker.UpdateTopicCaps(rpcRequestContext(), req.Topic, newIF, newAA)
		if alterErr != nil {
			return s.brokerError("alter topic", alterErr)
		}
	}
	if body.Partitions > 0 {
		t, alterErr = s.broker.IncreaseTopicPartitions(rpcRequestContext(), req.Topic, body.Partitions)
		if alterErr != nil {
			return s.brokerError("alter topic", alterErr)
		}
	}
	if len(body.Schema) > 0 {
		t, alterErr = s.broker.UpdateTopicSchema(rpcRequestContext(), req.Topic, body.Schema)
		if alterErr != nil {
			return s.brokerError("alter topic", alterErr)
		}
	}
	return jsonResponse(http.StatusOK, t)
}

func (s *RPCServer) handleDeleteTopic(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicNameRequest(payload, nodewire.OpDeleteTopic)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid delete topic request: "+err.Error())
	}
	if err := s.broker.DeleteTopic(rpcRequestContext(), req.Topic); err != nil {
		return s.brokerError("delete topic", err)
	}
	// The metastore record is gone and this node's own files are purged.
	// Fan the purge out to the other members so the partition owners drop
	// their directories too — the HTTP handler does this for a
	// leader-direct delete, but a follower-originated delete reaches the
	// leader here over RPC. Best-effort: the metastore delete already
	// succeeded, and the startup sweep reclaims any member we miss (e.g.
	// one that is briefly unreachable), so a fan-out failure must not fail
	// the delete.
	if s.broadcaster != nil {
		if err := s.broadcaster.BroadcastDeleteTopic(rpcRequestContext(), req.Topic); err != nil {
			s.logger.Warn("broadcast topic purge after forwarded delete failed; orphans will be reclaimed by startup sweep", "topic", req.Topic, "err", err)
		}
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handlePurgeTopic(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicNameRequest(payload, nodewire.OpPurgeTopic)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid purge topic request: "+err.Error())
	}
	// Wait until this node's local metastore replica reflects the
	// deletion before removing files. The leader broadcasts this purge
	// after the delete is quorum-committed, but a follower applies it to
	// its local replica asynchronously. Purging before the local replica
	// catches up would let a concurrent produce-dispatch/consume re-open
	// (and thus resurrect) the partition logs via Logs.Get, which keys
	// off the local replica. If the replica never reflects the deletion
	// (timeout), we skip the purge rather than risk deleting live data;
	// the startup orphan sweep is the backstop.
	if s.store != nil && !s.waitTopicDeletedLocally(req.Topic, purgeApplyWaitTimeout) {
		s.logger.Warn("skipping purge: local metastore still shows topic; deferring to orphan sweep", "topic", req.Topic)
		return nodewire.Response{Status: http.StatusNoContent}
	}
	if err := s.broker.PurgeTopic(rpcRequestContext(), req.Topic); err != nil {
		return s.brokerError("purge topic", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// purgeApplyWaitTimeout bounds how long a purge waits for the local Raft
// replica to reflect a topic deletion. Apply lag is normally sub-second.
const purgeApplyWaitTimeout = 5 * time.Second

// waitTopicDeletedLocally returns true once the local metastore no longer
// has the topic, or false if it still does after the timeout.
func (s *RPCServer) waitTopicDeletedLocally(topicName string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := s.store.GetTopic(rpcRequestContext(), topicName); errors.Is(err, errs.ErrNotFound) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *RPCServer) handleTopicPartitionStats(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicPartitionStatsRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid topic stats request: "+err.Error())
	}
	details, err := s.broker.GetTopicDetails(rpcRequestContext(), req.Topic)
	if err != nil {
		return s.brokerError("get topic", err)
	}
	if req.Partition < 0 || req.Partition >= len(details.Partitions) {
		return errorResponse(http.StatusBadRequest, "invalid partition")
	}
	return jsonResponse(http.StatusOK, details.Partitions[req.Partition])
}

func (s *RPCServer) handleRegisterMember(payload []byte) nodewire.Response {
	if s.store == nil {
		return errorResponse(http.StatusInternalServerError, "metastore unavailable")
	}
	req, err := nodewire.DecodeMemberRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid member request: "+err.Error())
	}
	member := metastore.Member{
		ID:            strings.TrimSpace(req.ID),
		Addr:          strings.TrimSpace(req.Addr),
		ClusterAddr:   strings.TrimSpace(req.ClusterAddr),
		Status:        metastore.MemberStatus(strings.TrimSpace(req.Status)),
		LastHeartbeat: req.LastHeartbeat,
	}
	if member.ID == "" {
		return errorResponse(http.StatusBadRequest, "member id is required")
	}
	if member.Addr == "" {
		return errorResponse(http.StatusBadRequest, "member addr is required")
	}
	if member.Status == "" {
		member.Status = metastore.MemberAlive
	}
	if member.Status != metastore.MemberAlive && member.Status != metastore.MemberDead {
		return errorResponse(http.StatusBadRequest, "member status is invalid")
	}
	if member.LastHeartbeat == 0 {
		member.LastHeartbeat = time.Now().Unix()
	}
	if err := s.store.RegisterMember(rpcRequestContext(), member); err != nil {
		if s.logger != nil {
			s.logger.Error("register member", "member", member.ID, "err", err)
		}
		return errorResponse(http.StatusConflict, "register member failed")
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (req rpcAlterTopicBody) validate() error {
	hasPartitions := req.Partitions > 0
	hasRetention := req.RetentionMs != nil
	hasCaps := req.MaxInFlightPerPartition != nil || req.MaxAckedAheadPerPartition != nil
	hasSchema := len(req.Schema) > 0

	if !hasPartitions && !hasRetention && !hasCaps && !hasSchema {
		return errors.New("at least one of partitions, retention_ms, max_*_per_partition, or schema is required")
	}
	if hasRetention && *req.RetentionMs < 0 {
		return errors.New("retention_ms must be >= 0 (0 = use default)")
	}
	if req.MaxInFlightPerPartition != nil && *req.MaxInFlightPerPartition < 0 {
		return errors.New("max_in_flight_per_partition must be >= 0 (0 = use default)")
	}
	if req.MaxAckedAheadPerPartition != nil && *req.MaxAckedAheadPerPartition < 0 {
		return errors.New("max_acked_ahead_per_partition must be >= 0 (0 = use default)")
	}
	if hasSchema && !json.Valid(req.Schema) {
		return errors.New("schema is not valid JSON")
	}
	return nil
}

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
