package cluster

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type rpcCreateTopicBody struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
	// Owner is set by the authenticated ingress node before forwarding;
	// the cluster port is not client-reachable, so it is trusted here.
	Owner string `json:"owner,omitempty"`
}

type rpcAlterTopicBody struct {
	Partitions                int             `json:"partitions"`
	RetentionMs               *int64          `json:"retention_ms,omitempty"`
	MaxInFlightPerPartition   *int64          `json:"max_in_flight_per_partition,omitempty"`
	MaxAckedAheadPerPartition *int64          `json:"max_acked_ahead_per_partition,omitempty"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

func (b rpcAlterTopicBody) validate() error {
	hasPartitions := b.Partitions > 0
	hasRetention := b.RetentionMs != nil
	hasCaps := b.MaxInFlightPerPartition != nil || b.MaxAckedAheadPerPartition != nil
	hasSchema := len(b.Schema) > 0

	if !hasPartitions && !hasRetention && !hasCaps && !hasSchema {
		return errors.New("at least one of partitions, retention_ms, max_*_per_partition, or schema is required")
	}
	if hasRetention && *b.RetentionMs < 0 {
		return errors.New("retention_ms must be >= 0 (0 = use default)")
	}
	if b.MaxInFlightPerPartition != nil && *b.MaxInFlightPerPartition < 0 {
		return errors.New("max_in_flight_per_partition must be >= 0 (0 = use default)")
	}
	if b.MaxAckedAheadPerPartition != nil && *b.MaxAckedAheadPerPartition < 0 {
		return errors.New("max_acked_ahead_per_partition must be >= 0 (0 = use default)")
	}
	if hasSchema && !json.Valid(b.Schema) {
		return errors.New("schema is not valid JSON")
	}
	return nil
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
		Owner:                     body.Owner,
	})
	if err != nil {
		return s.brokerError("create topic", err)
	}
	return jsonResponse(http.StatusCreated, t)
}

func (s *RPCServer) handleGetTopic(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicNameRequest(payload, nodewire.OpGetTopic)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid get topic request: "+err.Error())
	}
	t, err := s.broker.GetTopic(rpcRequestContext(), req.Topic)
	if err != nil {
		return s.brokerError("get topic", err)
	}
	return jsonResponse(http.StatusOK, t)
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
	t, err := s.applyTopicAlterations(req.Topic, body)
	if err != nil {
		return s.brokerError("alter topic", err)
	}
	return jsonResponse(http.StatusOK, t)
}

// applyTopicAlterations applies the requested field groups one at a time, in
// a fixed order, and returns the topic as of the last successful update. An
// error aborts the sequence, so a multi-field alter can be partially applied
// — each group is an independent broker update with no cross-group
// transaction.
func (s *RPCServer) applyTopicAlterations(topicName string, body rpcAlterTopicBody) (topic.Topic, error) {
	var t topic.Topic
	var err error
	if body.RetentionMs != nil {
		if t, err = s.broker.UpdateTopicRetention(rpcRequestContext(), topicName, *body.RetentionMs); err != nil {
			return topic.Topic{}, err
		}
	}
	if body.MaxInFlightPerPartition != nil || body.MaxAckedAheadPerPartition != nil {
		// UpdateTopicCaps replaces both caps, so an alter that sets only one
		// must carry the other's current value forward.
		current := t
		if current.Name == "" {
			if current, err = s.broker.GetTopic(rpcRequestContext(), topicName); err != nil {
				return topic.Topic{}, err
			}
		}
		inFlight := current.MaxInFlightPerPartition
		if body.MaxInFlightPerPartition != nil {
			inFlight = *body.MaxInFlightPerPartition
		}
		ackedAhead := current.MaxAckedAheadPerPartition
		if body.MaxAckedAheadPerPartition != nil {
			ackedAhead = *body.MaxAckedAheadPerPartition
		}
		if t, err = s.broker.UpdateTopicCaps(rpcRequestContext(), topicName, inFlight, ackedAhead); err != nil {
			return topic.Topic{}, err
		}
	}
	if body.Partitions > 0 {
		if t, err = s.broker.IncreaseTopicPartitions(rpcRequestContext(), topicName, body.Partitions); err != nil {
			return topic.Topic{}, err
		}
	}
	if len(body.Schema) > 0 {
		if t, err = s.broker.UpdateTopicSchema(rpcRequestContext(), topicName, body.Schema); err != nil {
			return topic.Topic{}, err
		}
	}
	return t, nil
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

// purgeApplyWaitTimeout bounds how long a purge waits for the local Raft
// replica to reflect a topic deletion. Apply lag is normally sub-second.
const purgeApplyWaitTimeout = 5 * time.Second

// purgeExecutionAllowance budgets the purge work a remote member does AFTER
// its replica reflects the deletion (removing partition directories, closing
// logs). The leader's per-member purge deadline is
// purgeApplyWaitTimeout + purgeExecutionAllowance (+ RPC grace): a member
// can exhaust the full apply wait before it even starts deleting, so the
// deadline must cover both phases while staying bounded.
const purgeExecutionAllowance = 10 * time.Second

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
