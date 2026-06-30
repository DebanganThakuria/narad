package topics

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// alterRequest accepts any combination of:
//   - partitions:                    increase partition count
//   - retention_ms:                  update retention duration
//   - max_in_flight_per_partition:   per-partition in-flight cap
//   - max_acked_ahead_per_partition: per-partition acked-ahead cap
//   - schema:                        register a new JSON Schema version
//
// At least one field is required. Sending multiple fields applies
// each change sequentially — if one fails the whole request fails.
//
// retention_ms / max_*_per_partition are *int64 (rather than int64)
// so the caller can distinguish "unset" from "set to zero" — zero
// means "inherit broker default".
type alterRequest struct {
	Partitions                int             `json:"partitions"`
	RetentionMs               *int64          `json:"retention_ms,omitempty"`
	MaxInFlightPerPartition   *int64          `json:"max_in_flight_per_partition,omitempty"`
	MaxAckedAheadPerPartition *int64          `json:"max_acked_ahead_per_partition,omitempty"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

func (req alterRequest) Validate() error {
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

// Alter handles PATCH /v1/topics/{topic}. Each supplied field
// triggers the matching broker call; order is retention → caps →
// partitions → schema. The returned topic record reflects all
// applied changes.
func Alter(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		body, ok := s.ReadBody(w, r, handlers.MaxJSONBodyBytes)
		if !ok {
			return
		}

		var req alterRequest
		if !s.DecodeJSONBytes(w, body, &req) {
			return
		}
		if err := req.Validate(); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		if s.Deps.Router != nil {
			if s.Deps.Router.RouteAlterTopic(r.Context(), w, r, topicName, body) {
				return
			}
		}

		var t topic.Topic

		// applyStep runs one broker mutation, stores its result in t, and
		// reports whether the request can continue. On error it writes the
		// mapped broker error and returns false so the caller bails out.
		applyStep := func(fn func() (topic.Topic, error)) bool {
			updated, err := fn()
			if err != nil {
				s.WriteBrokerError(w, "alter topic", err)
				return false
			}
			t = updated
			return true
		}

		if req.RetentionMs != nil {
			if !applyStep(func() (topic.Topic, error) {
				return s.Deps.Broker.UpdateTopicRetention(r.Context(), topicName, *req.RetentionMs)
			}) {
				return
			}
		}
		if req.MaxInFlightPerPartition != nil || req.MaxAckedAheadPerPartition != nil {
			current := t
			if current.Name == "" {
				if !applyStep(func() (topic.Topic, error) {
					return s.Deps.Broker.GetTopic(r.Context(), topicName)
				}) {
					return
				}
				current = t
			}
			newIF := current.MaxInFlightPerPartition
			if req.MaxInFlightPerPartition != nil {
				newIF = *req.MaxInFlightPerPartition
			}
			newAA := current.MaxAckedAheadPerPartition
			if req.MaxAckedAheadPerPartition != nil {
				newAA = *req.MaxAckedAheadPerPartition
			}
			if !applyStep(func() (topic.Topic, error) {
				return s.Deps.Broker.UpdateTopicCaps(r.Context(), topicName, newIF, newAA)
			}) {
				return
			}
		}
		if req.Partitions > 0 {
			if !applyStep(func() (topic.Topic, error) {
				return s.Deps.Broker.IncreaseTopicPartitions(r.Context(), topicName, req.Partitions)
			}) {
				return
			}
		}
		if len(req.Schema) > 0 {
			if !applyStep(func() (topic.Topic, error) {
				return s.Deps.Broker.UpdateTopicSchema(r.Context(), topicName, req.Schema)
			}) {
				return
			}
		}
		s.WriteJSON(w, http.StatusOK, t)
	}
}
