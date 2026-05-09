package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/topic"
)

// alterTopicRequest accepts any combination of:
//   - partitions:    increase partition count
//   - retention_ms:  update retention duration (0 = inherit default)
//   - schema:        register a new JSON Schema version
//
// At least one field is required. Sending multiple fields applies each
// change sequentially — if one fails the whole request fails.
//
// retention_ms is a *int64 (rather than int64) so the caller can
// distinguish "unset" from "set to zero" — zero means "inherit
// TopicConfig.DefaultRetentionMs".
type alterTopicRequest struct {
	Partitions  int             `json:"partitions"`
	RetentionMs *int64          `json:"retention_ms,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

func (req alterTopicRequest) Validate() error {
	hasPartitions := req.Partitions > 0
	hasRetention := req.RetentionMs != nil
	hasSchema := len(req.Schema) > 0

	if !hasPartitions && !hasRetention && !hasSchema {
		return errors.New("at least one of partitions, retention_ms, or schema is required")
	}
	if hasRetention && *req.RetentionMs < 0 {
		return errors.New("retention_ms must be >= 0 (0 = use default)")
	}
	if hasSchema && !json.Valid(req.Schema) {
		return errors.New("schema is not valid JSON")
	}
	return nil
}

// AlterTopic handles PATCH /v1/topics/{topic}. Each supplied field
// triggers the matching broker call; order is retention → partitions →
// schema. The returned topic record reflects all applied changes.
func (s *Set) AlterTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	var req alterTopicRequest
	if !s.decodeAndValidate(w, r, &req) {
		return
	}

	var (
		t   topic.Topic
		err error
	)

	if req.RetentionMs != nil {
		t, err = s.deps.Broker.UpdateTopicRetention(r.Context(), topicName, *req.RetentionMs)
		if err != nil {
			s.writeBrokerError(w, "alter topic", err)
			return
		}
	}
	if req.Partitions > 0 {
		t, err = s.deps.Broker.IncreaseTopicPartitions(r.Context(), topicName, req.Partitions)
		if err != nil {
			s.writeBrokerError(w, "alter topic", err)
			return
		}
	}
	if len(req.Schema) > 0 {
		t, err = s.deps.Broker.UpdateTopicSchema(r.Context(), topicName, req.Schema)
		if err != nil {
			s.writeBrokerError(w, "alter topic", err)
			return
		}
	}

	s.writeJSON(w, http.StatusOK, t)
}
