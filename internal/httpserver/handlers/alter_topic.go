package handlers

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/topic"
)

// alterTopicRequest accepts exactly one of:
//   - partitions: increase partition count
//   - retention:  update retention bounds (max_age_ms, max_bytes)
//
// Both at once is rejected — they're independent operational changes
// and one-thing-per-PATCH keeps semantics tight. To do both, send two
// PATCHes.
type alterTopicRequest struct {
	Partitions int              `json:"partitions"`
	Retention  *topic.Retention `json:"retention,omitempty"`
}

func (req alterTopicRequest) Validate() error {
	hasPartitions := req.Partitions > 0
	hasRetention := req.Retention != nil
	switch {
	case !hasPartitions && !hasRetention:
		return errors.New("exactly one of partitions or retention is required")
	case hasPartitions && hasRetention:
		return errors.New("partitions and retention cannot be updated in the same request")
	case hasPartitions && req.Partitions <= 0:
		return errors.New("partitions must be > 0")
	}
	return nil
}

// AlterTopic handles PATCH /v1/topics/{topic}. Dispatches to the
// broker method that matches the field set in the request.
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
	switch {
	case req.Retention != nil:
		t, err = s.deps.Broker.UpdateTopicRetention(r.Context(), topicName, *req.Retention)
	default:
		t, err = s.deps.Broker.IncreaseTopicPartitions(r.Context(), topicName, req.Partitions)
	}
	if err != nil {
		s.writeBrokerError(w, "alter topic", err)
		return
	}
	s.writeJSON(w, http.StatusOK, t)
}
