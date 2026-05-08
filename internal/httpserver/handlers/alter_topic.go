package handlers

import (
	"errors"
	"net/http"
)

type alterTopicRequest struct {
	Partitions int `json:"partitions"`
}

func (req alterTopicRequest) Validate() error {
	if req.Partitions <= 0 {
		return errors.New("partitions must be > 0")
	}
	return nil
}

// AlterTopic handles PATCH /v1/topics/{topic}. Currently only
// supports increasing the partition count.
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

	t, err := s.deps.Broker.IncreaseTopicPartitions(r.Context(), topicName, req.Partitions)
	if err != nil {
		s.writeBrokerError(w, "alter topic", err)
		return
	}
	s.writeJSON(w, http.StatusOK, t)
}
