package handlers

import "net/http"

type createTopicRequest struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

func (s *Set) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var req createTopicRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}

	t, err := s.deps.Broker.CreateTopic(r.Context(), req.Name, req.Partitions, req.ReplicationFactor)
	if err != nil {
		s.writeBrokerError(w, "create topic", err)
		return
	}
	s.writeJSON(w, http.StatusCreated, t)
}
