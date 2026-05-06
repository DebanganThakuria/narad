package handlers

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/topic"
)

// ListTopics handles GET /topics.
func (s *Set) ListTopics(w http.ResponseWriter, r *http.Request) {
	ts, err := s.deps.Broker.ListTopics(r.Context())
	if err != nil {
		s.deps.Logger.Error("list topics", "err", err)
		s.writeError(w, http.StatusInternalServerError, "list topics failed")
		return
	}
	if ts == nil {
		ts = []topic.Topic{}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"topics": ts})
}
