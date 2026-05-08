package handlers

import (
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/topic"
)

const (
	listTopicsDefaultLimit = 100
	listTopicsMaxLimit     = 1000
)

// ListTopics handles GET /v1/topics with keyset pagination.
//
// Query params:
//
//	?limit=N         max page size (default 100, max 1000)
//	?page_token=X    cursor returned by the previous call (empty for first page)
//
// Response:
//
//	{ "topics": [...], "next_page_token": "..." }
//
// next_page_token is "" when no more pages remain.
func (s *Set) ListTopics(w http.ResponseWriter, r *http.Request) {
	limit, ok := s.parseListLimit(w, r)
	if !ok {
		return
	}
	pageToken := r.URL.Query().Get("page_token")

	ts, nextToken, err := s.deps.Broker.ListTopics(r.Context(), metastore.ListOptions{
		Limit:     limit,
		PageToken: pageToken,
	})
	if err != nil {
		s.deps.Logger.Error("list topics", "err", err)
		s.writeError(w, http.StatusInternalServerError, "list topics failed")
		return
	}
	if ts == nil {
		ts = []topic.Topic{}
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"topics":          ts,
		"next_page_token": nextToken,
	})
}

func (s *Set) parseListLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return listTopicsDefaultLimit, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		s.writeError(w, http.StatusBadRequest, "limit must be a positive integer")
		return 0, false
	}
	if n > listTopicsMaxLimit {
		n = listTopicsMaxLimit
	}
	return n, true
}
