package topics

import (
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// List handles GET /v1/topics with keyset pagination.
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
func List(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit, ok := parseLimit(s, w, r)
		if !ok {
			return
		}
		pageToken := r.URL.Query().Get("page_token")

		ts, nextToken, err := s.Deps.Broker.ListTopics(r.Context(), metastore.ListOptions{
			Limit:     limit,
			PageToken: pageToken,
		})
		if err != nil {
			// WriteError logs the 5xx via logServerError; no separate log here.
			s.WriteError(w, http.StatusInternalServerError, "list topics failed")
			return
		}
		if ts == nil {
			ts = []topic.Topic{}
		}
		s.WriteJSON(w, http.StatusOK, map[string]any{
			"topics":          ts,
			"next_page_token": nextToken,
		})
	}
}

func parseLimit(s *handlers.Set, w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		s.WriteError(w, http.StatusBadRequest, "limit must be a positive integer")
		return 0, false
	}
	if n > maxLimit {
		n = maxLimit
	}
	return n, true
}
