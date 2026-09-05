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
//
// The page is filtered to topics the caller may read (any grant on the
// name, ownership, or admin; see handlers.CanReadTopic). Filtering
// happens after pagination so tokens stay stable and cheap: a page can
// therefore come back shorter than limit, or even empty, while
// next_page_token is still set. Clients must keep paging until the
// token is empty, not until a page is short.
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
			s.Deps.Logger.Error("list topics", "err", err)
			s.WriteError(w, http.StatusInternalServerError, "list topics failed")
			return
		}
		visible := make([]topic.Topic, 0, len(ts))
		for _, t := range ts {
			if handlers.CanReadTopic(r, t.Owner, t.Name) {
				visible = append(visible, t)
			}
		}
		ts = visible
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
