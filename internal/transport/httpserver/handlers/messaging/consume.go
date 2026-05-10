package messaging

import (
	"net/http"
	"strconv"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Consume handles GET /v1/topics/{topic}/consume.
//
// Query params: partition, offset, wait. No offset = queue-style
// pull. Offset set = replay (partition required). wait > 0 =
// long-poll up to MaxConsumeWait. Returns 204 if no message
// materialises within wait.
func Consume(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		opts, ok := parseConsumeQuery(s, w, r)
		if !ok {
			return
		}

		msg, found, err := s.Deps.Broker.Consume(r.Context(), topicName, opts)
		if err != nil {
			s.WriteBrokerError(w, "consume", err)
			return
		}
		if !found {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.WriteJSON(w, http.StatusOK, msg)
	}
}

func parseConsumeQuery(s *handlers.Set, w http.ResponseWriter, r *http.Request) (brokermsg.ConsumeOpts, bool) {
	q := r.URL.Query()
	opts := brokermsg.ConsumeOpts{}

	if v := q.Get("partition"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
			return opts, false
		}
		opts.Partition = &p
	}
	if v := q.Get("offset"); v != "" {
		o, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
			return opts, false
		}
		opts.Offset = &o
	}
	if v := q.Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid wait: "+err.Error())
			return opts, false
		}
		if d < 0 {
			d = 0
		}
		if s.Deps.MaxConsumeWait > 0 && d > s.Deps.MaxConsumeWait {
			d = s.Deps.MaxConsumeWait
		}
		opts.Wait = d
	}
	return opts, true
}
