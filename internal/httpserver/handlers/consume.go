package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
)

// Consume handles GET /v1/topics/{topic}/consume.
//
// Query params: partition, offset, wait. No offset = queue-style
// pull. Offset set = replay (partition required). wait > 0 =
// long-poll up to MaxConsumeWait. Returns 204 if no message
// materialises within wait.
func (s *Set) Consume(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	opts, ok := s.parseConsumeQuery(w, r)
	if !ok {
		return
	}

	msg, found, err := s.deps.Broker.Consume(r.Context(), topicName, opts)
	if err != nil {
		s.writeBrokerError(w, "consume", err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.writeJSON(w, http.StatusOK, msg)
}

func (s *Set) parseConsumeQuery(w http.ResponseWriter, r *http.Request) (broker.ConsumeOpts, bool) {
	q := r.URL.Query()
	opts := broker.ConsumeOpts{}

	if v := q.Get("partition"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
			return opts, false
		}
		opts.Partition = &p
	}
	if v := q.Get("offset"); v != "" {
		o, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
			return opts, false
		}
		opts.Offset = &o
	}
	if v := q.Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid wait: "+err.Error())
			return opts, false
		}
		if d < 0 {
			d = 0
		}
		if s.deps.MaxConsumeWait > 0 && d > s.deps.MaxConsumeWait {
			d = s.deps.MaxConsumeWait
		}
		opts.Wait = d
	}
	return opts, true
}
