package messaging

import (
	"errors"
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
// materializes within wait.
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

		if isLocalOnlyConsumeProbe(r, opts) {
			opts.Wait = 0
			done, found := consumeLocalOnly(s, w, r, topicName, opts)
			if done {
				return
			}
			if !found {
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}

		if s.Deps.Router != nil {
			forwarded, localPartition := s.Deps.Router.RouteConsume(r.Context(), w, r, topicName, opts.Partition)
			if forwarded {
				return
			}
			if localPartition != nil && opts.Offset == nil && opts.Partition == nil {
				handleQueueConsumeWithLocalOwner(s, w, r, topicName, opts, *localPartition)
				return
			}
		}

		done, found := consumeOnce(s, w, r, topicName, opts)
		if done {
			return
		}
		if !found {
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func handleQueueConsumeWithLocalOwner(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts, localPartition int) {
	originalWait := opts.Wait

	pinnedOpts := opts
	pinnedOpts.Partition = &localPartition
	pinnedOpts.Wait = 0
	if done, _ := consumeOnce(s, w, r, topicName, pinnedOpts); done {
		return
	}

	localScanOpts := opts
	localScanOpts.Partition = nil
	localScanOpts.Wait = 0
	if done, _ := consumeOnce(s, w, r, topicName, localScanOpts); done {
		return
	}

	forwarded, _ := s.Deps.Router.RouteConsumeRemote(r.Context(), w, r, topicName)
	if forwarded {
		return
	}
	if originalWait <= 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	waitOpts := opts
	waitOpts.Partition = nil
	waitOpts.Wait = originalWait
	done, found := consumeOnce(s, w, r, topicName, waitOpts)
	if done {
		return
	}
	if !found {
		w.WriteHeader(http.StatusNoContent)
	}
}

func isLocalOnlyConsumeProbe(r *http.Request, opts brokermsg.ConsumeOpts) bool {
	return r.URL.Query().Get("local_only") == "1" && opts.Partition == nil && opts.Offset == nil
}

func consumeLocalOnly(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts) (bool, bool) {
	msg, found, err := s.Deps.Broker.Consume(r.Context(), topicName, opts)
	if errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		return false, false
	}
	if err != nil {
		s.WriteBrokerError(w, "consume", err)
		return true, false
	}
	if !found {
		return false, false
	}
	s.WriteJSON(w, http.StatusOK, msg)
	return true, true
}

func consumeOnce(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts) (bool, bool) {
	msg, found, err := s.Deps.Broker.Consume(r.Context(), topicName, opts)
	if isQueueConsume(opts) && errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		return false, false
	}
	if err != nil {
		s.WriteBrokerError(w, "consume", err)
		return true, false
	}
	if !found {
		return false, false
	}
	s.WriteJSON(w, http.StatusOK, msg)
	return true, true
}

func isQueueConsume(opts brokermsg.ConsumeOpts) bool {
	return opts.Partition == nil && opts.Offset == nil
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
		ceiling := s.Deps.MaxConsumeWait
		if ceiling <= 0 {
			ceiling = handlers.DefaultMaxConsumeWait
		}
		if d > ceiling {
			d = ceiling
		}
		opts.Wait = d
	}
	return opts, true
}
