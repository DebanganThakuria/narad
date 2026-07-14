package messaging

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/user"
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
		if !s.Authorize(w, r, user.ActionConsume, topicName) {
			return
		}

		opts, localOnly, ok := parseConsumeQuery(s, w, r)
		if !ok {
			return
		}

		// local_only marks a peer's fan-out probe. Answer it strictly
		// from local partitions without waiting; routing it onward
		// would bounce the probe around the cluster.
		if localOnly && isQueueConsume(opts) {
			opts.Wait = 0
			if consumeOnce(s, w, r, topicName, opts) {
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if s.Deps.Router != nil {
			forwarded, localPartition := s.Deps.Router.RouteConsume(r.Context(), w, r, topicName, opts.Partition)
			if forwarded {
				return
			}
			if localPartition != nil && isQueueConsume(opts) {
				queueConsumeWithLocalOwner(s, w, r, topicName, opts, *localPartition)
				return
			}
		}

		if consumeOnce(s, w, r, topicName, opts) {
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// queueConsumeWithLocalOwner serves a queue-style consume on a node
// that owns at least one partition: try the router-selected local
// partition, then any other local partition, then remote owners, and
// only then spend the requested wait long-polling locally.
func queueConsumeWithLocalOwner(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts, localPartition int) {
	wait := opts.Wait

	pinned := opts
	pinned.Partition = &localPartition
	pinned.Wait = 0
	if consumeOnce(s, w, r, topicName, pinned) {
		return
	}

	localScan := opts
	localScan.Partition = nil
	localScan.Wait = 0
	if consumeOnce(s, w, r, topicName, localScan) {
		return
	}

	if forwarded, _ := s.Deps.Router.RouteConsumeRemote(r.Context(), w, r, topicName); forwarded {
		return
	}
	if wait <= 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	longPoll := opts
	longPoll.Partition = nil
	longPoll.Wait = wait
	if consumeOnce(s, w, r, topicName, longPoll) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// consumeOnce performs a single broker consume and reports whether a
// response was written. Queue-style consumes treat ErrNotPartitionOwner
// as "no message here" — ownership may have just moved, and the caller
// decides whether to route elsewhere or answer 204.
func consumeOnce(s *handlers.Set, w http.ResponseWriter, r *http.Request, topicName string, opts brokermsg.ConsumeOpts) bool {
	msg, found, err := s.Deps.Broker.Consume(r.Context(), topicName, opts)
	if isQueueConsume(opts) && errors.Is(err, brokermsg.ErrNotPartitionOwner) {
		return false
	}
	if err != nil {
		s.WriteBrokerError(w, "consume", err)
		return true
	}
	if !found {
		return false
	}
	s.WriteJSON(w, http.StatusOK, msg)
	return true
}

// isQueueConsume reports whether opts describe a queue-style pull:
// no explicit partition and no replay offset.
func isQueueConsume(opts brokermsg.ConsumeOpts) bool {
	return opts.Partition == nil && opts.Offset == nil
}

func parseConsumeQuery(s *handlers.Set, w http.ResponseWriter, r *http.Request) (brokermsg.ConsumeOpts, bool, bool) {
	q := r.URL.Query()
	opts := brokermsg.ConsumeOpts{}
	localOnly := q.Get("local_only") == "1"

	if v := q.Get("partition"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
			return opts, false, false
		}
		opts.Partition = &p
	}
	if v := q.Get("offset"); v != "" {
		o, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid offset: "+err.Error())
			return opts, false, false
		}
		if o < 0 {
			s.WriteError(w, http.StatusBadRequest, "invalid offset: must be >= 0")
			return opts, false, false
		}
		opts.Offset = &o
	}
	if v := q.Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid wait: "+err.Error())
			return opts, false, false
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
	return opts, localOnly, true
}
