package messaging

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

// consumeQuery holds the raw (unescaped) values of the query parameters
// Consume reads, exactly as url.Values.Get would have returned them.
type consumeQuery struct {
	partition string
	offset    string
	wait      string
	localOnly string
}

// consumeQueryFromRawQuery extracts Consume's four parameters in one
// walk of the raw query string: consume is on the hot path, and
// r.URL.Query() allocates a map (plus a slice per key) for every
// request. It reproduces url.ParseQuery's observable behaviour for
// these keys precisely, since callers previously read them through
// url.Values.Get:
//
//   - the FIRST successfully parsed occurrence of a key wins;
//   - a pair whose key or value carries a malformed percent-escape is
//     silently dropped (ParseQuery records the error, Query() discards
//     it), so "?partition=%ZZ" means "partition unset", not 400;
//   - a pair containing a semicolon is dropped, as ParseQuery does;
//   - a key present without "=" yields the empty value.
//
// Components are unescaped only when they contain an escape character.
func consumeQueryFromRawQuery(raw string) consumeQuery {
	var out consumeQuery
	var seenPartition, seenOffset, seenWait, seenLocalOnly bool
	for raw != "" {
		var part string
		part, raw, _ = strings.Cut(raw, "&")
		if part == "" || strings.Contains(part, ";") {
			continue
		}
		key, value, _ := strings.Cut(part, "=")
		if strings.ContainsAny(key, "%+") {
			unescaped, err := url.QueryUnescape(key)
			if err != nil {
				continue
			}
			key = unescaped
		}
		var dst *string
		var seen *bool
		switch key {
		case "partition":
			dst, seen = &out.partition, &seenPartition
		case "offset":
			dst, seen = &out.offset, &seenOffset
		case "wait":
			dst, seen = &out.wait, &seenWait
		case "local_only":
			dst, seen = &out.localOnly, &seenLocalOnly
		default:
			continue
		}
		if *seen {
			continue
		}
		if strings.ContainsAny(value, "%+") {
			unescaped, err := url.QueryUnescape(value)
			if err != nil {
				continue
			}
			value = unescaped
		}
		*dst = value
		*seen = true
	}
	return out
}

func parseConsumeQuery(s *handlers.Set, w http.ResponseWriter, r *http.Request) (brokermsg.ConsumeOpts, bool, bool) {
	q := consumeQueryFromRawQuery(r.URL.RawQuery)
	opts := brokermsg.ConsumeOpts{}
	localOnly := q.localOnly == "1"

	if v := q.partition; v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
			return opts, false, false
		}
		opts.Partition = &p
	}
	if v := q.offset; v != "" {
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
	if v := q.wait; v != "" {
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
