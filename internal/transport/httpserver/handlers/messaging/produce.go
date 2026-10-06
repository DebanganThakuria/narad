package messaging

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type produceQuery struct {
	key          string
	hasPartition bool
	partition    int
}

// Produce handles POST /v1/topics/{topic}/produce?key=...&partition=...
//
// The request body is the message payload. For topics without a schema the
// payload is opaque bytes; schema-enabled topics validate the same bytes in the
// broker before accepting them into the ingress WAL.
//
// A keyless produce stays keyless: the partitioner round-robins an empty
// key, and the partition is fixed at accept time, so nothing downstream
// needs one. (A synthetic key used to be generated here for stable
// routing across forwarded produce hops; WAL-first accept removed that
// need, and the key cost bytes on disk and showed consumers a key the
// producer never set.)
func Produce(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if !s.Authorize(w, r, user.ActionProduce, topicName) {
			return
		}
		if !admitProduce(s, w) {
			return
		}
		defer s.Deps.Drain.Done()

		query, ok := parseProduceQuery(s, w, r)
		if !ok {
			return
		}

		body, ok := s.ReadBody(w, r, handlers.MaxMessageBodyBytes)
		if !ok {
			return
		}
		if len(body) == 0 {
			s.WriteError(w, http.StatusBadRequest, "message required")
			return
		}

		var err error
		if query.hasPartition {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, query.key, body, query.partition)
		} else {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, query.key, body)
		}
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func parseProduceQuery(s *handlers.Set, w http.ResponseWriter, r *http.Request) (produceQuery, bool) {
	query, err := produceQueryFromRawQuery(r.URL.RawQuery)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return produceQuery{}, false
	}
	return query, true
}

// produceQueryFromRawQuery parses key and partition by walking the
// raw query string directly: produce is on the hot path, and
// url.ParseQuery would allocate a map for parameters the handler
// ignores. Walking the string also lets it reject duplicate key or
// partition parameters, which url.Values would silently collapse.
func produceQueryFromRawQuery(raw string) (produceQuery, error) {
	var out produceQuery
	seenKey := false
	seenPartition := false

	for raw != "" {
		part := raw
		if idx := strings.IndexByte(raw, '&'); idx >= 0 {
			part = raw[:idx]
			raw = raw[idx+1:]
		} else {
			raw = ""
		}
		if part == "" {
			continue
		}

		key, value, hasValue := strings.Cut(part, "=")
		decodedKey, err := unescapeQueryComponent(key)
		if err != nil {
			return produceQuery{}, fmt.Errorf("invalid query parameter: %w", err)
		}

		switch decodedKey {
		case "key":
			if seenKey {
				return produceQuery{}, errors.New("duplicate key query parameter")
			}
			seenKey = true
			if !hasValue || value == "" {
				continue
			}
			decodedValue, err := unescapeQueryComponent(value)
			if err != nil {
				return produceQuery{}, fmt.Errorf("invalid key: %w", err)
			}
			out.key = decodedValue
		case "partition":
			if seenPartition {
				return produceQuery{}, errors.New("duplicate partition query parameter")
			}
			seenPartition = true
			if !hasValue || value == "" {
				return produceQuery{}, errors.New("invalid partition: empty")
			}
			decodedValue, err := unescapeQueryComponent(value)
			if err != nil {
				return produceQuery{}, fmt.Errorf("invalid partition: %w", err)
			}
			partition, err := strconv.Atoi(decodedValue)
			if err != nil {
				return produceQuery{}, fmt.Errorf("invalid partition: %w", err)
			}
			if partition < 0 {
				return produceQuery{}, errors.New("invalid partition: must be >= 0")
			}
			out.partition = partition
			out.hasPartition = true
		}
	}

	return out, nil
}

// unescapeQueryComponent skips url.QueryUnescape for the common case
// of a component with no escape characters.
func unescapeQueryComponent(s string) (string, error) {
	if strings.ContainsAny(s, "%+") {
		return url.QueryUnescape(s)
	}
	return s, nil
}
