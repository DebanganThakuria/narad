package messaging

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

var generatedProduceKeySeq atomic.Uint64

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
func Produce(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		query, err := produceQueryFromRawQuery(r.URL.RawQuery)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
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

		key := query.key
		if key == "" {
			key = generateProduceKey()
		}

		if query.hasPartition {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, body, query.partition)
		} else {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, body)
		}
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

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
		decodedKey, err := queryComponent(key)
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
			decodedValue, err := queryComponent(value)
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
			decodedValue, err := queryComponent(value)
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

func queryComponent(s string) (string, error) {
	if strings.ContainsAny(s, "%+") {
		return url.QueryUnescape(s)
	}
	return s, nil
}

func generateProduceKey() string {
	seq := generatedProduceKeySeq.Add(1)
	key := make([]byte, 0, 17)
	key = append(key, "key-"...)
	key = strconv.AppendUint(key, seq, 36)
	return string(key)
}
