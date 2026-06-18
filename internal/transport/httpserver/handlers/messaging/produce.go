package messaging

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type produceRequest struct {
	Key     string          `json:"key,omitempty"`
	Message json.RawMessage `json:"message"`
}

func (req produceRequest) Validate() error {
	if len(req.Message) == 0 {
		return errors.New("message required")
	}
	if !json.Valid(req.Message) {
		return errors.New("message is not valid JSON")
	}
	return nil
}

// Produce handles POST /v1/topics/{topic}/produce.
func Produce(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		pinnedPartition, ok := parseProduceQuery(s, w, r)
		if !ok {
			return
		}

		// Read body once — may need to forward it to the partition owner.
		body, ok := s.ReadBody(w, r, handlers.MaxJSONBodyBytes)
		if !ok {
			return
		}

		var req produceRequest
		if !s.DecodeJSONBytes(w, body, &req) {
			return
		}
		if err := req.Validate(); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		key, body := produceKeyAndBody(body, req.Key)
		if req.Key == "" {
			req.Key = key
		}

		if s.Deps.Router != nil && pinnedPartition == nil {
			if s.Deps.Router.RouteProduce(r.Context(), w, r, topicName, key, body) {
				return
			}
		}

		var (
			offset    int64
			partition int
			err       error
		)
		if pinnedPartition != nil {
			offset, partition, err = s.Deps.Broker.Produce(r.Context(), topicName, key, []byte(req.Message), *pinnedPartition)
		} else {
			offset, partition, err = s.Deps.Broker.Produce(r.Context(), topicName, key, []byte(req.Message))
		}
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, map[string]any{
			"offset":    offset,
			"partition": partition,
		})
	}
}

func parseProduceQuery(s *handlers.Set, w http.ResponseWriter, r *http.Request) (*int, bool) {
	v := r.URL.Query().Get("partition")
	if v == "" {
		return nil, true
	}
	p, err := strconv.Atoi(v)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid partition: "+err.Error())
		return nil, false
	}
	return &p, true
}

func generateProduceKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "key-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(b[:])
}

func rewriteProduceBodyKey(body []byte, key string) []byte {
	var req produceRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return body
	}
	if req.Key != "" {
		return body
	}
	req.Key = key
	updated, err := json.Marshal(req)
	if err != nil {
		return body
	}
	return updated
}

func produceKeyAndBody(body []byte, key string) (string, []byte) {
	if key != "" {
		return key, body
	}
	resolvedKey := generateProduceKey()
	return resolvedKey, rewriteProduceBodyKey(body, resolvedKey)
}
