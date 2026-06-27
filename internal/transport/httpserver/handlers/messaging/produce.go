package messaging

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type produceRequest struct {
	Key     string          `json:"key,omitempty"`
	Message json.RawMessage `json:"message"`
}

var generatedProduceKeySeq atomic.Uint64

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

		body, ok := s.ReadBody(w, r, handlers.MaxJSONBodyBytes)
		if !ok {
			return
		}

		req, ok := decodeProduceRequest(s, w, body)
		if !ok {
			return
		}
		if len(req.Message) == 0 {
			s.WriteError(w, http.StatusBadRequest, "message required")
			return
		}
		key := req.Key
		if key == "" {
			key = generateProduceKey()
		}
		if req.Key == "" {
			req.Key = key
		}

		var err error
		if pinnedPartition != nil {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, []byte(req.Message), *pinnedPartition)
		} else {
			_, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, []byte(req.Message))
		}
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
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
	if p < 0 {
		s.WriteError(w, http.StatusBadRequest, "invalid partition: must be >= 0")
		return nil, false
	}
	return &p, true
}

func generateProduceKey() string {
	seq := generatedProduceKeySeq.Add(1)
	key := make([]byte, 0, 17)
	key = append(key, "key-"...)
	key = strconv.AppendUint(key, seq, 36)
	return string(key)
}
