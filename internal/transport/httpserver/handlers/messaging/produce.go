package messaging

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type produceRequest struct {
	Key     string          `json:"key,omitempty"`
	Message json.RawMessage `json:"message"`
}

type acceptedProduceResponse struct {
	Status           string `json:"status"`
	MessageID        string `json:"message_id"`
	Topic            string `json:"topic"`
	Partition        int    `json:"partition"`
	AcceptedAtUnixMs int64  `json:"accepted_at_unix_ms"`
}

func (r acceptedProduceResponse) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"status":`...)
	dst = strconv.AppendQuote(dst, r.Status)
	dst = append(dst, `,"message_id":`...)
	dst = strconv.AppendQuote(dst, r.MessageID)
	dst = append(dst, `,"topic":`...)
	dst = strconv.AppendQuote(dst, r.Topic)
	dst = append(dst, `,"partition":`...)
	dst = strconv.AppendInt(dst, int64(r.Partition), 10)
	dst = append(dst, `,"accepted_at_unix_ms":`...)
	dst = strconv.AppendInt(dst, r.AcceptedAtUnixMs, 10)
	dst = append(dst, '}')
	return dst
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

		var (
			accepted ingress.AcceptedProduce
			err      error
		)
		if pinnedPartition != nil {
			accepted, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, []byte(req.Message), *pinnedPartition)
		} else {
			accepted, err = s.Deps.Broker.AcceptProduce(r.Context(), topicName, key, []byte(req.Message))
		}
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		s.WriteJSON(w, http.StatusAccepted, newAcceptedProduceResponse(accepted))
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

func newAcceptedProduceResponse(accepted ingress.AcceptedProduce) acceptedProduceResponse {
	return acceptedProduceResponse{
		Status:           "accepted",
		MessageID:        accepted.MessageID,
		Topic:            accepted.Topic,
		Partition:        accepted.TargetPartition,
		AcceptedAtUnixMs: accepted.CreatedAtUnixMs,
	}
}
