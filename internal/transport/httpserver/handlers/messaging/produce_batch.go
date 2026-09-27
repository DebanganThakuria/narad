package messaging

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// MaxProduceBatch is the most messages one batch produce may carry, the
// same bound as a batch consume's max and a batch ack's handles.
const MaxProduceBatch = MaxConsumeBatch

// produceBatchRequest is the body of a batch produce.
type produceBatchRequest struct {
	Messages []produceBatchMessage `json:"messages"`
}

// produceBatchMessage is one message of a batch produce. It mirrors the
// encoding a consume returns a record in:
//
//   - payload is a JSON value, accepted as the message's bytes exactly
//     as written (a JSON string is stored with its quotes, as a single
//     produce of that body would be). With "payload_encoding":"base64"
//     it is instead a base64 string (standard alphabet, padded) of any
//     bytes, which is how a payload that is not JSON is sent.
//   - key is a string; with "key_encoding":"base64" it is base64 of any
//     bytes, for a key that is not valid UTF-8. Absent or empty is no
//     key, as for a single produce without ?key=.
//   - partition pins the message to a partition, as ?partition= does.
type produceBatchMessage struct {
	Key             string          `json:"key"`
	KeyEncoding     string          `json:"key_encoding"`
	Payload         json.RawMessage `json:"payload"`
	PayloadEncoding string          `json:"payload_encoding"`
	Partition       *int            `json:"partition"`
}

// InFlightGate takes n of the caller's in-flight budget for the rest of
// a request whose weight the handler learns only from the body (a
// batch produce weighs its message count). It answers the request
// itself (429) and reports false when the budget cannot take n more;
// otherwise release gives the n back. A nil gate admits everything.
type InFlightGate func(w http.ResponseWriter, r *http.Request, n int) (release func(), ok bool)

// ProduceBatch handles POST /v1/topics/{topic}/produce/batch: a JSON
// body {"messages":[...]} of up to MaxProduceBatch messages (see
// produceBatchMessage), accepted all or none. Every message is checked
// as a single produce checks one before any is accepted; the first that
// fails is answered with the status a single produce of it would get,
// its error prefixed with "message <index>: ". A valid batch goes into
// the ingress WAL in order, and the answer, once every message is
// durable, is 202 with {"accepted":N}.
//
// It is a path of its own, not a form of POST /produce, because a
// produce body is opaque bytes: {"messages":[...]} is a valid payload,
// and a server that predates batches must refuse a batch (404) rather
// than store it as one message. The body cap is a single produce's, and
// the permission is produce.
func ProduceBatch(s *handlers.Set, gate InFlightGate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if !s.Authorize(w, r, user.ActionProduce, topicName) {
			return
		}
		if err := checkProduceBatchQuery(r.URL.RawQuery); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		bp, ok := s.Deps.Broker.(broker.BatchProducer)
		if !ok {
			s.WriteError(w, http.StatusNotImplemented, "batch produce is not available on this node")
			return
		}

		body, ok := s.ReadBody(w, r, handlers.MaxMessageBodyBytes)
		if !ok {
			return
		}
		var req produceBatchRequest
		if !s.DecodeJSONBytes(w, body, &req) {
			return
		}
		n := len(req.Messages)
		if n == 0 {
			s.WriteError(w, http.StatusBadRequest, "messages required")
			return
		}
		if n > MaxProduceBatch {
			s.WriteError(w, http.StatusBadRequest, "too many messages: "+strconv.Itoa(n)+" (max "+strconv.Itoa(MaxProduceBatch)+")")
			return
		}
		msgs := make([]brokermsg.ProduceMessage, n)
		for i := range req.Messages {
			msg, err := req.Messages[i].decode()
			if err != nil {
				s.WriteError(w, http.StatusBadRequest, "message "+strconv.Itoa(i)+": "+err.Error())
				return
			}
			msgs[i] = msg
		}

		if gate != nil {
			release, ok := gate(w, r, n)
			if !ok {
				return
			}
			defer release()
		}
		if _, err := bp.AcceptProduceBatch(r.Context(), topicName, msgs); err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		writeProduceBatchAccepted(w, n)
	}
}

// decode checks one message as a single produce checks its key,
// partition and body, and returns it for the broker.
func (m *produceBatchMessage) decode() (brokermsg.ProduceMessage, error) {
	var out brokermsg.ProduceMessage
	switch m.KeyEncoding {
	case "":
		out.Key = m.Key
	case "base64":
		key, err := base64.StdEncoding.DecodeString(m.Key)
		if err != nil {
			return out, errors.New("invalid key: " + err.Error())
		}
		out.Key = string(key)
	default:
		return out, errors.New(`invalid key_encoding: want "base64" or none`)
	}

	switch m.PayloadEncoding {
	case "":
		out.Payload = m.Payload
	case "base64":
		var encoded string
		if err := json.Unmarshal(m.Payload, &encoded); err != nil {
			return out, errors.New("invalid payload: a base64 payload must be a JSON string")
		}
		payload, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return out, errors.New("invalid payload: " + err.Error())
		}
		out.Payload = payload
	default:
		return out, errors.New(`invalid payload_encoding: want "base64" or none`)
	}
	if len(out.Payload) == 0 {
		return out, errors.New("message required")
	}

	if m.Partition != nil {
		if *m.Partition < 0 {
			return out, errors.New("invalid partition: must be >= 0")
		}
		out.Partition = *m.Partition
		out.HasPartition = true
	}
	return out, nil
}

// checkProduceBatchQuery refuses the parameters a single produce takes:
// in a batch the key and the partition belong to each message, and one
// given for the whole batch would otherwise be silently ignored.
func checkProduceBatchQuery(raw string) error {
	for raw != "" {
		var part string
		part, raw, _ = strings.Cut(raw, "&")
		name, _, _ := strings.Cut(part, "=")
		if decoded, err := unescapeQueryComponent(name); err == nil {
			name = decoded
		}
		if name == "key" || name == "partition" {
			return errors.New(name + " is set per message in a batch produce, not as a query parameter")
		}
	}
	return nil
}

// headerJSON is the batch reply's Content-Type value, shared rather
// than built per reply by Header.Set; net/http only reads it.
var headerJSON = []string{"application/json"}

// writeProduceBatchAccepted answers 202 with {"accepted":n}.
func writeProduceBatchAccepted(w http.ResponseWriter, n int) {
	var buf [32]byte
	body := append(buf[:0], `{"accepted":`...)
	body = strconv.AppendInt(body, int64(n), 10)
	body = append(body, "}\n"...)
	h := w.Header()
	h["Content-Type"] = headerJSON
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(body)
}
