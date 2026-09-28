package messaging

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// zzWP18BatchProducer is the handler tests' broker with the batch
// produce surface: it records every batch it is handed.
type zzWP18BatchProducer struct {
	*fakeBroker
	calls [][]brokermsg.ProduceMessage
	topic string
	err   error
}

func (b *zzWP18BatchProducer) AcceptProduceBatch(_ context.Context, topicName string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	b.calls = append(b.calls, msgs)
	b.topic = topicName
	if b.err != nil {
		return nil, b.err
	}
	return make([]ingress.AcceptedProduce, len(msgs)), nil
}

func zzWP18PostBatch(t *testing.T, h http.HandlerFunc, target, body string, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.SetPathValue("topic", "orders")
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	res := httptest.NewRecorder()
	h(res, req)
	return res
}

func zzWP18ErrorBody(t *testing.T, res *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &e); err != nil {
		t.Fatalf("error body %q: %v", res.Body, err)
	}
	return e.Error
}

// TestZZWP18ProduceBatchAccepts checks a valid batch reaches the broker
// as one call, each message decoded exactly: a JSON payload verbatim
// (whitespace and all), a base64 payload and key as their bytes, a
// pinned partition, and a keyless message.
func TestZZWP18ProduceBatchAccepts(t *testing.T) {
	br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
	h := ProduceBatch(newTestSet(br, nil), nil)
	binKey := string([]byte{0xff, 0x00, 'k'})
	binPayload := []byte{0x00, 0xfe, 0x01, 0x02}
	body := `{"messages":[
		{"key":"c-1","payload":{"a": 1, "b":[true,null]}},
		{"key":"` + base64.StdEncoding.EncodeToString([]byte(binKey)) + `","key_encoding":"base64","payload":"` +
		base64.StdEncoding.EncodeToString(binPayload) + `","payload_encoding":"base64","partition":2},
		{"payload":"text"},
		{"key":"","payload":42}
	]}`
	res := zzWP18PostBatch(t, h, "/v1/topics/orders/produce/batch", body, nil)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", res.Code, res.Body)
	}
	if got := res.Body.String(); got != "{\"accepted\":4}\n" {
		t.Fatalf("body = %q, want {\"accepted\":4}", got)
	}
	if ct := res.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if len(br.calls) != 1 || br.topic != "orders" {
		t.Fatalf("broker calls = %d on %q, want one on orders", len(br.calls), br.topic)
	}
	want := []brokermsg.ProduceMessage{
		{Key: "c-1", Payload: []byte(`{"a": 1, "b":[true,null]}`)},
		{Key: binKey, Payload: binPayload, Partition: 2, HasPartition: true},
		{Payload: []byte(`"text"`)},
		{Payload: []byte(`42`)},
	}
	got := br.calls[0]
	if len(got) != len(want) {
		t.Fatalf("broker got %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || !bytes.Equal(got[i].Payload, want[i].Payload) ||
			got[i].Partition != want[i].Partition || got[i].HasPartition != want[i].HasPartition {
			t.Fatalf("message %d = %+v (payload %q), want %+v (payload %q)", i, got[i], got[i].Payload, want[i], want[i].Payload)
		}
	}
}

// TestZZWP18ProduceBatchRefusesInvalid checks every malformed batch is
// refused with 400 before the broker sees any of it, naming the first
// bad message where there is one.
func TestZZWP18ProduceBatchRefusesInvalid(t *testing.T) {
	ok := `{"payload":{"a":1}}`
	many := make([]string, MaxProduceBatch+1)
	for i := range many {
		many[i] = ok
	}
	for _, tc := range []struct {
		name, target, body, want string
	}{
		{"missing payload", "", `{"messages":[` + ok + `,{"key":"k"}]}`, "message 1: message required"},
		{"empty base64 payload", "", `{"messages":[{"payload":"","payload_encoding":"base64"}]}`, "message 0: message required"},
		{"null base64 payload", "", `{"messages":[{"payload":null,"payload_encoding":"base64"}]}`, "message 0: message required"},
		{"bad payload encoding", "", `{"messages":[` + ok + `,` + ok + `,{"payload":"x","payload_encoding":"hex"}]}`, `message 2: invalid payload_encoding`},
		{"base64 payload not a string", "", `{"messages":[{"payload":{"a":1},"payload_encoding":"base64"}]}`, "message 0: invalid payload: a base64 payload must be a JSON string"},
		{"bad base64 payload", "", `{"messages":[{"payload":"%%%","payload_encoding":"base64"}]}`, "message 0: invalid payload"},
		{"bad key encoding", "", `{"messages":[{"key":"k","key_encoding":"utf16","payload":1}]}`, "message 0: invalid key_encoding"},
		{"bad base64 key", "", `{"messages":[{"key":"!!","key_encoding":"base64","payload":1}]}`, "message 0: invalid key"},
		{"negative partition", "", `{"messages":[` + ok + `,{"payload":1,"partition":-1}]}`, "message 1: invalid partition: must be >= 0"},
		{"unknown field", "", `{"messages":[{"payload":1,"headers":{}}]}`, "invalid json"},
		{"unknown top-level field", "", `{"messages":[` + ok + `],"key":"k"}`, "invalid json"},
		{"not json", "", `{"messages":[`, "invalid json"},
		{"no messages", "", `{"messages":[]}`, "messages required"},
		{"messages absent", "", `{}`, "messages required"},
		{"too many", "", `{"messages":[` + strings.Join(many, ",") + `]}`, fmt.Sprintf("too many messages: more than %d (max %d)", MaxProduceBatch, MaxProduceBatch)},
		{"key parameter", "?key=k", `{"messages":[` + ok + `]}`, "key is set per message"},
		{"empty key parameter", "?key=", `{"messages":[` + ok + `]}`, "key is set per message"},
		{"partition parameter", "?partition=1", `{"messages":[` + ok + `]}`, "partition is set per message"},
	} {
		br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
		h := ProduceBatch(newTestSet(br, nil), nil)
		res := zzWP18PostBatch(t, h, "/v1/topics/orders/produce/batch"+tc.target, tc.body, nil)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400 (body %s)", tc.name, res.Code, res.Body)
		}
		if msg := zzWP18ErrorBody(t, res); !strings.Contains(msg, tc.want) {
			t.Fatalf("%s: error = %q, want it to contain %q", tc.name, msg, tc.want)
		}
		if len(br.calls) != 0 {
			t.Fatalf("%s: the broker was handed a refused batch", tc.name)
		}
	}
}

// TestZZWP18ProduceBatchBodyCap checks a batch body is held to a single
// produce's cap.
func TestZZWP18ProduceBatchBodyCap(t *testing.T) {
	br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
	h := ProduceBatch(newTestSet(br, nil), nil)
	big := strings.Repeat("x", 1<<20)
	res := zzWP18PostBatch(t, h, "/v1/topics/orders/produce/batch", `{"messages":[{"payload":"`+big+`"}]}`, nil)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", res.Code)
	}
	if len(br.calls) != 0 {
		t.Fatal("the broker was handed an oversized batch")
	}
}

// TestZZWP18ProduceBatchBrokerErrors checks a broker refusal is answered
// with the status a single produce would get for it, message intact.
func TestZZWP18ProduceBatchBrokerErrors(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		msg    string
	}{
		{fmt.Errorf("message 3: %w", fmt.Errorf("%w: schema validation failed", errs.ErrInvalidArgument)), http.StatusBadRequest, "message 3: invalid argument: schema validation failed"},
		{errs.ErrTopicNotFound, http.StatusNotFound, "topic not found"},
		{errs.ErrDelayedChildProduce, http.StatusConflict, ""},
		{errors.New("disk on fire"), http.StatusInternalServerError, "produce failed"},
	} {
		br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}, err: tc.err}
		h := ProduceBatch(newTestSet(br, nil), nil)
		res := zzWP18PostBatch(t, h, "/v1/topics/orders/produce/batch", `{"messages":[{"payload":1}]}`, nil)
		if res.Code != tc.status {
			t.Fatalf("%v: status = %d, want %d", tc.err, res.Code, tc.status)
		}
		if msg := zzWP18ErrorBody(t, res); tc.msg != "" && msg != tc.msg {
			t.Fatalf("%v: error = %q, want %q", tc.err, msg, tc.msg)
		}
	}
}

// TestZZWP18ProduceBatchNeedsBatchBroker checks a broker without the
// batch surface refuses the batch rather than accepting it piecemeal.
func TestZZWP18ProduceBatchNeedsBatchBroker(t *testing.T) {
	calls := 0
	fb := &fakeBroker{acceptProduceFn: func(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
		calls++
		return ingress.AcceptedProduce{}, nil
	}}
	res := zzWP18PostBatch(t, ProduceBatch(newTestSet(fb, nil), nil), "/v1/topics/orders/produce/batch", `{"messages":[{"payload":1}]}`, nil)
	if res.Code != http.StatusNotImplemented || calls != 0 {
		t.Fatalf("status = %d with %d single accepts, want 501 and none", res.Code, calls)
	}
}

// TestZZWP18ProduceBatchAuthorization checks a batch needs the produce
// grant on its topic, exactly as a single produce does.
func TestZZWP18ProduceBatchAuthorization(t *testing.T) {
	body := `{"messages":[{"payload":1}]}`
	for _, tc := range []struct {
		name   string
		grants []user.Grant
		want   int
	}{
		{"produce granted", []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders"}}}, http.StatusAccepted},
		{"consume only", []user.Grant{{Action: user.ActionConsume, Patterns: []string{"orders"}}}, http.StatusForbidden},
		{"other topic", []user.Grant{{Action: user.ActionProduce, Patterns: []string{"logs"}}}, http.StatusForbidden},
	} {
		br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
		ctx := security.WithIdentity(context.Background(), user.User{Username: "svc", Grants: tc.grants})
		res := zzWP18PostBatch(t, ProduceBatch(newTestSet(br, nil), nil), "/v1/topics/orders/produce/batch", body, ctx)
		if res.Code != tc.want {
			t.Fatalf("%s: status = %d, want %d", tc.name, res.Code, tc.want)
		}
		if tc.want != http.StatusAccepted && len(br.calls) != 0 {
			t.Fatalf("%s: an unauthorized batch reached the broker", tc.name)
		}
	}
}

// zzWP18Hold records what the handler does with a gate's hold.
type zzWP18Hold struct {
	raised   []int
	released int
	refuse   bool
}

func (h *zzWP18Hold) Raise(w http.ResponseWriter, n int) bool {
	h.raised = append(h.raised, n)
	if h.refuse {
		w.WriteHeader(http.StatusTooManyRequests)
		return false
	}
	return true
}

func (h *zzWP18Hold) Release() { h.released++ }

// zzWP18ReadTracker is a request body that records its first read.
type zzWP18ReadTracker struct {
	io.Reader
	read bool
}

func (r *zzWP18ReadTracker) Read(p []byte) (int, error) {
	r.read = true
	return r.Reader.Read(p)
}

func (r *zzWP18ReadTracker) Close() error { return nil }

// TestZZWP18ProduceBatchGateWeighsMessages checks the in-flight gate is
// taken before the body is read and raised to the batch's message count
// once it is decoded, that the hold is released whether or not the
// raise succeeds, and that a refusal keeps the batch from the broker.
func TestZZWP18ProduceBatchGateWeighsMessages(t *testing.T) {
	var holds []*zzWP18Hold
	var body *zzWP18ReadTracker
	refuseRaise := false
	gate := func(_ http.ResponseWriter, _ *http.Request) (InFlightHold, bool) {
		if body.read {
			t.Error("the gate was taken after the body was read")
		}
		h := &zzWP18Hold{refuse: refuseRaise}
		holds = append(holds, h)
		return h, true
	}
	br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
	h := ProduceBatch(newTestSet(br, nil), gate)
	post := func() int {
		body = &zzWP18ReadTracker{Reader: strings.NewReader(`{"messages":[{"payload":1},{"payload":2},{"payload":3}]}`)}
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", body)
		req.SetPathValue("topic", "orders")
		res := httptest.NewRecorder()
		h(res, req)
		return res.Code
	}
	if code := post(); code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", code)
	}
	refuseRaise = true
	if code := post(); code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", code)
	}
	if len(holds) != 2 || len(br.calls) != 1 {
		t.Fatalf("holds %d, broker calls %d; want 2, 1", len(holds), len(br.calls))
	}
	for i, h := range holds {
		if len(h.raised) != 1 || h.raised[0] != 3 || h.released != 1 {
			t.Fatalf("hold %d: raised %v, released %d times; want [3] and once", i, h.raised, h.released)
		}
	}
}
