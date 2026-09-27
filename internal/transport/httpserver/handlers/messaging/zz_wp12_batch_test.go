package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// zzWP12Msg is a reserved record of "orders" as the broker returns it.
func zzWP12Msg(partition int, offset int64) topic.Message {
	return topic.Message{
		Topic: "orders", Partition: partition, Offset: offset,
		Payload:       []byte(fmt.Sprintf(`{"n":%d}`, offset)),
		Timestamp:     1790000000,
		ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: partition, Offset: offset, Nonce: offset + 1000}),
	}
}

// zzWP12BatchCall is one ConsumeBatch call the fake saw.
type zzWP12BatchCall struct {
	opts brokermsg.ConsumeOpts
	max  int
	held int
}

// zzWP12BatchBroker is a fakeBroker with the batch-consume surface.
// batch answers the call-th ConsumeBatch (its records are appended to
// dst); wait answers ConsumeWait on the waiter an empty batch returned.
type zzWP12BatchBroker struct {
	*fakeBroker
	mu    sync.Mutex
	calls []zzWP12BatchCall
	batch func(call int, opts brokermsg.ConsumeOpts, max int) ([]topic.Message, error)
	wait  func(wait time.Duration) (topic.Message, bool, error)
}

func (b *zzWP12BatchBroker) ConsumeBatch(_ context.Context, _ string, opts brokermsg.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	b.mu.Lock()
	call := len(b.calls)
	b.calls = append(b.calls, zzWP12BatchCall{opts: opts, max: max, held: len(dst)})
	b.mu.Unlock()
	msgs, err := b.batch(call, opts, max)
	if err != nil {
		return dst, nil, err
	}
	if len(msgs) == 0 {
		return dst, &brokermsg.ConsumeWaiter{}, nil
	}
	return append(dst, msgs...), nil, nil
}

func (b *zzWP12BatchBroker) ConsumeWait(context.Context, *brokermsg.ConsumeWaiter, time.Duration, <-chan struct{}) (topic.Message, bool, bool, error) {
	if b.wait == nil {
		return topic.Message{}, false, false, nil
	}
	msg, found, err := b.wait(0)
	return msg, found, false, err
}

func (b *zzWP12BatchBroker) seen() []zzWP12BatchCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]zzWP12BatchCall(nil), b.calls...)
}

func zzWP12Consume(s *handlers.Set, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume"+target, nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Consume(s).ServeHTTP(res, req)
	return res
}

// zzWP12Envelope is the batch body for msgs, as the handler must write
// it: each record exactly as a single consume encodes it.
func zzWP12Envelope(msgs ...topic.Message) string {
	var b bytes.Buffer
	b.WriteString(`{"messages":[`)
	for i, m := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(m.AppendJSON(nil))
	}
	b.WriteString("]}\n")
	return b.String()
}

func zzWP12Records(msgs ...topic.Message) func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) {
	return func(call int, _ brokermsg.ConsumeOpts, max int) ([]topic.Message, error) {
		if call > 0 {
			return nil, nil
		}
		return msgs[:min(max, len(msgs))], nil
	}
}

// ---- consume ---------------------------------------------------------------

func TestZZWP12ConsumeBatchRejectsBadMax(t *testing.T) {
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records()}
	s := newTestSet(br, nil)
	for _, q := range []string{"?max=0", "?max=101", "?max=-3", "?max=abc", "?max=1.5", "?max=5&partition=0&offset=3"} {
		res := zzWP12Consume(s, q)
		if res.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400: %s", q, res.Code, res.Body)
		}
	}
	if res := zzWP12Consume(s, "?max=5&partition=0&offset=3"); !strings.Contains(res.Body.String(), "offset") {
		t.Fatalf("max with offset: body %s, want it to name offset", res.Body)
	}
	if calls := br.seen(); len(calls) != 0 {
		t.Fatalf("a refused request reached the broker: %+v", calls)
	}
}

// A batch is {"messages":[...]} with each record encoded exactly as a
// single consume encodes it; without max the response is today's single
// message, and the batch surface is never asked.
func TestZZWP12ConsumeBatchEnvelope(t *testing.T) {
	msgs := []topic.Message{zzWP12Msg(0, 1), zzWP12Msg(0, 2), zzWP12Msg(1, 7)}
	br := &zzWP12BatchBroker{
		fakeBroker: &fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
			return msgs[0], true, nil
		}},
		batch: zzWP12Records(msgs...),
	}
	s := newTestSet(br, nil)
	res := zzWP12Consume(s, "?max=5")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(msgs...) {
		t.Fatalf("batch = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(msgs...))
	}
	if ct := res.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var decoded struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &decoded); err != nil || len(decoded.Messages) != 3 || decoded.Messages[2].ReceiptHandle != msgs[2].ReceiptHandle {
		t.Fatalf("decoded batch = %+v, err %v", decoded, err)
	}
	if calls := br.seen(); len(calls) != 1 || calls[0].max != 5 {
		t.Fatalf("ConsumeBatch calls = %+v, want one with max 5", calls)
	}

	single := zzWP12Consume(s, "")
	if single.Code != http.StatusOK || single.Body.String() != string(msgs[0].AppendJSON(nil))+"\n" {
		t.Fatalf("single consume = %d %q, want today's one-message body", single.Code, single.Body)
	}
	if calls := br.seen(); len(calls) != 1 {
		t.Fatalf("a consume without max asked for a batch: %+v", calls)
	}
}

// Nothing reservable and a wait: the wait delivers one record, and one
// more non-blocking scan tops the batch up.
func TestZZWP12ConsumeBatchWaitTopsUp(t *testing.T) {
	a, b, c := zzWP12Msg(0, 1), zzWP12Msg(0, 2), zzWP12Msg(1, 3)
	br := &zzWP12BatchBroker{
		fakeBroker: &fakeBroker{},
		batch: func(call int, _ brokermsg.ConsumeOpts, _ int) ([]topic.Message, error) {
			if call == 0 {
				return nil, nil
			}
			return []topic.Message{b, c}, nil
		},
		wait: func(time.Duration) (topic.Message, bool, error) { return a, true, nil },
	}
	res := zzWP12Consume(newTestSet(br, nil), "?max=5&wait=1s")
	if res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(a, b, c) {
		t.Fatalf("batch = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(a, b, c))
	}
	calls := br.seen()
	if len(calls) != 2 || calls[1].max != 4 || calls[1].held != 1 || calls[1].opts.Wait != 0 {
		t.Fatalf("calls = %+v; want the top-up to ask for 4 more without waiting", calls)
	}
}

func TestZZWP12ConsumeBatchEmpty(t *testing.T) {
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records()}
	s := newTestSet(br, nil)
	if res := zzWP12Consume(s, "?max=5"); res.Code != http.StatusNoContent || res.Body.Len() != 0 {
		t.Fatalf("empty batch without wait = %d %q, want 204", res.Code, res.Body)
	}
	if res := zzWP12Consume(s, "?max=5&wait=10ms"); res.Code != http.StatusNoContent {
		t.Fatalf("empty batch after a wait = %d %q, want 204", res.Code, res.Body)
	}
}

// Broker failures map as a single consume's do.
func TestZZWP12ConsumeBatchBrokerError(t *testing.T) {
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) {
		return nil, errs.ErrTopicNotFound
	}}
	if res := zzWP12Consume(newTestSet(br, nil), "?max=5"); res.Code != http.StatusNotFound {
		t.Fatalf("unknown topic = %d %q, want 404", res.Code, res.Body)
	}
	br.batch = func(int, brokermsg.ConsumeOpts, int) ([]topic.Message, error) { return nil, errs.ErrNotPartitionOwner }
	if res := zzWP12Consume(newTestSet(br, nil), "?max=5&partition=2"); res.Code != http.StatusMisdirectedRequest {
		t.Fatalf("pinned partition owned elsewhere = %d %q, want 421 as a single consume answers", res.Code, res.Body)
	}
	if res := zzWP12Consume(newTestSet(br, nil), "?max=5"); res.Code != http.StatusNoContent {
		t.Fatalf("queue batch on a node that just lost its partitions = %d, want 204", res.Code)
	}
}

// A broker without the batch surface still answers a batch request, one
// record at a time through the single-record flow.
func TestZZWP12ConsumeBatchWithoutBatchBroker(t *testing.T) {
	m := zzWP12Msg(3, 9)
	s := newTestSet(&fakeBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return m, true, nil
	}}, nil)
	if res := zzWP12Consume(s, "?max=5"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(m) {
		t.Fatalf("batch = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(m))
	}
}

// Forwarded by the router: the one record another node delivered is a
// one-record batch; a 204 or an error goes through unchanged.
func TestZZWP12ConsumeBatchForwarded(t *testing.T) {
	m := zzWP12Msg(4, 11)
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records()}
	var reply func(w http.ResponseWriter)
	router := &fakeRouter{routeConsumeFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ *int) (bool, *int) {
		reply(w)
		return true, nil
	}}
	s := newTestSet(br, router)

	reply = func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append(m.AppendJSON(nil), '\n'))
	}
	if res := zzWP12Consume(s, "?max=5"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(m) {
		t.Fatalf("forwarded record = %d %q, want %q", res.Code, res.Body, zzWP12Envelope(m))
	}

	reply = func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }
	if res := zzWP12Consume(s, "?max=5"); res.Code != http.StatusNoContent || res.Body.Len() != 0 {
		t.Fatalf("forwarded 204 = %d %q", res.Code, res.Body)
	}

	reply = func(w http.ResponseWriter) {
		http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
	}
	res := zzWP12Consume(s, "?max=5&partition=4")
	if res.Code != http.StatusServiceUnavailable || res.Body.String() != "partition owner is down; retry later\n" ||
		!strings.HasPrefix(res.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("forwarded error = %d %q %q; want it unchanged", res.Code, res.Header().Get("Content-Type"), res.Body)
	}
	if calls := br.seen(); len(calls) != 0 {
		t.Fatalf("a forwarded batch scanned locally: %+v", calls)
	}
}

// On a node that owns some partitions: one local scan from the router's
// pick; when that is empty, the remote owners; the probe is non-blocking
// and never pinned.
func TestZZWP12ConsumeBatchLocalOwner(t *testing.T) {
	local, remote := zzWP12Msg(2, 5), zzWP12Msg(0, 6)
	pick := 2
	remoteAsked := 0
	router := &fakeRouter{
		routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
			return false, &pick
		},
		routeConsumeRemote: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) (bool, bool) {
			remoteAsked++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(append(remote.AppendJSON(nil), '\n'))
			return true, true
		},
	}
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records(local)}
	s := newTestSet(br, router)
	if res := zzWP12Consume(s, "?max=5&wait=2s"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(local) {
		t.Fatalf("local batch = %d %q", res.Code, res.Body)
	}
	calls := br.seen()
	if len(calls) != 1 || calls[0].opts.ScanStart == nil || *calls[0].opts.ScanStart != pick || calls[0].opts.Partition != nil || calls[0].opts.Wait != 0 {
		t.Fatalf("probe = %+v; want one non-blocking scan from partition %d", calls, pick)
	}
	if remoteAsked != 0 {
		t.Fatal("remote owners asked although the local scan found records")
	}

	br.batch = zzWP12Records() // local partitions empty now
	if res := zzWP12Consume(s, "?max=5&wait=2s"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(remote) {
		t.Fatalf("remote record = %d %q", res.Code, res.Body)
	}
	if remoteAsked != 1 {
		t.Fatalf("remote owners asked %d times, want once", remoteAsked)
	}
}

// A peer's local-only probe is answered from local partitions only,
// without waiting.
func TestZZWP12ConsumeBatchLocalOnlyProbe(t *testing.T) {
	m := zzWP12Msg(1, 1)
	router := &fakeRouter{routeConsumeFn: func(context.Context, http.ResponseWriter, *http.Request, string, *int) (bool, *int) {
		panic("a local-only probe was routed")
	}}
	br := &zzWP12BatchBroker{fakeBroker: &fakeBroker{}, batch: zzWP12Records(m)}
	if res := zzWP12Consume(newTestSet(br, router), "?local_only=1&max=3&wait=5s"); res.Code != http.StatusOK || res.Body.String() != zzWP12Envelope(m) {
		t.Fatalf("probe = %d %q", res.Code, res.Body)
	}
	if calls := br.seen(); len(calls) != 1 || calls[0].opts.Wait != 0 {
		t.Fatalf("probe calls = %+v, want one without a wait", calls)
	}
}

// ---- ack -------------------------------------------------------------------

func zzWP12Ack(s *handlers.Set, query, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack"+query, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Ack(s).ServeHTTP(res, req)
	return res
}

type zzWP12Result struct {
	Status int    `json:"status"`
	Error  string `json:"error"`
}

func zzWP12Results(t *testing.T, res *httptest.ResponseRecorder) []zzWP12Result {
	t.Helper()
	if res.Code != http.StatusOK {
		t.Fatalf("batch ack status = %d: %s", res.Code, res.Body)
	}
	var out struct {
		Results []zzWP12Result `json:"results"`
	}
	dec := json.NewDecoder(bytes.NewReader(res.Body.Bytes()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode %q: %v", res.Body, err)
	}
	return out.Results
}

func zzWP12HandleBody(handles ...string) string {
	b, _ := json.Marshal(map[string][]string{"receipt_handles": handles})
	return string(b)
}

func zzWP12Handle(partition int, offset, nonce int64) string {
	return consumer.EncodeHandle(consumer.Handle{Partition: partition, Offset: offset, Nonce: nonce})
}

// errorOf is the "error" of a single ack's JSON error body.
func zzWP12ErrorOf(t *testing.T, res *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode error body %q: %v", res.Body, err)
	}
	return e.Error
}

// Each handle is settled on its own and answered in request order with
// the status and message a single ack of it gets.
func TestZZWP12AckBatchLocal(t *testing.T) {
	var mu sync.Mutex
	var acked []int64
	br := &fakeBroker{ackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		mu.Lock()
		acked = append(acked, h.Nonce)
		mu.Unlock()
		if h.Nonce == 2 {
			return fmt.Errorf("%w: reservation lapsed", errs.ErrHandleStale)
		}
		return nil
	}}
	s := newTestSet(br, nil)
	good, stale, bad := zzWP12Handle(0, 1, 1), zzWP12Handle(0, 2, 2), "not-a-handle"
	results := zzWP12Results(t, zzWP12Ack(s, "", zzWP12HandleBody(good, stale, bad, zzWP12Handle(1, 3, 3))))

	singleStale := zzWP12Ack(s, "?receipt_handle="+stale, "")
	singleBad := zzWP12Ack(s, "?receipt_handle="+bad, "")
	want := []zzWP12Result{
		{Status: http.StatusNoContent},
		{Status: singleStale.Code, Error: zzWP12ErrorOf(t, singleStale)},
		{Status: singleBad.Code, Error: zzWP12ErrorOf(t, singleBad)},
		{Status: http.StatusNoContent},
	}
	if fmt.Sprint(results) != fmt.Sprint(want) {
		t.Fatalf("results = %+v, want %+v", results, want)
	}
	if want[1].Status != http.StatusGone || want[2].Status != http.StatusBadRequest {
		t.Fatalf("single outcomes %d/%d, want 410/400", want[1].Status, want[2].Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(acked[:3]) != "[1 2 3]" {
		t.Fatalf("broker acks = %v, want 1 2 3 then the singles", acked)
	}
}

func TestZZWP12AckBatchRejectsBadBodies(t *testing.T) {
	br := &fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error {
		panic("a refused batch reached the broker")
	}}
	s := newTestSet(br, nil)
	many := make([]string, MaxConsumeBatch+1)
	for i := range many {
		many[i] = zzWP12Handle(0, int64(i), 1)
	}
	for name, tc := range map[string]struct {
		query, body string
		want        int
	}{
		"empty list":       {"", `{"receipt_handles":[]}`, http.StatusBadRequest},
		"no list":          {"", `{}`, http.StatusBadRequest},
		"too many":         {"", zzWP12HandleBody(many...), http.StatusBadRequest},
		"not json":         {"", `receipt_handles=a`, http.StatusBadRequest},
		"unknown field":    {"", `{"receipt_handles":["0:1:1"],"extra":1}`, http.StatusBadRequest},
		"two values":       {"", `{"receipt_handles":["0:1:1"]}{}`, http.StatusBadRequest},
		"bad extend":       {"?extend=maybe", zzWP12HandleBody(zzWP12Handle(0, 1, 1)), http.StatusBadRequest},
		"oversize body":    {"", `{"receipt_handles":["` + strings.Repeat("x", maxAckBatchBodyBytes) + `"]}`, http.StatusRequestEntityTooLarge},
		"no handle at all": {"", "", http.StatusBadRequest},
	} {
		if res := zzWP12Ack(s, tc.query, tc.body); res.Code != tc.want {
			t.Errorf("%s: status %d, want %d: %s", name, res.Code, tc.want, res.Body)
		}
	}
}

// A receipt_handle parameter keeps today's single ack whatever the body.
func TestZZWP12AckQueryHandleWins(t *testing.T) {
	var got []int64
	br := &fakeBroker{ackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		got = append(got, h.Nonce)
		return nil
	}}
	res := zzWP12Ack(newTestSet(br, nil), "?receipt_handle="+zzWP12Handle(0, 1, 7), zzWP12HandleBody(zzWP12Handle(0, 2, 8)))
	if res.Code != http.StatusNoContent || res.Body.Len() != 0 || fmt.Sprint(got) != "[7]" {
		t.Fatalf("single ack with a body = %d %q, acked %v", res.Code, res.Body, got)
	}
}

// extend applies to every handle of a batch.
func TestZZWP12AckBatchModes(t *testing.T) {
	var nacked, extended []int64
	br := &fakeBroker{
		nackFn: func(_ context.Context, _ string, h consumer.Handle) error {
			nacked = append(nacked, h.Nonce)
			return nil
		},
		extendAckFn: func(_ context.Context, _ string, h consumer.Handle) error {
			extended = append(extended, h.Nonce)
			return nil
		},
	}
	s := newTestSet(br, nil)
	body := zzWP12HandleBody(zzWP12Handle(0, 1, 1), zzWP12Handle(0, 2, 2))
	for _, r := range zzWP12Results(t, zzWP12Ack(s, "?extend=0", body)) {
		if r.Status != http.StatusNoContent {
			t.Fatalf("nack batch result %+v", r)
		}
	}
	zzWP12Results(t, zzWP12Ack(s, "?extend=true", body))
	if fmt.Sprint(nacked) != "[1 2]" || fmt.Sprint(extended) != "[1 2]" {
		t.Fatalf("nacked %v extended %v", nacked, extended)
	}
}

// zzWP12AckRouter is a fakeRouter with the batch ack route.
type zzWP12AckRouter struct {
	*fakeRouter
	ops   []string
	route func(handles []consumer.Handle, statuses []int, msgs []string)
}

func (r *zzWP12AckRouter) RouteAckBatch(_ context.Context, _, op string, handles []consumer.Handle, statuses []int, msgs []string) {
	r.ops = append(r.ops, op)
	r.route(handles, statuses, msgs)
}

// Handles other nodes own are settled by the router; the rest locally.
func TestZZWP12AckBatchRouted(t *testing.T) {
	var local []int64
	br := &fakeBroker{nackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		local = append(local, h.Nonce)
		return nil
	}}
	router := &zzWP12AckRouter{fakeRouter: &fakeRouter{}, route: func(handles []consumer.Handle, statuses []int, msgs []string) {
		for i, h := range handles {
			switch {
			case statuses[i] != 0:
			case h.Partition == 1:
				statuses[i] = http.StatusNoContent
			case h.Partition == 2:
				statuses[i], msgs[i] = http.StatusMisdirectedRequest, "not the partition owner"
			}
		}
	}}
	s := newTestSet(br, router)
	body := zzWP12HandleBody(zzWP12Handle(0, 1, 1), zzWP12Handle(1, 2, 2), zzWP12Handle(2, 3, 3), "junk", zzWP12Handle(0, 4, 4))
	results := zzWP12Results(t, zzWP12Ack(s, "?extend=0", body))
	statuses := make([]int, len(results))
	for i, r := range results {
		statuses[i] = r.Status
	}
	if fmt.Sprint(statuses) != "[204 204 421 400 204]" || results[2].Error != "not the partition owner" {
		t.Fatalf("results = %+v", results)
	}
	if fmt.Sprint(local) != "[1 4]" || fmt.Sprint(router.ops) != "[nack]" {
		t.Fatalf("local nacks %v, router ops %v", local, router.ops)
	}
}

// A router without the batch route: each handle is routed as a single
// ack would be, its outcome captured.
func TestZZWP12AckBatchSingleRouteFallback(t *testing.T) {
	var local []int64
	br := &fakeBroker{ackFn: func(_ context.Context, _ string, h consumer.Handle) error {
		local = append(local, h.Nonce)
		return nil
	}}
	router := &fakeRouter{routeAckFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, h consumer.Handle) bool {
		switch h.Partition {
		case 1:
			w.WriteHeader(http.StatusNoContent)
			return true
		case 2:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGone)
			_, _ = w.Write([]byte(`{"error":"receipt handle is stale"}` + "\n"))
			return true
		case 3:
			http.Error(w, "partition owner is down; retry later", http.StatusServiceUnavailable)
			return true
		}
		return false
	}}
	s := newTestSet(br, router)
	body := zzWP12HandleBody(zzWP12Handle(0, 1, 1), zzWP12Handle(1, 2, 2), zzWP12Handle(2, 3, 3), zzWP12Handle(3, 4, 4))
	results := zzWP12Results(t, zzWP12Ack(s, "", body))
	want := []zzWP12Result{
		{Status: http.StatusNoContent},
		{Status: http.StatusNoContent},
		{Status: http.StatusGone, Error: "receipt handle is stale"},
		{Status: http.StatusServiceUnavailable, Error: "partition owner is down; retry later"},
	}
	if fmt.Sprint(results) != fmt.Sprint(want) {
		t.Fatalf("results = %+v, want %+v", results, want)
	}
	if fmt.Sprint(local) != "[1]" {
		t.Fatalf("local acks = %v, want only the local handle", local)
	}
}
