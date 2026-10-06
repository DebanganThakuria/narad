package messaging

// The target side of remote children: the batch produce caps (Q4, Q13),
// request compression (Q14), the node's batch body budget, and a round
// trip of the replicator's encoder through this handler's own decoder.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote/sink"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// recordingBatchProducer is the handler tests' broker with the batch
// produce surface: it records every batch it is handed.
type recordingBatchProducer struct {
	*fakeBroker
	calls [][]brokermsg.ProduceMessage
	topic string
	err   error
}

func (b *recordingBatchProducer) AcceptProduceBatch(_ context.Context, topicName string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	b.calls = append(b.calls, msgs)
	b.topic = topicName
	if b.err != nil {
		return nil, b.err
	}
	return make([]ingress.AcceptedProduce, len(msgs)), nil
}

// errorBodyOf is the "error" field of a JSON error answer.
func errorBodyOf(t *testing.T, res *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &e); err != nil {
		t.Fatalf("error body %q: %v", res.Body, err)
	}
	return e.Error
}

// heapAllocatedBy is the heap one call of f allocates.
func heapAllocatedBy(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func postBatchWith(t *testing.T, h http.HandlerFunc, body []byte, encoding string, contentLength int64) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", bytes.NewReader(body))
	req.SetPathValue("topic", "orders")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	if contentLength != 0 {
		req.ContentLength = contentLength
	}
	res := httptest.NewRecorder()
	h(res, req)
	return res
}

func batchOf(n int, payload string) []byte {
	var b strings.Builder
	b.WriteString(`{"messages":[`)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"payload":` + payload + `}`)
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// Q4: a 16 MiB body carries sixteen near-1 MiB messages, so every record
// a single produce accepts fits in a batch; one message past 1 MiB gets
// the single produce's 413, with its index.
func TestRemoteBatchBodyCapIs16MiB(t *testing.T) {
	br := &recordingBatchProducer{fakeBroker: &fakeBroker{}}
	h := ProduceBatch(newTestSet(br, nil), nil)
	payload := `"` + strings.Repeat("x", int(handlers.MaxMessageBodyBytes)-64) + `"`
	body := batchOf(16, payload)
	if int64(len(body)) > MaxBatchBodyBytes {
		t.Fatalf("test body is %d bytes, over the cap", len(body))
	}
	if res := postBatchWith(t, h, body, "", 0); res.Code != http.StatusAccepted {
		t.Fatalf("16 near-1 MiB messages: status %d (%s), want 202", res.Code, res.Body)
	}

	over := `"` + strings.Repeat("y", int(handlers.MaxMessageBodyBytes)) + `"`
	body = []byte(`{"messages":[{"payload":1},{"payload":` + over + `}]}`)
	res := postBatchWith(t, h, body, "", 0)
	if res.Code != http.StatusRequestEntityTooLarge || errorBodyOf(t, res) != "message 1: message too large" {
		t.Fatalf("a message over 1 MiB: %d %q, want 413 \"message 1: message too large\"", res.Code, res.Body)
	}

	tooBig := batchOf(17, payload)
	if res := postBatchWith(t, h, tooBig, "", 0); res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("a body over 16 MiB: status %d, want 413", res.Code)
	}
}

// Q13: 1,000 messages are one batch; 1,001 are refused, and the
// replicator's 101-message probe learns the target takes 1,000.
func TestRemoteBatchTakesAThousandMessages(t *testing.T) {
	br := &recordingBatchProducer{fakeBroker: &fakeBroker{}}
	h := ProduceBatch(newTestSet(br, nil), nil)
	if res := postBatchWith(t, h, batchOf(1000, `{"a":1}`), "", 0); res.Code != http.StatusAccepted {
		t.Fatalf("1,000 messages: status %d (%s)", res.Code, res.Body)
	}
	res := postBatchWith(t, h, batchOf(1001, `{"a":1}`), "", 0)
	if res.Code != http.StatusBadRequest || !strings.HasPrefix(errorBodyOf(t, res), "too many messages: more than 1000") {
		t.Fatalf("1,001 messages: %d %q", res.Code, res.Body)
	}
	probe := []byte(`{"messages":[` + strings.TrimSuffix(strings.Repeat(`{},`, 101), ",") + `]}`)
	res = postBatchWith(t, h, probe, "", 0)
	if res.Code != http.StatusBadRequest || errorBodyOf(t, res) != "message 0: message required" {
		t.Fatalf("the capability probe: %d %q, want \"message 0: message required\"", res.Code, res.Body)
	}
	if len(br.calls) != 1 {
		t.Fatalf("broker calls = %d, want only the valid batch", len(br.calls))
	}
}

func zstdOf(t *testing.T, b []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return enc.EncodeAll(b, nil)
}

func gzipOf(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.Bytes()
}

// Q14: zstd and gzip bodies decode to the same batch; an unknown
// encoding is 415; a compressed empty batch answers as an uncompressed
// one does (the replicator's zstd probe).
func TestRemoteBatchCompressedBodies(t *testing.T) {
	plain := batchOf(10, `{"k":"v"}`)
	for _, enc := range []struct {
		name string
		body []byte
	}{{"zstd", zstdOf(t, plain)}, {"gzip", gzipOf(t, plain)}} {
		br := &recordingBatchProducer{fakeBroker: &fakeBroker{}}
		h := ProduceBatch(newTestSet(br, nil), nil)
		res := postBatchWith(t, h, enc.body, enc.name, 0)
		if res.Code != http.StatusAccepted || len(br.calls) != 1 || len(br.calls[0]) != 10 ||
			string(br.calls[0][3].Payload) != `{"k":"v"}` {
			t.Fatalf("%s: status %d (%s), calls %d", enc.name, res.Code, res.Body, len(br.calls))
		}
		res = postBatchWith(t, h, map[string][]byte{"zstd": zstdOf(t, []byte(`{"messages":[]}`)), "gzip": gzipOf(t, []byte(`{"messages":[]}`))}[enc.name], enc.name, 0)
		if res.Code != http.StatusBadRequest || errorBodyOf(t, res) != "messages required" {
			t.Fatalf("%s empty batch: %d %q, want 400 \"messages required\"", enc.name, res.Code, res.Body)
		}
		res = postBatchWith(t, h, []byte("not compressed at all"), enc.name, 0)
		if res.Code != http.StatusBadRequest || !strings.HasPrefix(errorBodyOf(t, res), "invalid json") {
			t.Fatalf("%s garbage: %d %q, want 400 invalid json", enc.name, res.Code, res.Body)
		}
	}
	h := ProduceBatch(newTestSet(&recordingBatchProducer{fakeBroker: &fakeBroker{}}, nil), nil)
	if res := postBatchWith(t, h, plain, "br", 0); res.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("an unknown encoding: status %d, want 415", res.Code)
	}
}

// A compression bomb costs no more than a plain body at the cap: the
// decoder stops at 16 MiB and answers 413.
func TestRemoteBatchCompressionBombIsCapped(t *testing.T) {
	h := ProduceBatch(newTestSet(&recordingBatchProducer{fakeBroker: &fakeBroker{}}, nil), nil)
	bomb := make([]byte, 64<<20) // 64 MiB of zeros compresses to a few KiB
	for _, enc := range []struct {
		name string
		body []byte
	}{{"zstd", zstdOf(t, bomb)}, {"gzip", gzipOf(t, bomb)}} {
		if len(enc.body) > 1<<20 {
			t.Fatalf("%s bomb is %d bytes; want a small body", enc.name, len(enc.body))
		}
		var res *httptest.ResponseRecorder
		allocated := heapAllocatedBy(func() { res = postBatchWith(t, h, enc.body, enc.name, 0) })
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s bomb: status %d (%s), want 413", enc.name, res.Code, res.Body)
		}
		if limit := uint64(3 * MaxBatchBodyBytes); allocated > limit {
			t.Fatalf("%s bomb allocated %d bytes, want at most %d", enc.name, allocated, limit)
		}
	}
}

// The node's budget for bodies above 1 MiB answers 503 with Retry-After
// when full, counts the refusal, and never touches a 1 MiB body.
func TestRemoteBatchBodyBudget(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_budget_rejections"})
	InstrumentBatchBodyBudget(counter)
	t.Cleanup(func() { InstrumentBatchBodyBudget(nil) })

	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	br := &blockingBatchProducer{recordingBatchProducer: recordingBatchProducer{fakeBroker: &fakeBroker{}}, entered: entered, release: release}
	s := handlers.New(handlers.Deps{Broker: br, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BatchBodyBudget: 4 << 20})
	h := ProduceBatch(s, nil)

	payload := `"` + strings.Repeat("z", 900<<10) + `"`
	big := batchOf(5, payload) // about 4.4 MiB: needs 3.4 MiB of the 4 MiB budget
	var wg sync.WaitGroup
	wg.Go(func() {
		if res := postBatchWith(t, h, big, "", 0); res.Code != http.StatusAccepted {
			t.Errorf("first big body: %d", res.Code)
		}
	})
	<-entered
	res := postBatchWith(t, h, big, "", 0)
	if res.Code != http.StatusServiceUnavailable || res.Header().Get("Retry-After") != "1" {
		t.Fatalf("second big body with the budget held: %d Retry-After %q, want 503 and 1", res.Code, res.Header().Get("Retry-After"))
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Fatalf("rejections counted = %v, want 1", got)
	}
	small := batchOf(1, `"`+strings.Repeat("s", 1000<<10)+`"`)
	if int64(len(small)) > handlers.MaxMessageBodyBytes {
		t.Fatalf("small body is %d bytes", len(small))
	}
	done := make(chan int, 1)
	go func() { done <- postBatchWith(t, h, small, "", 0).Code }()
	<-entered
	close(release)
	if code := <-done; code != http.StatusAccepted {
		t.Fatalf("a 1 MiB body while the budget is full: %d, want 202 (it never touches the budget)", code)
	}
	wg.Wait()
	if res := postBatchWith(t, h, big, "", 0); res.Code != http.StatusAccepted {
		t.Fatalf("a big body after the first released: %d, want 202", res.Code)
	}
}

// blockingBatchProducer holds each batch until release closes.
type blockingBatchProducer struct {
	recordingBatchProducer
	entered chan struct{}
	release chan struct{}
}

func (b *blockingBatchProducer) AcceptProduceBatch(_ context.Context, _ string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	b.entered <- struct{}{}
	<-b.release
	return make([]ingress.AcceptedProduce, len(msgs)), nil
}

// The replicator's encoder, decoded by this handler's own decoder: every
// key and payload arrives byte for byte, raw JSON included.
func TestRemoteEncoderRoundTripsThroughTheBatchDecoder(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		return b
	}
	var recs []topic.KeyedRecord
	payloads := [][]byte{
		[]byte(`{"a":1}`), []byte(`{"html":"<b>&</b>","esc":" \n\"q\""}`), []byte(`[1, 2 ,3]`),
		[]byte(` {"padded":true}`), []byte("{\"a\":1}\n"), []byte(`"plain string"`), []byte(`12.50e3`),
		[]byte(`null`), []byte("\xff\xfe binary"), randBytes(33),
		[]byte(`"` + strings.Repeat("n", 1000<<10) + `"`), randBytes(700 << 10),
	}
	keys := []string{"", "k", "ключ", "\xff\x00", string(randBytes(20)), "with \"quotes\" and \\"}
	for i := range 64 {
		recs = append(recs, topic.KeyedRecord{Key: keys[i%len(keys)], Offset: int64(i), Payload: payloads[i%len(payloads)]})
	}
	for start := 0; start < len(recs); {
		var b sink.ChunkBuilder
		body, n := b.Build(recs[start:], MaxProduceBatch, int(MaxBatchBodyBytes), nil)
		batch, err := decodeBoundedList[produceBatchMessage](body, "messages", MaxProduceBatch)
		if err != nil || len(batch) != n {
			t.Fatalf("decode chunk at %d: %v (%d of %d)", start, err, len(batch), n)
		}
		for i := range batch {
			msg, err := batch[i].decode()
			if err != nil {
				t.Fatalf("record %d: %v", start+i, err)
			}
			want := recs[start+i]
			if msg.Key != want.Key || !bytes.Equal(msg.Payload, want.Payload) || msg.HasPartition {
				t.Fatalf("record %d round trip: key %q payload %.40q, want key %q payload %.40q",
					start+i, msg.Key, msg.Payload, want.Key, want.Payload)
			}
			if sink.RawPayload(want.Payload) && !utf8.Valid(want.Payload) {
				t.Fatalf("record %d went raw with invalid UTF-8", start+i)
			}
		}
		start += n
	}
	// And the whole chunk as the handler would see it.
	var b sink.ChunkBuilder
	body, _ := b.Build(recs[:6], MaxProduceBatch, int(MaxBatchBodyBytes), nil)
	br := &recordingBatchProducer{fakeBroker: &fakeBroker{}}
	if res := postBatchWith(t, ProduceBatch(newTestSet(br, nil), nil), body, "", 0); res.Code != http.StatusAccepted {
		t.Fatalf("an encoded chunk through the handler: %d %s", res.Code, res.Body)
	}
	var check struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &check); err != nil || len(check.Messages) != 6 {
		t.Fatalf("encoded chunk is not a batch: %v", err)
	}
}

// A zstd body of several frames is charged for everything it decodes,
// not for its first frame's declared size: a tiny first frame that
// declares nothing cannot carry a 15 MiB second frame past the node's
// budget.
func TestRemoteBatchZstdFramesAfterTheFirstDrawFromTheBudget(t *testing.T) {
	s := handlers.New(handlers.Deps{Broker: &recordingBatchProducer{fakeBroker: &fakeBroker{}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BatchBodyBudget: 4 << 20})
	h := ProduceBatch(s, nil)
	// Frame 1: an empty single-segment frame that declares a content
	// size of 0 (FHD 0x20, FCS 0, then one empty last raw block).
	body := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}
	// Frame 2: a streamed frame with no declared size, decoding to
	// about 15 MiB.
	var second bytes.Buffer
	zw, err := zstd.NewWriter(&second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = zw.Write([]byte(`{"messages":[{"payload":"`))
	_, _ = zw.Write(bytes.Repeat([]byte("p"), 15<<20))
	_, _ = zw.Write([]byte(`"}]}`))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	body = append(body, second.Bytes()...)
	if int64(len(body)) > batchBudgetFree {
		t.Fatalf("test body is %d bytes; it must look small", len(body))
	}
	res := postBatchWith(t, h, body, "zstd", 0)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("a multi-frame zstd body decoding to 15 MiB with a 4 MiB budget: status %d (%s), want 503", res.Code, res.Body)
	}
}

// A body sent without Content-Length (chunked, or HTTP/2 with no
// content-length) is charged for the bytes it actually sends: a small
// one never touches the budget, even when the budget is full, and a
// large one still cannot read past it.
func TestRemoteBatchBodyWithoutLengthTakesOnlyWhatItSends(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	br := &blockingBatchProducer{recordingBatchProducer: recordingBatchProducer{fakeBroker: &fakeBroker{}}, entered: entered, release: release}
	s := handlers.New(handlers.Deps{Broker: br, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BatchBodyBudget: 4 << 20})
	h := ProduceBatch(s, nil)

	big := batchOf(5, `"`+strings.Repeat("z", 900<<10)+`"`) // takes the whole 4 MiB budget
	var wg sync.WaitGroup
	wg.Go(func() {
		if res := postBatchWith(t, h, big, "", 0); res.Code != http.StatusAccepted {
			t.Errorf("big body: %d", res.Code)
		}
	})
	<-entered
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- postBatchWith(t, h, batchOf(20, `"small"`), "", -1) }()
	select {
	case <-entered:
	case res := <-done:
		t.Fatalf("a 2 KiB body without Content-Length while the budget is full: %d (%s), want 202", res.Code, res.Body)
	}
	close(release)
	if res := <-done; res.Code != http.StatusAccepted {
		t.Fatalf("a small body without Content-Length: %d, want 202", res.Code)
	}
	wg.Wait()

	tooBig := batchOf(7, `"`+strings.Repeat("z", 900<<10)+`"`) // about 6 MiB: needs 5 MiB of 4
	if res := postBatchWith(t, h, tooBig, "", -1); res.Code != http.StatusServiceUnavailable {
		t.Fatalf("a 6 MiB body without Content-Length on a 4 MiB budget: %d, want 503", res.Code)
	}
}
