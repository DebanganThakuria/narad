package messaging

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// zzWP22AllocBytes is the heap one call of f allocates.
func zzWP22AllocBytes(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// zzWP22Fill is open, then the element repeated (comma-separated) as
// many times as fits in size bytes, then closing.
func zzWP22Fill(open, elem, closing string, size int) string {
	n := (size - len(open) - len(closing)) / (len(elem) + 1)
	var b strings.Builder
	b.Grow(size)
	b.WriteString(open)
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(elem)
	}
	b.WriteString(closing)
	return b.String()
}

// A batch produce body is refused at the message past the bound, so a
// body at the cap costs a few times its size whatever it holds: before,
// the whole array was decoded before it was counted, and a 1 MiB body of
// [0,0,...] allocated about 296 MB (282 times the body), [{},{},...]
// 173 MB, and a valid batch followed by a trailing [0,0,...] 57 MB.
func TestWP22ProduceBatchDecodeIsBoundedByTheRequest(t *testing.T) {
	const size = int(handlers.MaxMessageBodyBytes)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"zeros", zzWP22Fill(`{"messages":[`, `0`, `]}`, size), "invalid json"},
		{"empty objects", zzWP22Fill(`{"messages":[`, `{}`, `]}`, size), "too many messages: more than 100"},
		{"nulls", zzWP22Fill(`{"messages":[`, `null`, `]}`, size), "too many messages: more than 100"},
		{"trailing value", zzWP22Fill(`{"messages":[{"payload":1}]}[`, `0`, `]`, size), "multiple JSON values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := &zzWP18BatchProducer{fakeBroker: &fakeBroker{}}
			h := ProduceBatch(newTestSet(br, nil), nil)
			var res *httptest.ResponseRecorder
			allocated := zzWP22AllocBytes(func() {
				res = zzWP18PostBatch(t, h, "/v1/topics/orders/produce/batch", tc.body, nil)
			})
			if res.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %.200s)", res.Code, res.Body)
			}
			if msg := zzWP18ErrorBody(t, res); !strings.Contains(msg, tc.want) {
				t.Fatalf("error = %q, want it to contain %q", msg, tc.want)
			}
			// The body copy, the decoder's buffer and at most a full batch.
			if limit := uint64(8 * len(tc.body)); allocated > limit {
				t.Fatalf("a %d-byte body allocated %d bytes (%.0fx), want at most %d", len(tc.body), allocated,
					float64(allocated)/float64(len(tc.body)), limit)
			}
			t.Logf("%d-byte body: %d bytes allocated (%.1fx)", len(tc.body), allocated, float64(allocated)/float64(len(tc.body)))
		})
	}
}

// The same bound for a batch ack: a 64 KiB body of [0,0,...] allocated
// 9.1 MB (139 times the body).
func TestWP22AckBatchDecodeIsBoundedByTheRequest(t *testing.T) {
	const size = maxAckBatchBodyBytes
	for _, tc := range []struct {
		name, body, want string
	}{
		{"zeros", zzWP22Fill(`{"receipt_handles":[`, `0`, `]}`, size), "invalid json"},
		{"strings", zzWP22Fill(`{"receipt_handles":[`, `""`, `]}`, size), "too many receipt_handles: more than 100"},
		{"trailing value", zzWP22Fill(`{"receipt_handles":["x"]}[`, `0`, `]`, size), "multiple JSON values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := Ack(newTestSet(&fakeBroker{}, nil))
			var res *httptest.ResponseRecorder
			allocated := zzWP22AllocBytes(func() {
				req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", strings.NewReader(tc.body))
				req.SetPathValue("topic", "orders")
				res = httptest.NewRecorder()
				h(res, req)
			})
			if res.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %.200s)", res.Code, res.Body)
			}
			if msg := zzWP18ErrorBody(t, res); !strings.Contains(msg, tc.want) {
				t.Fatalf("error = %q, want it to contain %q", msg, tc.want)
			}
			if limit := uint64(8 * len(tc.body)); allocated > limit {
				t.Fatalf("a %d-byte body allocated %d bytes (%.0fx), want at most %d", len(tc.body), allocated,
					float64(allocated)/float64(len(tc.body)), limit)
			}
		})
	}
}

// The streaming decode keeps the strict decode's answers: unknown
// fields at either level, a wrong type, null, a duplicate field (the
// last wins, as encoding/json does) and the field name without regard
// to case.
func TestWP22DecodeBoundedListIsStrict(t *testing.T) {
	for _, tc := range []struct {
		body    string
		want    int
		wantErr string
	}{
		{`{"messages":[{"payload":1},{"payload":2}]}`, 2, ""},
		{` {"Messages":[{"payload":1}]} `, 1, ""},
		{`{"messages":[{"payload":1}],"messages":[]}`, 0, ""},
		{`{"messages":null}`, 0, ""},
		{`null`, 0, ""},
		{`{}`, 0, ""},
		{``, 0, "EOF"},
		{`{"messages":[{"payload":1,"headers":{}}]}`, 0, "unknown field"},
		{`{"messages":[],"key":"k"}`, 0, `unknown field "key"`},
		{`{"messages":{}}`, 0, "cannot unmarshal object"},
		{`{"messages":"x"}`, 0, "cannot unmarshal string"},
		{`[{"payload":1}]`, 0, "cannot unmarshal array"},
		{`{"messages":[1]}`, 0, "cannot unmarshal number"},
		{`{"messages":[{"payload":1}]} x`, 0, "invalid character"},
		{`{"messages":[{"payload":1}]}{}`, 0, "multiple JSON values"},
		{`{"messages":[{"payload":1}`, 0, "unexpected"},
	} {
		for _, stream := range []bool{false, true} {
			got, err := decodeBoundedListPath[produceBatchMessage]([]byte(tc.body), "messages", MaxProduceBatch, stream)
			if tc.wantErr != "" {
				// The one-call path words a wrong type as encoding/json
				// does; both refuse it.
				if err == nil || (stream || !strings.Contains(tc.wantErr, "cannot unmarshal")) && !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%q (stream %v): err = %v, want one containing %q", tc.body, stream, err, tc.wantErr)
				}
				continue
			}
			if err != nil || len(got) != tc.want {
				t.Fatalf("%q (stream %v): %d messages, err %v; want %d", tc.body, stream, len(got), err, tc.want)
			}
		}
	}
	// Payloads with more commas than the bound send a valid batch down
	// the element-at-a-time path, which accepts it the same.
	wide := `{"messages":[` + strings.TrimSuffix(strings.Repeat(`{"payload":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]},`, 10), ",") + `]}`
	if got, err := decodeBoundedList[produceBatchMessage]([]byte(wide), "messages", MaxProduceBatch); err != nil || len(got) != 10 ||
		string(got[9].Payload) != `[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]` {
		t.Fatalf("a batch with comma-rich payloads: %d messages, err %v", len(got), err)
	}
	exactly := `{"messages":[` + strings.TrimSuffix(strings.Repeat(`{"payload":1},`, MaxProduceBatch), ",") + `]}`
	if got, err := decodeBoundedList[produceBatchMessage]([]byte(exactly), "messages", MaxProduceBatch); err != nil || len(got) != MaxProduceBatch {
		t.Fatalf("a full batch: %d messages, err %v; want %d", len(got), err, MaxProduceBatch)
	}
	if _, err := decodeBoundedList[produceBatchMessage]([]byte(`{"messages":[`+strings.Repeat(`{},`, MaxProduceBatch)+`{}]}`), "messages", MaxProduceBatch); err != errListTooLong {
		t.Fatalf("one past the bound: err = %v, want errListTooLong", err)
	}
}
