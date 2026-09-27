package messaging

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP18DiscardWriter keeps the status and body length only, and reuses
// its header map, so an allocation count sees the handler alone.
type zzWP18DiscardWriter struct {
	h      http.Header
	status int
	n      int
}

func (d *zzWP18DiscardWriter) Header() http.Header { return d.h }
func (d *zzWP18DiscardWriter) WriteHeader(s int)   { d.status = s }
func (d *zzWP18DiscardWriter) Write(b []byte) (int, error) {
	d.n += len(b)
	return len(b), nil
}

var zzWP18ConsumeMsg = topic.Message{
	Topic: "orders", Partition: 3, Offset: 12345, Key: "k-1",
	Payload:       []byte(`{"a":1,"b":"hello","c":[1,2,3],"d":true,"e":null}`),
	Timestamp:     1790000000,
	ReceiptHandle: "3:12345:991234567",
}

// TestZZWP18LocalConsumeResponseAllocs pins the cost of a delivered
// record's response on the local consume path. Boxing the message into
// WriteJSON's interface moved it to the heap on every delivery, and
// Header.Set built a fresh Content-Type slice; what is left is the
// Content-Length value.
func TestZZWP18LocalConsumeResponseAllocs(t *testing.T) {
	br := &fakeBroker{
		consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
			return zzWP18ConsumeMsg, true, nil
		},
	}
	h := Consume(newTestSet(br, nil))
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	req.SetPathValue("topic", "orders")
	w := &zzWP18DiscardWriter{h: http.Header{}}
	serve := func() {
		clear(w.h)
		w.status, w.n = 0, 0
		h(w, req)
	}
	serve()
	if w.status != http.StatusOK || w.n == 0 {
		t.Fatalf("consume = %d with %d body bytes, want 200 with a body", w.status, w.n)
	}
	// The handler's own allocations: the Content-Length string and its
	// header slice. The message, its encoding (a pooled buffer) and the
	// Content-Type value cost none.
	const want = 2
	if zzWP18RaceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	if got := testing.AllocsPerRun(200, serve); got > want {
		t.Fatalf("local consume response: %.1f allocs per delivery, want <= %d", got, want)
	}
}

// TestZZWP18WriteMessageMatchesWriteJSON checks that the message writer
// sends exactly the headers and bytes the generic JSON writer sends for
// the same message, for each payload and key encoding.
func TestZZWP18WriteMessageMatchesWriteJSON(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)
	msgs := []topic.Message{
		zzWP18ConsumeMsg,
		{Topic: "t", Partition: 0, Offset: 1, Payload: []byte("plain text"), Timestamp: 2, ReceiptHandle: "0:1:2"},
		{Topic: "t", Partition: 1, Offset: 9, Key: "\xff\x00bin", Payload: []byte{0x00, 0xfe, 0x01}, Timestamp: 3},
		{Topic: "t", Partition: 2, Offset: 0, Payload: bytes.Repeat([]byte("x"), 70<<10), Timestamp: 4, ReceiptHandle: "2:0:5"},
	}
	for i := range msgs {
		legacy := httptest.NewRecorder()
		s.WriteJSON(legacy, http.StatusOK, msgs[i])
		got := httptest.NewRecorder()
		s.WriteMessage(got, http.StatusOK, &msgs[i])
		if got.Code != legacy.Code {
			t.Fatalf("message %d: status %d, want %d", i, got.Code, legacy.Code)
		}
		if !bytes.Equal(got.Body.Bytes(), legacy.Body.Bytes()) {
			t.Fatalf("message %d: body differs:\n got %q\nwant %q", i, got.Body.Bytes(), legacy.Body.Bytes())
		}
		for _, k := range []string{"Content-Type", "Content-Length"} {
			if g, w := got.Header().Values(k), legacy.Header().Values(k); len(g) != 1 || len(w) != 1 || g[0] != w[0] {
				t.Fatalf("message %d: %s = %q, want %q", i, k, g, w)
			}
		}
		if len(got.Header()) != len(legacy.Header()) {
			t.Fatalf("message %d: headers %v, want %v", i, got.Header(), legacy.Header())
		}
	}
}
