package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// zzPerfDBatchBody is an owner's 200 reply to a forwarded batch consume
// of n records, {"messages":[...]} as the owner encodes it, with
// payloads sized so the body is at least minBytes.
func zzPerfDBatchBody(n, minBytes int) []byte {
	payload := []byte(`{"k":"` + strings.Repeat("x", minBytes/n) + `"}`)
	body := make([]byte, 0, minBytes+n*256)
	body = append(body, `{"messages":[`...)
	for i := range n {
		if i > 0 {
			body = append(body, ',')
		}
		m := topic.Message{Topic: "orders", Partition: 0, Offset: int64(i), Payload: payload, ReceiptHandle: fmt.Sprintf("0:%d:9", i)}
		body = m.AppendJSON(body)
	}
	return append(body, "]}\n"...)
}

// zzPerfDWriter is a response writer that discards the body and keeps
// the status and how many body bytes were written, reused across
// requests.
type zzPerfDWriter struct {
	h       http.Header
	status  int
	written int
}

func (w *zzPerfDWriter) Header() http.Header  { return w.h }
func (w *zzPerfDWriter) WriteHeader(code int) { w.status = code }

func (w *zzPerfDWriter) Write(p []byte) (int, error) {
	w.written += len(p)
	return len(p), nil
}

// zzPerfDForwardedConsume builds a node that owns none of "orders",
// whose owner (a fake peer) answers every consume with body, and
// returns its HTTP consume handler and a GET /consume?max=n request for
// it.
func zzPerfDForwardedConsume(tb testing.TB, body []byte, n int) (http.HandlerFunc, *http.Request) {
	tb.Helper()
	peer := fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body}, nil
	}}
	router := newZZWP18Gateway(tb, "node-remote.example:7942", peer, 2)
	set := handlers.New(handlers.Deps{
		Broker: zzWP18GatewayBroker{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router: router,
	})
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/topics/orders/consume?max=%d", n), nil)
	req.SetPathValue("topic", "orders")
	return httpmessaging.Consume(set), req
}

// BenchmarkPerfDForwardedBatchCopy is a batch consume on a node that
// owns none of the topic, forwarded to an owner whose reply is a
// 4 MiB batch: each op is one GET /consume?max=100 through the HTTP
// handler. B/op shows each copy of the owner's reply between the peer
// response and the client's writer.
func BenchmarkPerfDForwardedBatchCopy(b *testing.B) {
	const n, size = 100, 4 << 20
	body := zzPerfDBatchBody(n, size)
	if len(body) < size {
		b.Fatalf("batch body is %d bytes, want at least %d", len(body), size)
	}
	h, req := zzPerfDForwardedConsume(b, body, n)
	w := &zzPerfDWriter{h: make(http.Header)}
	h(w, req)
	if w.status != http.StatusOK || w.written != len(body) {
		b.Fatalf("forwarded batch consume = %d with %d body bytes, want 200 with the owner's %d", w.status, w.written, len(body))
	}
	b.ReportAllocs()
	for b.Loop() {
		w.status = 0
		h(w, req)
		if w.status != http.StatusOK {
			b.Fatalf("forwarded batch consume = %d, want 200", w.status)
		}
	}
}
