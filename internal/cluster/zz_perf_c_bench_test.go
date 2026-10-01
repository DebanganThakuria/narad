package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// zzPerfCEmptyBroker is the broker of node-gw, which owns partition 0
// of "orders" and has nothing in it: every batch scan comes back empty
// with no waiter, so a batch consume there falls back on the remote
// owners.
type zzPerfCEmptyBroker struct{ broker.Broker }

func (zzPerfCEmptyBroker) ConsumeBatch(_ context.Context, _ string, _ brokermsg.ConsumeOpts, _ int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	return dst, nil, nil
}

// zzPerfCWriter is a response writer that discards the body and keeps
// the status, reused across requests.
type zzPerfCWriter struct {
	h      http.Header
	status int
}

func (w *zzPerfCWriter) Header() http.Header         { return w.h }
func (w *zzPerfCWriter) WriteHeader(code int)        { w.status = code }
func (w *zzPerfCWriter) Write(p []byte) (int, error) { return len(p), nil }

// zzPerfCLocalOwnerConsume builds node-gw, which owns partition 0 of
// "orders" (empty), and node-remote over real QUIC, which owns
// partition 1 and answers every batch consume with max copies of a
// small record and every single consume with one. It returns the HTTP
// batch consume handler of node-gw and a GET /consume?max=n request
// (no wait) for it.
func zzPerfCLocalOwnerConsume(tb testing.TB, n int) (http.HandlerFunc, *http.Request) {
	tb.Helper()
	ctx := context.Background()
	msg := topic.Message{Topic: "orders", Partition: 1, Offset: 7, Payload: []byte(`{"k":"v","n":1234567}`), ReceiptHandle: "1:7:9"}
	addr, client := zzWP9QUICPeer(tb, zzWP18BatchBroker{&zzWP9Broker{msg: msg, found: true}})
	store := zzWP9Store(tb)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		tb.Fatal(err)
	}
	for _, m := range []metastore.Member{
		{ID: "node-gw", Addr: "node-gw.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote", Addr: addr, Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			tb.Fatal(err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-gw"); err != nil {
		tb.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		tb.Fatal(err)
	}
	router := NewRouter(store, "node-gw", partition.NewHashRoundRobin(), "")
	router.peer = client
	set := handlers.New(handlers.Deps{
		Broker: zzPerfCEmptyBroker{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router: router,
	})
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/topics/orders/consume?max=%d", n), nil)
	req.SetPathValue("topic", "orders")
	return httpmessaging.Consume(set), req
}

// zzPerfCRecords serves one request and returns how many records its
// 200 {"messages":[...]} body carries.
func zzPerfCRecords(tb testing.TB, h http.HandlerFunc, req *http.Request) int {
	tb.Helper()
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		tb.Fatalf("batch consume = %d %s, want 200", rec.Code, rec.Body)
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
		tb.Fatalf("reply %q: %v", rec.Body, err)
	}
	return len(reply.Messages)
}

// BenchmarkPerfCLocalOwnerBatchFallback is a batch consume on a node
// that owns one of the topic's partitions, which is empty, while a
// remote owner over real QUIC has records: each op is one
// GET /consume?max=N with no wait through the HTTP handler.
// records/response is how many records one response carries (one
// while the fallback asks the remote owners for a single record);
// ns/record is the cost per record moved.
func BenchmarkPerfCLocalOwnerBatchFallback(b *testing.B) {
	for _, n := range []int{100} {
		b.Run(fmt.Sprintf("max=%d", n), func(b *testing.B) {
			h, req := zzPerfCLocalOwnerConsume(b, n)
			perResponse := zzPerfCRecords(b, h, req)
			w := &zzPerfCWriter{h: make(http.Header)}
			b.ReportAllocs()
			for b.Loop() {
				w.status = 0
				h(w, req)
				if w.status != http.StatusOK {
					b.Fatalf("batch consume = %d, want 200", w.status)
				}
			}
			b.ReportMetric(float64(perResponse), "records/response")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(perResponse), "ns/record")
		})
	}
}
