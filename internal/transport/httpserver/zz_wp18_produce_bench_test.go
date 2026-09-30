package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
)

// zzWP18WALBroker is the edge benchmark's broker with a real ingress
// WAL behind produce: the accept is the durable append a real node
// makes (and waits for its fsync), without the topic lookup, schema
// check and partition pick, which cost the same per message whether it
// arrives alone or in a batch.
type zzWP18WALBroker struct {
	*fakeBroker
	wal *ingress.Manager
}

func (b *zzWP18WALBroker) AcceptProduce(ctx context.Context, topicName, key string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
	return b.wal.AcceptProduceWithTopicID(ctx, topicName, "id-1", key, 0, payload)
}

func (b *zzWP18WALBroker) AcceptProduceBatch(ctx context.Context, topicName string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	records := make([]ingress.BatchRecord, len(msgs))
	for i, m := range msgs {
		records[i] = ingress.BatchRecord{Key: m.Key, Payload: m.Payload}
	}
	return b.wal.AcceptProduceBatch(ctx, topicName, "id-1", records)
}

func zzWP18WALRouter(b *testing.B) (http.Handler, *zzWP18WALBroker) {
	b.Helper()
	m, err := ingress.OpenManager(b.TempDir(), ingress.DefaultWALOptions())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = m.Close() })
	br := &zzWP18WALBroker{fakeBroker: &fakeBroker{}, wal: m}
	return NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, DefaultRouterOptions()), br
}

// zzWP18Serve runs one request through h and fails the benchmark on an
// unexpected status.
func zzWP18Serve(b *testing.B, h http.Handler, target string, body []byte, want int) {
	req, err := http.NewRequest(http.MethodPost, "http://x"+target, bytes.NewReader(body))
	if err != nil {
		b.Fatal(err)
	}
	req.Header.Set("X-Narad-Client", "bench")
	req.Header.Set("Content-Type", "application/octet-stream")
	rw := &wp1bDiscardRW{h: http.Header{}}
	h.ServeHTTP(rw, req)
	if rw.status != want {
		b.Fatalf("POST %s: status %d, want %d", target, rw.status, want)
	}
}

// BenchmarkZZWP18ProduceWAL produces 100-byte messages through the real
// router into a real ingress WAL, one request per message (single) or
// n per batch request (batch=n), from one client (serial) and from many
// at once (parallel). ns/op is per message.
func BenchmarkZZWP18ProduceWAL(b *testing.B) {
	payload := []byte(`{"n":` + strings.Repeat("7", 92) + `}`)
	b.Run("single/serial", func(b *testing.B) {
		h, _ := zzWP18WALRouter(b)
		b.ReportAllocs()
		for b.Loop() {
			zzWP18Serve(b, h, "/v1/topics/orders/produce", payload, http.StatusAccepted)
		}
	})
	b.Run("single/parallel", func(b *testing.B) {
		h, _ := zzWP18WALRouter(b)
		b.ReportAllocs()
		b.SetParallelism(16)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				zzWP18Serve(b, h, "/v1/topics/orders/produce", payload, http.StatusAccepted)
			}
		})
	})
	for _, n := range []int{1, 10, 100} {
		msgs := make([]string, n)
		for i := range msgs {
			msgs[i] = `{"payload":` + string(payload) + `}`
		}
		body := []byte(`{"messages":[` + strings.Join(msgs, ",") + `]}`)
		b.Run(fmt.Sprintf("batch=%d/serial", n), func(b *testing.B) {
			h, _ := zzWP18WALRouter(b)
			b.ReportAllocs()
			for moved := 0; moved < b.N; moved += n {
				zzWP18Serve(b, h, "/v1/topics/orders/produce/batch", body, http.StatusAccepted)
			}
		})
		b.Run(fmt.Sprintf("batch=%d/parallel", n), func(b *testing.B) {
			h, _ := zzWP18WALRouter(b)
			b.ReportAllocs()
			b.SetParallelism(16)
			var moved atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					// pb.Next counts one message; a request moves n, so
					// only every nth iteration sends one.
					if moved.Add(1)%int64(n) == 0 {
						zzWP18Serve(b, h, "/v1/topics/orders/produce/batch", body, http.StatusAccepted)
					}
				}
			})
		})
	}
}

// zzWP18NopBatchBroker accepts batches without doing anything, so the
// edge benchmark measures the router, middleware and handler alone.
type zzWP18NopBatchBroker struct{ *fakeBroker }

func (zzWP18NopBatchBroker) AcceptProduceBatch(_ context.Context, _ string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	return make([]ingress.AcceptedProduce, len(msgs)), nil
}

// BenchmarkZZWP18EdgeProduce is the CPU cost of produce at the edge (the
// real router and middleware, a no-op broker): one request per message
// against one batch request per n. ns/op is per message.
func BenchmarkZZWP18EdgeProduce(b *testing.B) {
	payload := []byte(`{"n":` + strings.Repeat("7", 92) + `}`)
	h := NewRouterWithOptions(newTestSet(zzWP18NopBatchBroker{&fakeBroker{}}), newTestLogger(), nil, nil, nil, DefaultRouterOptions())
	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			zzWP18Serve(b, h, "/v1/topics/orders/produce", payload, http.StatusAccepted)
		}
	})
	for _, n := range []int{1, 10, 100} {
		msgs := make([]string, n)
		for i := range msgs {
			msgs[i] = `{"payload":` + string(payload) + `}`
		}
		body := []byte(`{"messages":[` + strings.Join(msgs, ",") + `]}`)
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for moved := 0; moved < b.N; moved += n {
				zzWP18Serve(b, h, "/v1/topics/orders/produce/batch", body, http.StatusAccepted)
			}
		})
	}
}
