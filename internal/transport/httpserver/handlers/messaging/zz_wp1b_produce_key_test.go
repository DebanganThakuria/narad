package messaging

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// TestWP1BProduceKeylessPassesEmptyKey: a produce without a key must
// reach the broker with an empty key. The partitioner round-robins
// empty keys, as the client docs promise, and consumers must not
// receive a key the producer never set.
func TestWP1BProduceKeylessPassesEmptyKey(t *testing.T) {
	for _, target := range []string{
		"/v1/topics/orders/produce",
		"/v1/topics/orders/produce?key=",
		"/v1/topics/orders/produce?partition=2",
	} {
		t.Run(target, func(t *testing.T) {
			calls := 0
			var gotKey string
			s := newTestSet(&fakeBroker{acceptProduceFn: func(_ context.Context, _ string, key string, _ []byte, _ ...int) (ingress.AcceptedProduce, error) {
				calls++
				gotKey = key
				return ingress.AcceptedProduce{TargetPartition: 2, CreatedAtUnixMs: 123}, nil
			}}, nil)

			req := httptest.NewRequest(http.MethodPost, target, bytes.NewBufferString(`{"id":1}`))
			req.SetPathValue("topic", "orders")
			res := httptest.NewRecorder()
			Produce(s).ServeHTTP(res, req)

			if res.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d", res.Code, http.StatusAccepted)
			}
			if calls != 1 {
				t.Fatalf("AcceptProduce called %d times, want 1", calls)
			}
			if gotKey != "" {
				t.Fatalf("keyless produce reached the broker with key %q, want empty", gotKey)
			}
		})
	}
}
