package messaging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
)

// BenchmarkWP22AckBatch is a batch ack of 100 handles through the
// handler with a no-op broker: the body decode is most of its cost.
func BenchmarkWP22AckBatch(b *testing.B) {
	handles := make([]string, 100)
	for i := range handles {
		handles[i] = `"` + consumer.EncodeHandle(consumer.Handle{Partition: i % 8, Offset: int64(1000 + i), Nonce: int64(7 * i)}) + `"`
	}
	body := `{"receipt_handles":[` + strings.Join(handles, ",") + `]}`
	h := Ack(newTestSet(&fakeBroker{ackFn: func(context.Context, string, consumer.Handle) error { return nil }}, nil))
	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", strings.NewReader(body))
		req.SetPathValue("topic", "orders")
		res := httptest.NewRecorder()
		h(res, req)
		if res.Code != http.StatusOK {
			b.Fatalf("status %d: %s", res.Code, res.Body)
		}
	}
}
