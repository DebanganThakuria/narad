package httpserver

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// BenchmarkWP22EdgeProduceGated is BenchmarkZZWP18EdgeProduce's batch
// cases with the per-identity produce cap on, so the gate's cost shows.
// ns/op is per message.
func BenchmarkWP22EdgeProduceGated(b *testing.B) {
	payload := `{"n":` + strings.Repeat("7", 92) + `}`
	h := NewRouterWithOptions(newTestSet(zzWP18NopBatchBroker{&fakeBroker{}}), newTestLogger(), nil, nil, nil,
		RouterOptions{MetricsOnAPI: true, MetricsRequireAuth: true, ProduceInFlightPerIdentity: 1000})
	for _, n := range []int{1, 10, 100} {
		msgs := make([]string, n)
		for i := range msgs {
			msgs[i] = `{"payload":` + payload + `}`
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
