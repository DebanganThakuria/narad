package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP18BatchBroker answers every batch consume with max copies of msg.
type zzWP18BatchBroker struct {
	*zzWP9Broker
}

func (b zzWP18BatchBroker) ConsumeBatch(_ context.Context, _ string, _ brokermsg.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	for range max {
		dst = append(dst, b.msg)
	}
	return dst, nil, nil
}

// BenchmarkZZWP18ForwardedConsumeQUIC moves records from a remote owner
// to a node that owns none of the topic, over real QUIC: one forwarded
// probe per record (what a batch consume there cost before Max was
// forwarded), against one forwarded batch of n. ns/op is per record.
func BenchmarkZZWP18ForwardedConsumeQUIC(b *testing.B) {
	msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Payload: []byte(`{"k":"v","n":1234567}`), ReceiptHandle: "0:7:9"}
	addr, client := zzWP9QUICPeer(b, zzWP18BatchBroker{&zzWP9Broker{msg: msg, found: true}})
	router := newZZWP18Gateway(b, addr, client, 2)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			w := &zzWP9Writer{h: make(http.Header)}
			if forwarded, _ := router.RouteConsumeRemote(context.Background(), w, req, "orders"); !forwarded || w.status != http.StatusOK {
				b.Fatalf("forwarded=%v status=%d", forwarded, w.status)
			}
		}
	})
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for moved := 0; moved < b.N; moved += n {
				w := &zzWP9Writer{h: make(http.Header)}
				forwarded, batch, _ := router.RouteConsumeBatch(context.Background(), w, req, "orders", nil, n)
				if !forwarded || !batch || w.status != http.StatusOK {
					b.Fatalf("forwarded=%v batch=%v status=%d", forwarded, batch, w.status)
				}
			}
		})
	}
}
