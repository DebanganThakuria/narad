package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP12BatchBroker is the edge benchmark's no-op broker with the batch
// consume surface: every consume finds records at once.
type zzWP12BatchBroker struct {
	*fakeBroker
	msg topic.Message
}

func (b *zzWP12BatchBroker) ConsumeBatch(_ context.Context, _ string, _ brokermsg.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	for range max {
		dst = append(dst, b.msg)
	}
	return dst, nil, nil
}

// BenchmarkZZWP12LoopbackMessages moves messages through the real router
// and net/http over loopback, consume then ack, with a no-op broker: one
// consume and one ack request per message, against one consume?max=N and
// one batch ack per N messages. ns/op is per message.
func BenchmarkZZWP12LoopbackMessages(b *testing.B) {
	msg := topic.Message{
		Topic: "orders", Partition: 3, Offset: 12345,
		Payload:       []byte(`{"a":1,"b":"hello","c":[1,2,3],"d":true,"e":null}`),
		Timestamp:     1790000000,
		ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: 3, Offset: 12345, Nonce: 991234567}),
	}
	fb := &fakeBroker{
		ackFn: func(context.Context, string, consumer.Handle) error { return nil },
		consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
			return msg, true, nil
		},
	}
	br := &zzWP12BatchBroker{fakeBroker: fb, msg: msg}
	srv := httptest.NewServer(NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, RouterOptions{ConsumeInFlightPerIdentity: 1024}))
	b.Cleanup(srv.Close)
	client := srv.Client()
	do := func(method, url string, body []byte, want int) []byte {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		req.Header.Set("X-Narad-Client", "bench")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		out, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != want {
			b.Fatalf("%s %s: status %d: %s", method, url, res.StatusCode, out)
		}
		return out
	}
	base := srv.URL + "/v1/topics/orders/"

	b.Run("single", func(b *testing.B) {
		ack := base + "ack?receipt_handle=" + strings.ReplaceAll(msg.ReceiptHandle, ":", "%3A")
		b.ReportAllocs()
		for b.Loop() {
			do(http.MethodGet, base+"consume", nil, http.StatusOK)
			do(http.MethodPost, ack, nil, http.StatusNoContent)
		}
	})
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			handles := make([]string, n)
			for i := range handles {
				handles[i] = msg.ReceiptHandle
			}
			ackBody, _ := json.Marshal(map[string][]string{"receipt_handles": handles})
			consume := fmt.Sprintf("%sconsume?max=%d", base, n)
			b.ReportAllocs()
			b.ResetTimer()
			for moved := 0; moved < b.N; moved += n {
				do(http.MethodGet, consume, nil, http.StatusOK)
				do(http.MethodPost, base+"ack", ackBody, http.StatusOK)
			}
		})
	}
}
