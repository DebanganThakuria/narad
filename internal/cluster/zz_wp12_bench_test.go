package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP12RPCCounter counts the peer RPCs a client issues.
type zzWP12RPCCounter struct{ n atomic.Int64 }

func (c *zzWP12RPCCounter) ObserveRPC(string, string, time.Duration) { c.n.Add(1) }

// zzWP12CPUTime is the process's user plus system CPU time so far. The
// RPC server runs in the same process, so it covers both ends.
func zzWP12CPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// BenchmarkZZWP12RouteAckQUICConc drives forwarded acks through the
// router, PeerClient and real QUIC on loopback from c goroutines at
// once, each sending its next ack as soon as the last one is answered
// (closed loop). It reports the process CPU and the peer RPCs spent per
// ack: at low concurrency every ack is its own RPC, and only when acks
// to the owner overlap can they share one.
func BenchmarkZZWP12RouteAckQUICConc(b *testing.B) {
	addr, client := zzWP9QUICPeer(b, &zzWP9Broker{})
	var rpcs zzWP12RPCCounter
	client.SetMetrics(&rpcs)
	router := zzWP9Router(b, addr, client)
	h := consumer.Handle{Partition: 0, Offset: 42, Nonce: 7}
	for _, c := range []int{1, 2, 4, 8, 16, 32, 128} {
		b.Run(fmt.Sprintf("c=%d", c), func(b *testing.B) {
			base, cancelBase := context.WithCancel(context.Background())
			defer cancelBase()
			var next atomic.Int64
			var failed atomic.Int64
			rpcs.n.Store(0)
			cpu0 := zzWP12CPUTime()
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for range c {
				wg.Go(func() {
					for next.Add(1) <= int64(b.N) {
						ctx, cancel := context.WithCancel(base)
						w := &zzWP9Writer{h: make(http.Header)}
						router.RouteAck(ctx, w, nil, "orders", h)
						cancel()
						if w.status != http.StatusNoContent {
							failed.Add(1)
						}
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			if n := failed.Load(); n > 0 {
				b.Fatalf("%d acks did not answer 204", n)
			}
			b.ReportMetric(float64(zzWP12CPUTime()-cpu0)/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(rpcs.n.Load())/float64(b.N), "rpcs/op")
		})
	}
}

// BenchmarkZZWP12ServerAckBatch is the owner's side of n acks: n OpAck
// frames against one OpAckBatch of n records. ns/op is per ack.
func BenchmarkZZWP12ServerAckBatch(b *testing.B) {
	s := NewRPCServer(&zzWP9Broker{}, nil, nil)
	done := make(chan int, 1)
	respond := func(f clusterwire.StreamFrame) { done <- len(f.Payload) }
	single, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("single", func(b *testing.B) {
		frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: single}
		b.ReportAllocs()
		for b.Loop() {
			s.HandleStreamRequest(context.Background(), frame, respond)
			<-done
		}
	})
	for _, n := range []int{2, 16, 64} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			req := nodewire.AckBatchRequest{Items: make([]nodewire.AckBatchItem, n)}
			for i := range req.Items {
				req.Items[i] = nodewire.AckBatchItem{Topic: "orders", Partition: i % 3, Offset: int64(i), Nonce: 2}
			}
			payload, err := nodewire.EncodeAckBatchRequest(req)
			if err != nil {
				b.Fatal(err)
			}
			frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}
			b.ReportAllocs()
			b.ResetTimer()
			for acked := 0; acked < b.N; acked += n {
				s.HandleStreamRequest(context.Background(), frame, respond)
				<-done
			}
		})
	}
}
