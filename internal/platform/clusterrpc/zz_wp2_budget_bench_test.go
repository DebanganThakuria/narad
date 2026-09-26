package clusterrpc

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The forwarded-ack shape with the budget passed to the transport instead
// of a context.WithTimeout per call: compare with the "deadline" variants
// of BenchmarkZZWP2PipeAckRoundTrip and BenchmarkZZWP2QUICAck.
func BenchmarkZZWP2PipeAckRoundTripBudget(b *testing.B) {
	payload := zzWP2AckPayload(b)
	client := zzWP2PipeClient(b)
	b.ReportAllocs()
	for b.Loop() {
		frame, _, err := client.roundTrip(context.Background(), time.Now().Add(2*time.Second), clusterwire.StreamFrameNodeRequest, payload)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := nodewire.DecodeResponse(frame.Payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2QUICAckBudget(b *testing.B) {
	client, addr := zzWP2QUICClient(b)
	payload := zzWP2AckPayload(b)
	for range 4 * quicAckLanes {
		if _, err := client.RequestOnLane(context.Background(), addr, LaneAck, clusterwire.StreamFrameNodeRequest, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			frame, err := client.RequestOnLaneTimeout(context.Background(), addr, LaneAck, 2*time.Second, clusterwire.StreamFrameNodeRequest, payload)
			if err != nil {
				b.Error(err)
				return
			}
			if _, err := nodewire.DecodeResponse(frame.Payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
