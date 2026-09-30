package clusterrpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP2AckHandler answers like RPCServer.HandleStreamRequest does for an
// ack: decode the request on a goroutine, encode a bodiless Response.
type zzWP2AckHandler struct{}

func (zzWP2AckHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (zzWP2AckHandler) HandleStreamRequest(_ context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	go func() {
		status := 204
		if _, err := nodewire.DecodeAckRequest(frame.Payload); err != nil {
			status = 400
		}
		payload, _ := nodewire.EncodeResponse(nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON})
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: payload})
	}()
	return true
}

func zzWP2AckPayload(b *testing.B) []byte {
	b.Helper()
	payload, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 3, Offset: 12345, Nonce: 99})
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

// zzWP2PipeClient serves one net.Pipe end the way the QUIC listener
// serves a stream (the raw stream is the reader) and returns a stream
// client on the other end.
func zzWP2PipeClient(b *testing.B) *streamClient {
	b.Helper()
	clientConn, serverConn := net.Pipe()
	b.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	go serveStreamConn(serverConn, serverConn, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), zzWP2AckHandler{})
	client := newStreamClient(clientConn, 30*time.Second)
	go client.readLoop()
	return client
}

// One ack-shaped round trip over a pipe: client write, server read and
// dispatch, reply encode and write, client read and decode. deadline
// mirrors today's forwarded ack (a 2s context per call); nodeadline takes
// the fallback timer path.
func BenchmarkZZWP2PipeAckRoundTrip(b *testing.B) {
	payload := zzWP2AckPayload(b)
	b.Run("deadline", func(b *testing.B) {
		client := zzWP2PipeClient(b)
		b.ReportAllocs()
		for b.Loop() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			frame, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, payload)
			cancel()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := nodewire.DecodeResponse(frame.Payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("nodeadline", func(b *testing.B) {
		client := zzWP2PipeClient(b)
		b.ReportAllocs()
		for b.Loop() {
			frame, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, payload)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := nodewire.DecodeResponse(frame.Payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		client := zzWP2PipeClient(b)
		b.ReportAllocs()
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, payload)
				cancel()
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func zzWP2QUICClient(b *testing.B) (*QUICFrameClient, string) {
	b.Helper()
	server, err := listenQUIC("127.0.0.1:0", "sekret", false)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, server.listener, listenerAuth{secret: "sekret"}, slog.New(slog.NewTextHandler(io.Discard, nil)), zzWP2AckHandler{})
	}()
	client := NewQUICFrameClient(5*time.Second, "sekret")
	b.Cleanup(func() { _ = client.Close(); cancel(); server.close(); <-done })
	return client, server.addr().String()
}

// Real QUIC with auth: parallel ack-lane round trips through the pool,
// each with a 2s context as forwarded acks carry today. Every lane shard
// is opened before timing starts.
func BenchmarkZZWP2QUICAck(b *testing.B) {
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			frame, err := client.RequestOnLane(ctx, addr, LaneAck, clusterwire.StreamFrameNodeRequest, payload)
			cancel()
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

// Real QUIC, produce lane, a 64 KiB request per call (a commit batch):
// the staging-buffer path for larger frames.
func BenchmarkZZWP2QUICProduce64K(b *testing.B) {
	client, addr := zzWP2QUICClient(b)
	payload := make([]byte, 64<<10)
	for range 4 * quicProduceLanes {
		if _, err := client.RequestOnLane(context.Background(), addr, LaneProduce, clusterwire.StreamFrameNodeRequest, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := client.RequestOnLane(ctx, addr, LaneProduce, clusterwire.StreamFrameNodeRequest, payload)
			cancel()
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}
