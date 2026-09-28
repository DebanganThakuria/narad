package clusterrpc

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2SizedHandler replies with as many bytes as an 8-byte request asks
// for (a big-endian count), from one shared buffer; any other request
// gets an empty reply.
type zzWP2SizedHandler struct{ reply []byte }

func (h zzWP2SizedHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	reply := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID}
	if len(frame.Payload) == 8 {
		reply.Payload = h.reply[:binary.BigEndian.Uint64(frame.Payload)]
	}
	go respond(reply)
	return true
}

// Real QUIC, one stream: large request frames (a commit batch, say) and
// large reply frames (a segment chunk), each size on both sides of the
// retained staging buffer's cap.
func BenchmarkZZWP2QUICLargeFrames(b *testing.B) {
	sizes := []int{16 << 10, 64 << 10, 128 << 10, 1 << 20}
	server, err := listenQUIC("127.0.0.1:0", "sekret", false)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, server.listener, listenerAuth{secret: "sekret"}, slog.New(slog.NewTextHandler(io.Discard, nil)), zzWP2SizedHandler{reply: make([]byte, 1<<20)})
	}()
	client := NewQUICFrameClient(5*time.Second, "sekret")
	b.Cleanup(func() { _ = client.Close(); cancel(); server.close(); <-done })
	addr := server.addr().String()
	call := func(b *testing.B, payload []byte) {
		if _, err := client.RequestOnLane(context.Background(), addr, LaneProduce, clusterwire.StreamFrameNodeRequest, payload); err != nil {
			b.Fatal(err)
		}
	}
	for _, size := range sizes {
		b.Run("request="+strconv.Itoa(size>>10)+"K", func(b *testing.B) {
			payload := make([]byte, size)
			for range quicProduceLanes {
				call(b, payload)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				call(b, payload)
			}
		})
	}
	for _, size := range sizes {
		b.Run("reply="+strconv.Itoa(size>>10)+"K", func(b *testing.B) {
			payload := binary.BigEndian.AppendUint64(nil, uint64(size))
			for range quicProduceLanes {
				call(b, payload)
			}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				call(b, payload)
			}
		})
	}
}
