package clusterrpc

import (
	"bufio"
	"context"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP17FrameHeaderBytes is clusterwire's frame header size. Passed as
// WriteStreamFrameStaged's maxStaged it stages nothing but the header:
// every frame goes out as two Writes, header then payload in place.
const zzWP17FrameHeaderBytes = 20

// zzWP17WriteStrategies are the two ways to put a small frame on a
// stream: staged (header and payload copied into the stream's retained
// buffer, one Write; what the transport does) and split (two Writes,
// no copy).
var zzWP17WriteStrategies = []struct {
	name      string
	maxStaged int
}{
	{"staged", maxRetainedWriteBuffer},
	{"split", zzWP17FrameHeaderBytes},
}

// zzWP17CPUTime is the process's user plus system CPU time so far. Both
// ends of the connection run in this process, so it covers both.
func zzWP17CPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// zzWP17QUICStreamPair returns both ends of one stream on a real QUIC
// connection over loopback, with the cluster's TLS and transport
// settings.
func zzWP17QUICStreamPair(b *testing.B) (client, server *quic.Stream) {
	b.Helper()
	srv, err := listenQUIC("127.0.0.1:0", "sekret", false)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := quic.DialAddr(ctx, srv.addr().String(), quicClientTLSConfig(false), quicConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.CloseWithError(0, ""); cancel(); srv.close() })
	serverConn, err := srv.listener.Accept(ctx)
	if err != nil {
		b.Fatal(err)
	}
	client, err = conn.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	// The peer learns of a stream from its first bytes.
	if err := clusterwire.WriteStreamFrame(client, clusterwire.StreamFrame{Type: clusterwire.StreamFramePing}); err != nil {
		b.Fatal(err)
	}
	server, err = serverConn.AcceptStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := clusterwire.ReadStreamFrame(server, 0); err != nil {
		b.Fatal(err)
	}
	return client, server
}

// BenchmarkZZWP17QUICFrameWrite checks the claim behind the transport's
// per-stream staging buffer: that a second Write on a QUIC stream costs
// more than copying a small frame's payload behind its header and
// writing once. It runs both over real QUIC loopback for 64 B, 1 KiB and
// 16 KiB payloads.
//
// roundtrip: the client writes a frame and waits for the server to echo
// it back, written the same way (an RPC's shape). oneway: the client
// writes frames back to back and the server drains them (a busy lane).
// cpu-ns/op is the CPU both ends spent per frame.
func BenchmarkZZWP17QUICFrameWrite(b *testing.B) {
	for _, size := range []int{64, 1 << 10, 16 << 10} {
		label := strconv.Itoa(size) + "B"
		if size >= 1<<10 {
			label = strconv.Itoa(size>>10) + "KiB"
		}
		for _, st := range zzWP17WriteStrategies {
			b.Run("roundtrip/payload="+label+"/write="+st.name, func(b *testing.B) {
				client, server := zzWP17QUICStreamPair(b)
				go func() {
					br := bufio.NewReaderSize(server, 64<<10)
					var buf []byte
					for {
						frame, err := clusterwire.ReadStreamFrame(br, 0)
						if err != nil {
							return
						}
						frame.Type = clusterwire.StreamFrameNodeReply
						if buf, _, err = clusterwire.WriteStreamFrameStaged(server, buf, frame, st.maxStaged); err != nil {
							return
						}
					}
				}()
				br := bufio.NewReaderSize(client, 64<<10)
				payload := make([]byte, size)
				var buf []byte
				var id uint64
				b.SetBytes(int64(size))
				b.ReportAllocs()
				cpu0 := zzWP17CPUTime()
				for b.Loop() {
					id++
					var err error
					buf, _, err = clusterwire.WriteStreamFrameStaged(client, buf, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: id, Payload: payload}, st.maxStaged)
					if err != nil {
						b.Fatal(err)
					}
					reply, err := clusterwire.ReadStreamFrame(br, 0)
					if err != nil || reply.RequestID != id {
						b.Fatalf("reply %d (err %v), want %d", reply.RequestID, err, id)
					}
				}
				b.ReportMetric(float64(zzWP17CPUTime()-cpu0)/float64(b.N), "cpu-ns/op")
			})
			b.Run("oneway/payload="+label+"/write="+st.name, func(b *testing.B) {
				client, server := zzWP17QUICStreamPair(b)
				drained := make(chan error, 1)
				go func() {
					br := bufio.NewReaderSize(server, 64<<10)
					for {
						frame, err := clusterwire.ReadStreamFrame(br, 0)
						if err != nil {
							drained <- err
							return
						}
						if frame.Type == clusterwire.StreamFramePing {
							drained <- nil
						}
					}
				}()
				payload := make([]byte, size)
				var buf []byte
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				cpu0 := zzWP17CPUTime()
				// A counted loop, not b.Loop: the drain below must be timed.
				for i := range b.N {
					var err error
					buf, _, err = clusterwire.WriteStreamFrameStaged(client, buf, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: uint64(i), Payload: payload}, st.maxStaged)
					if err != nil {
						b.Fatal(err)
					}
				}
				// Wait for the server to read everything written.
				if err := clusterwire.WriteStreamFrame(client, clusterwire.StreamFrame{Type: clusterwire.StreamFramePing}); err != nil {
					b.Fatal(err)
				}
				if err := <-drained; err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(zzWP17CPUTime()-cpu0)/float64(b.N), "cpu-ns/op")
			})
		}
	}
}
