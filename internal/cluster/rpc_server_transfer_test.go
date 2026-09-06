package cluster

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// transferBroker records the chunk length the RPC layer hands to the
// storage read and, when release is set, parks every read on it.
type transferBroker struct {
	broker.Broker
	lengths  chan int64
	release  chan struct{}
	started  chan struct{}
	inFlight atomic.Int32
	maxSeen  atomic.Int32
}

func (b *transferBroker) ReadPartitionSegment(_ context.Context, _ string, _ int, _, _, length int64) ([]byte, error) {
	if b.lengths != nil {
		b.lengths <- length
	}
	if b.release != nil {
		n := b.inFlight.Add(1)
		for {
			seen := b.maxSeen.Load()
			if n <= seen || b.maxSeen.CompareAndSwap(seen, n) {
				break
			}
		}
		select {
		case b.started <- struct{}{}:
		default:
		}
		<-b.release
		b.inFlight.Add(-1)
	}
	return []byte("chunk"), nil
}

func encodeFetchChunk(t *testing.T, req nodewire.FetchSegmentChunkRequest) []byte {
	t.Helper()
	payload, err := nodewire.EncodeFetchSegmentChunkRequest(req)
	if err != nil {
		t.Fatalf("EncodeFetchSegmentChunkRequest() error = %v", err)
	}
	return payload
}

// A peer used to be able to ask for a 1 TiB chunk and have the owner
// allocate it. The wire length is clamped before it reaches storage,
// and a non-positive length or negative offset is a 400.
func TestRPCServerFetchSegmentChunkClampsWireLength(t *testing.T) {
	br := &transferBroker{lengths: make(chan int64, 4)}
	s := &RPCServer{broker: br}

	res := roundTripRPC(t, s, encodeFetchChunk(t, nodewire.FetchSegmentChunkRequest{Topic: "orders", Length: 1 << 40}))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", res.Status, res.Body)
	}
	if got := <-br.lengths; got != storage.MaxSegmentReadBytes {
		t.Fatalf("storage read length = %d, want clamp %d", got, storage.MaxSegmentReadBytes)
	}

	res = roundTripRPC(t, s, encodeFetchChunk(t, nodewire.FetchSegmentChunkRequest{Topic: "orders", Length: 1 << 20}))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", res.Status, res.Body)
	}
	if got := <-br.lengths; got != 1<<20 {
		t.Fatalf("storage read length = %d, want the requested 1 MiB", got)
	}

	for _, bad := range []nodewire.FetchSegmentChunkRequest{
		{Topic: "orders", Length: 0},
		{Topic: "orders", Length: -1},
		{Topic: "orders", At: -1, Length: 1},
	} {
		if res := roundTripRPC(t, s, encodeFetchChunk(t, bad)); res.Status != http.StatusBadRequest {
			t.Fatalf("request %+v: status = %d, want 400", bad, res.Status)
		}
	}
	if len(br.lengths) != 0 {
		t.Fatal("a rejected request reached storage")
	}
}

// Transfer ops are bounded by their own gate: extra chunk reads queue
// instead of running, while unrelated ops still get answered.
func TestRPCServerTransferGateBoundsConcurrency(t *testing.T) {
	br := &transferBroker{release: make(chan struct{}), started: make(chan struct{}, 1)}
	s := &RPCServer{broker: br}
	s.SetTransferConcurrency(1)

	payload := encodeFetchChunk(t, nodewire.FetchSegmentChunkRequest{Topic: "orders", Length: 1 << 20})
	replies := make(chan clusterwire.StreamFrame, 8)
	respond := func(f clusterwire.StreamFrame) { replies <- f }
	for i := range 3 {
		s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: uint64(i), Payload: payload}, respond)
	}
	<-br.started
	time.Sleep(50 * time.Millisecond)
	if got := br.inFlight.Load(); got != 1 {
		t.Fatalf("chunk reads executing concurrently = %d, want 1 (gate size)", got)
	}

	// An op outside the transfer gate is answered while it is held.
	s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 100, Payload: []byte{0xFE}}, respond)
	select {
	case reply := <-replies:
		if reply.RequestID != 100 {
			t.Fatalf("chunk read %d replied while the gate should hold it", reply.RequestID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ungated op did not reply while the transfer gate was saturated")
	}

	close(br.release)
	for range 3 {
		select {
		case <-replies:
		case <-time.After(2 * time.Second):
			t.Fatal("gated chunk reads did not complete after release")
		}
	}
	if br.maxSeen.Load() != 1 {
		t.Fatalf("max concurrent chunk reads = %d, want 1", br.maxSeen.Load())
	}
}

// A request cancelled while queued on a gate is answered 503 rather
// than parked until a slot frees, so a stream that closed does not keep
// a goroutine waiting.
func TestRPCServerGateHonoursCancellation(t *testing.T) {
	br := &transferBroker{release: make(chan struct{}), started: make(chan struct{}, 1)}
	s := &RPCServer{broker: br}
	s.SetTransferConcurrency(1)
	defer close(br.release)

	payload := encodeFetchChunk(t, nodewire.FetchSegmentChunkRequest{Topic: "orders", Length: 1})
	replies := make(chan clusterwire.StreamFrame, 2)
	s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}, func(f clusterwire.StreamFrame) { replies <- f })
	<-br.started

	ctx, cancel := context.WithCancel(context.Background())
	s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 2, Payload: payload}, func(f clusterwire.StreamFrame) { replies <- f })
	cancel()
	select {
	case reply := <-replies:
		res, err := nodewire.DecodeResponse(reply.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if reply.RequestID != 2 || res.Status != http.StatusServiceUnavailable {
			t.Fatalf("cancelled queued request got %+v / status %d, want 503", reply, res.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request stayed queued on the gate")
	}
}

func TestRPCServerDefaultGates(t *testing.T) {
	s := NewRPCServer(nil, nil, nil)
	if cap(s.transferSem) != defaultTransferConcurrency {
		t.Fatalf("transfer gate cap = %d, want %d", cap(s.transferSem), defaultTransferConcurrency)
	}
	if cap(s.controlSem) != defaultControlConcurrency {
		t.Fatalf("control gate cap = %d, want %d", cap(s.controlSem), defaultControlConcurrency)
	}
	s.SetTransferConcurrency(0)
	s.SetControlConcurrency(0)
	if s.transferSem != nil || s.controlSem != nil {
		t.Fatal("zero did not disable the gates")
	}
}
