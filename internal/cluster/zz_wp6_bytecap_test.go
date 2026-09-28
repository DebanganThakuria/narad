package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP6NumberedBacklog accepts n records of size bytes spread
// round-robin over parts from many goroutines (so the WAL group-commits
// them in large groups). Each payload starts with its index; the
// returned slice maps an index to the WAL seq it was accepted at.
func zzWP6NumberedBacklog(t *testing.T, m *ingress.Manager, n, size int, parts ...int) []uint64 {
	t.Helper()
	const workers = 128
	seqs := make([]uint64, n)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			for i := w; i < n; i += workers {
				payload := bytes.Repeat([]byte("x"), size)
				binary.BigEndian.PutUint64(payload, uint64(i))
				res, err := m.AcceptProduce(context.Background(), "orders", "k", parts[i%len(parts)], payload)
				if err != nil {
					errs <- err
					return
				}
				seqs[i] = res.WAL.Seq
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return seqs
}

// zzWP6FramePeer stands in for remote owners behind the real stream
// client: it encodes each batch as PeerClient.CommitProduceBatch does
// and refuses one whose frame payload is over
// clusterwire.MaxStreamFramePayloadBytes, as the stream client does
// before sending. It checks that each partition gets its records in WAL
// order, once each.
type zzWP6FramePeer struct {
	seqs []uint64

	mu        sync.Mutex
	lastSeq   map[int]uint64
	seen      map[uint64]bool
	batches   []int
	maxFrame  int
	refused   int
	problems  []string
	committed atomic.Int64
}

func (p *zzWP6FramePeer) client() fakePeerClient {
	return fakePeerClient{commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		enc, err := nodewire.EncodeCommitProduceBatchRequest(req)
		if err != nil {
			return nodewire.Response{}, err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.maxFrame = max(p.maxFrame, len(enc))
		if len(enc) > clusterwire.MaxStreamFramePayloadBytes {
			p.refused++
			return nodewire.Response{}, fmt.Errorf("stream frame payload too large: %d bytes", len(enc))
		}
		p.batches = append(p.batches, len(req.Records))
		for _, r := range req.Records {
			seq := p.seqs[binary.BigEndian.Uint64(r.Payload)]
			if p.seen[seq] {
				p.problems = append(p.problems, fmt.Sprintf("seq %d committed twice", seq))
			}
			if last, ok := p.lastSeq[r.TargetPartition]; ok && seq <= last {
				p.problems = append(p.problems, fmt.Sprintf("partition %d got seq %d after %d", r.TargetPartition, seq, last))
			}
			p.seen[seq] = true
			p.lastSeq[r.TargetPartition] = seq
		}
		p.committed.Add(int64(len(req.Records)))
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
}

// produce-dispatch review, remote batch bytes: a destination's commit
// was sized by record count alone, so a backlog of records of a few KiB
// each made one remote commit larger than the stream frame limit. The
// stream client refuses such a frame before sending it; the destination
// turned failing, its one-record probe then succeeded and cleared the
// failure well inside the reroute grace, and the next batch was the same
// oversized one, so the destination never drained. Each remote commit
// must fit in a frame, in order, and the backlog must drain.
func TestZZWP6RemoteBatchFitsStreamFrame(t *testing.T) {
	for _, tc := range []struct {
		name    string
		parts   []int // of a 3-partition topic: 0 local, 1 and 2 remote
		payload int
		n       int
	}{
		{"1remote-5KiB", []int{1}, 5 << 10, 4000},
		{"2remote-6KiB", []int{1, 2}, 6 << 10, 6000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := zzWP6NewStore(t)
			zzWP6SeedSpread(t, store, 3)
			m := zzWP6Manager(t)
			peer := &zzWP6FramePeer{
				seqs:    zzWP6NumberedBacklog(t, m, tc.n, tc.payload, tc.parts...),
				lastSeq: map[int]uint64{},
				seen:    map[uint64]bool{},
			}
			d := NewProduceDispatcher(m, store, "node-self", &fakeProduceCommitter{}, peer.client(), nil, ProduceDispatcherConfig{})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { d.Run(ctx); close(done) }()
			deadline := time.Now().Add(10 * time.Second)
			for peer.committed.Load() < int64(tc.n) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			<-done

			peer.mu.Lock()
			defer peer.mu.Unlock()
			for _, p := range peer.problems {
				t.Error(p)
			}
			if peer.refused > 0 {
				t.Errorf("%d batches refused, largest frame %d bytes over the %d-byte limit",
					peer.refused, peer.maxFrame, clusterwire.MaxStreamFramePayloadBytes)
			}
			if got := peer.committed.Load(); got != int64(tc.n) {
				t.Fatalf("committed %d of %d records in 10s", got, tc.n)
			}
			if peer.maxFrame > produceRemoteBatchBytes {
				t.Fatalf("largest frame %d bytes, over the %d-byte remote batch budget", peer.maxFrame, produceRemoteBatchBytes)
			}
		})
	}
}

// The byte bound must not cost small records their batching: a remote
// hot destination of 256-byte records still carries a whole base window
// per commit while a backlog remains.
func TestZZWP6RemoteHotDestinationCommitsABaseWindow(t *testing.T) {
	store := zzWP6NewStore(t)
	zzWP6SeedSpread(t, store, 3)
	m := zzWP6Manager(t)
	const total = 2*produceDispatchBaseWindow + 100
	peer := &zzWP6FramePeer{
		seqs:    zzWP6NumberedBacklog(t, m, total, 256, 1),
		lastSeq: map[int]uint64{},
		seen:    map[uint64]bool{},
	}
	d := NewProduceDispatcher(m, store, "node-self", &fakeProduceCommitter{}, peer.client(), nil, ProduceDispatcherConfig{})
	for range 20 {
		if _, err := d.DispatchAvailable(context.Background()); err != nil {
			t.Fatal(err)
		}
		if peer.committed.Load() == total {
			break
		}
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, p := range peer.problems {
		t.Error(p)
	}
	if got := peer.committed.Load(); got != total {
		t.Fatalf("committed %d of %d", got, total)
	}
	for i, n := range peer.batches[:len(peer.batches)-1] {
		if n < produceDispatchBaseWindow {
			t.Fatalf("batch %d of %v carried %d records, want a whole base window (%d) per commit while a backlog remains",
				i, peer.batches, n, produceDispatchBaseWindow)
		}
	}
}

// remoteBatchLen's estimate must never fall short of the real encoding,
// and a record over the budget must still go out on its own.
func TestZZWP6RemoteBatchLen(t *testing.T) {
	id := func(i int) string { return fmt.Sprintf("topic-id-%d", i%2) }
	rec := func(i, size int) ingress.ProduceRecord {
		return ingress.ProduceRecord{
			Topic:           "orders",
			TopicID:         id(i),
			Key:             strings.Repeat("k", i%7),
			TargetPartition: 1,
			Payload:         bytes.Repeat([]byte("x"), size),
		}
	}
	encoded := func(records []ingress.ProduceRecord) int {
		req := nodewire.CommitProduceBatchRequest{}
		for _, r := range records {
			req.Records = append(req.Records, nodewire.CommitProduceRequest{
				Topic: r.Topic, TopicID: r.TopicID, Key: r.Key, TargetPartition: r.TargetPartition, Payload: r.Payload,
			})
		}
		enc, err := nodewire.EncodeCommitProduceBatchRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		return len(enc)
	}

	small := make([]ingress.ProduceRecord, produceDispatchBaseWindow)
	for i := range small {
		small[i] = rec(i, 256)
	}
	if got := remoteBatchLen(small); got != len(small) {
		t.Fatalf("256-byte records: remoteBatchLen = %d, want the whole base window %d", got, len(small))
	}

	// Alternating topic IDs are the encoding's worst case: one run per
	// record. The batch header (op, count, run count) is 9 bytes.
	big := make([]ingress.ProduceRecord, 4000)
	for i := range big {
		big[i] = rec(i, 5<<10)
	}
	n := remoteBatchLen(big)
	if n <= 1 || n >= len(big) {
		t.Fatalf("5 KiB records: remoteBatchLen = %d, want a proper prefix", n)
	}
	if got := encoded(big[:n]); got > produceRemoteBatchBytes+9 {
		t.Fatalf("5 KiB records: %d taken encode to %d bytes, over the %d-byte budget", n, got, produceRemoteBatchBytes)
	}
	if got := encoded(big[:n+1]); got <= produceRemoteBatchBytes*9/10 {
		t.Fatalf("5 KiB records: stopped at %d records, %d bytes with one more: well below the budget", n, got)
	}

	huge := []ingress.ProduceRecord{rec(0, produceRemoteBatchBytes+1), rec(1, 10)}
	if got := remoteBatchLen(huge); got != 1 {
		t.Fatalf("record over the budget: remoteBatchLen = %d, want 1 (it goes alone)", got)
	}
	if got := remoteBatchLen(huge[:1]); got != 1 {
		t.Fatalf("lone record over the budget: remoteBatchLen = %d, want 1", got)
	}
}
