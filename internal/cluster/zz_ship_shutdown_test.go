package cluster

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Run cancelled with a commit in flight and records queued behind it
// waits for the commit to abort, returns its records and the queue to
// the WAL, and stores a checkpoint that covers what did commit and
// stops at the first record that did not. A fresh dispatcher on the same
// WAL then delivers every record.
func TestRunCancelledMidCommitResumesFromWhatCommitted(t *testing.T) {
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, 1) // partition 0 here, 1 on node-remote
	m := newDispatchIngressManagerLargeSegments(t)
	ctx := context.Background()
	accept := func(part int, payload string) uint64 {
		t.Helper()
		res, err := m.AcceptProduce(ctx, "orders", "k", part, []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		return res.WAL.Seq
	}

	inflight := make(chan struct{}, 8)
	hung := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		inflight <- struct{}{}
		<-ctx.Done() // never answers; only Run's cancellation ends it
		return nodewire.Response{}, ctx.Err()
	}}
	local := &fakeProduceCommitter{}
	d := NewProduceDispatcher(m, store, "node-self", local, hung, nil, ProduceDispatcherConfig{})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Run(runCtx); close(done) }()

	accept(0, `{"local":0}`)
	firstRemote := accept(1, `{"remote":0}`)
	select {
	case <-inflight:
	case <-time.After(5 * time.Second):
		t.Fatal("the remote commit never started")
	}
	for i := 1; i <= 3; i++ {
		accept(1, fmt.Sprintf(`{"remote":%d}`, i)) // queued behind the hung commit
	}
	// Once a later local record commits, the loop has read (and queued)
	// every record before it.
	accept(0, `{"local":1}`)
	deadline := time.Now().Add(5 * time.Second)
	for len(local.committed()) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("local records not committed: %d", len(local.committed()))
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel with a commit in flight")
	}
	st := d.state
	if st.outstanding != 0 || st.held != 0 || st.skipped != 4 {
		t.Fatalf("after Run: outstanding %d held %d skipped %d; want the 4 remote records back in the WAL", st.outstanding, st.held, st.skipped)
	}
	stored, err := m.LoadProduceCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if stored != firstRemote {
		t.Fatalf("stored checkpoint %d, want %d: past the committed local record, at the first uncommitted one", stored, firstRemote)
	}

	// A fresh dispatcher on the same WAL, with the owner answering.
	var mu sync.Mutex
	var remote []string
	peer := fakePeerClient{commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range req.Records {
			remote = append(remote, string(r.Payload))
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	local2 := &fakeProduceCommitter{}
	d2 := NewProduceDispatcher(m, store, "node-self", local2, peer, nil, ProduceDispatcherConfig{})
	for range 10 {
		if _, err := d2.DispatchAvailable(ctx); err != nil {
			t.Fatalf("DispatchAvailable after restart: %v", err)
		}
		if d2.state.nextSeq == m.DurableProduceNext() {
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{`{"remote":0}`, `{"remote":1}`, `{"remote":2}`, `{"remote":3}`}; !slices.Equal(remote, want) {
		t.Fatalf("remote partition got %v after the restart, want %v", remote, want)
	}
	// {"local":1} sits past the checkpoint, so it may commit again: a
	// duplicate, never a loss.
	for _, r := range local2.committed() {
		if string(r.Payload) != `{"local":1}` {
			t.Fatalf("restart re-committed %s, which the stored checkpoint covered", r.Payload)
		}
	}
	if d2.state.nextSeq != m.DurableProduceNext() {
		t.Fatalf("checkpoint %d after the restart, want the durable frontier %d", d2.state.nextSeq, m.DurableProduceNext())
	}
}
