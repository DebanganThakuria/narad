package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP14Call is one commit RPC as the transport saw it: the budget the
// caller handed it, whether ctx carried a deadline of its own, and the
// decoded request.
type zzWP14Call struct {
	budget      time.Duration
	hasDeadline bool
	req         nodewire.CommitProduceBatchRequest
}

// zzWP14Frames is a frameTransport for commit_produce_batch requests:
// it records every call and answers the i-th (counting from zero, over
// its life) with reply(i).
type zzWP14Frames struct {
	t     *testing.T
	reply func(i int) (nodewire.Response, error)

	mu    sync.Mutex
	n     int
	calls []zzWP14Call
}

func (f *zzWP14Frames) RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLaneTimeout(ctx, addr, lane, 0, frameType, payload)
}

func (f *zzWP14Frames) RequestOnLaneTimeout(ctx context.Context, _ string, _ clusterrpc.Lane, timeout time.Duration, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	req, err := nodewire.DecodeCommitProduceBatchRequest(payload)
	if err != nil {
		f.t.Errorf("transport saw a request that is not a commit batch: %v", err)
		return clusterwire.StreamFrame{}, err
	}
	_, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	i := f.n
	f.n++
	f.calls = append(f.calls, zzWP14Call{budget: timeout, hasDeadline: hasDeadline, req: req})
	f.mu.Unlock()
	res, err := f.reply(i)
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, Payload: zzWP9Reply(f.t, res)}, nil
}

func (f *zzWP14Frames) take() []zzWP14Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := f.calls
	f.calls = nil
	return calls
}

// A remote commit hands its budget to the transport, not a context
// derived per commit: a batch gets produceCommitRPCTimeout and the
// single-record probe of a failing destination produceProbeRPCTimeout.
func TestZZWP14RemoteCommitHandsItsBudgetToTheTransport(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	manager := newDispatchIngressManager(t)
	for range 3 {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce() error = %v", err)
		}
	}
	frames := &zzWP14Frames{t: t, reply: func(i int) (nodewire.Response, error) {
		if i == 0 {
			return nodewire.Response{}, errors.New("owner unreachable")
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	d := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, &PeerClient{frames: frames}, nil, ProduceDispatcherConfig{})

	committed := 0
	var calls []zzWP14Call
	for pass := 0; pass < 5 && committed < 3; pass++ {
		_, _ = d.DispatchAvailable(ctx)
		for _, c := range frames.take() {
			calls = append(calls, c)
			if len(calls) > 1 {
				committed += len(c.req.Records)
			}
		}
	}
	if committed != 3 {
		t.Fatalf("owner committed %d records after the failed batch, want 3 (calls %+v)", committed, calls)
	}
	if calls[0].budget != produceCommitRPCTimeout || len(calls[0].req.Records) != 3 {
		t.Fatalf("first commit: budget %s with %d records, want the %s batch budget with 3",
			calls[0].budget, len(calls[0].req.Records), produceCommitRPCTimeout)
	}
	if calls[1].budget != produceProbeRPCTimeout || len(calls[1].req.Records) != 1 {
		t.Fatalf("retry of the failing destination: budget %s with %d records, want the %s probe budget with 1",
			calls[1].budget, len(calls[1].req.Records), produceProbeRPCTimeout)
	}
	for i, c := range calls {
		if c.hasDeadline {
			t.Fatalf("commit %d carried a derived context deadline, want the budget only", i)
		}
		if i > 1 && c.budget != produceCommitRPCTimeout {
			t.Fatalf("commit %d after the probe: budget %s, want %s", i, c.budget, produceCommitRPCTimeout)
		}
	}
}

// An owner on an older release refuses the topic IDs; the resend
// without them goes out on what is left of the same budget, not a fresh
// one, and neither call derives a context deadline.
func TestZZWP14LegacyResendSharesTheBudget(t *testing.T) {
	frames := &zzWP14Frames{t: t, reply: func(i int) (nodewire.Response, error) {
		if i == 0 {
			return nodewire.Response{Status: http.StatusBadRequest, Body: []byte("invalid commit produce batch request: trailing node rpc payload data")}, nil
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	d := NewProduceDispatcher(nil, nil, "node-self", nil, &PeerClient{frames: frames}, nil, ProduceDispatcherConfig{})
	records := []ingress.ProduceRecord{{Topic: "orders", TopicID: "incarnation-1", Key: "k", Payload: []byte(`{"id":1}`)}}

	if err := d.commitRemote(context.Background(), "remote.example:7942", records, produceCommitRPCTimeout); err != nil {
		t.Fatalf("commitRemote() error = %v", err)
	}
	calls := frames.take()
	if len(calls) != 2 {
		t.Fatalf("%d commit RPCs, want the refused one and its resend", len(calls))
	}
	if calls[0].budget != produceCommitRPCTimeout || calls[0].req.Records[0].TopicID != "incarnation-1" {
		t.Fatalf("first call: budget %s, topic id %q; want %s with the id", calls[0].budget, calls[0].req.Records[0].TopicID, produceCommitRPCTimeout)
	}
	if calls[1].budget <= 0 || calls[1].budget > produceCommitRPCTimeout || calls[1].req.Records[0].TopicID != "" {
		t.Fatalf("resend: budget %s, topic id %q; want what is left of %s, without the id", calls[1].budget, calls[1].req.Records[0].TopicID, produceCommitRPCTimeout)
	}
	for i, c := range calls {
		if c.hasDeadline {
			t.Fatalf("call %d carried a derived context deadline, want the budget only", i)
		}
	}
	if !d.legacyOwner("remote.example:7942") {
		t.Fatal("owner not remembered as legacy after refusing the topic IDs")
	}
}

// A budget spent by the time the refusal arrives ends the commit with an
// error wrapping context.DeadlineExceeded, without a resend.
func TestZZWP14LegacyResendAfterTheBudgetIsSpent(t *testing.T) {
	const budget = 20 * time.Millisecond
	frames := &zzWP14Frames{t: t, reply: func(int) (nodewire.Response, error) {
		time.Sleep(2 * budget)
		return nodewire.Response{Status: http.StatusBadRequest, Body: []byte("trailing node rpc payload data")}, nil
	}}
	d := NewProduceDispatcher(nil, nil, "node-self", nil, &PeerClient{frames: frames}, nil, ProduceDispatcherConfig{})
	records := []ingress.ProduceRecord{{Topic: "orders", TopicID: "incarnation-1", Payload: []byte(`{}`)}}

	err := d.commitRemote(context.Background(), "remote.example:7942", records, budget)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commitRemote() error = %v, want one wrapping context.DeadlineExceeded", err)
	}
	if calls := frames.take(); len(calls) != 1 {
		t.Fatalf("%d commit RPCs, want only the refused one", len(calls))
	}
}

// zzWP14StuckOwner is an owner whose commits never finish until the
// request is abandoned; acks answer at once so zzWP9QUICPeer can see it
// is up.
type zzWP14StuckOwner struct {
	zzWP9Broker
	started chan struct{}
}

func (b *zzWP14StuckOwner) CommitAcceptedProduceBatch(ctx context.Context, _ []ingress.ProduceRecord) ([]int64, error) {
	select {
	case b.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// Over real QUIC, a remote commit that outlives its budget fails with an
// error wrapping context.DeadlineExceeded, as it did when the budget was
// a context deadline, and does so in about the budget.
func TestZZWP14RemoteCommitBudgetExpiresAsDeadlineExceeded(t *testing.T) {
	owner := &zzWP14StuckOwner{started: make(chan struct{}, 1)}
	addr, client := zzWP9QUICPeer(t, owner)
	d := NewProduceDispatcher(nil, nil, "node-self", nil, client, nil, ProduceDispatcherConfig{})
	records := []ingress.ProduceRecord{{Topic: "orders", Key: "k", Payload: []byte(`{"id":1}`)}}

	const budget = 200 * time.Millisecond
	start := time.Now()
	err := d.commitRemote(context.Background(), addr, records, budget)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commitRemote() error = %v, want one wrapping context.DeadlineExceeded", err)
	}
	select {
	case <-owner.started:
	default:
		t.Fatal("the owner never saw the commit")
	}
	if elapsed < budget || elapsed > 3*time.Second {
		t.Fatalf("commit ended after %s, want about its %s budget", elapsed, budget)
	}
}
