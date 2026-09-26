package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func wp10bNewStore(t *testing.T, nodeID string) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   nodeID,
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// The heartbeater's first attempt runs at boot, before any leader
// exists, and fails. It used to wait the full interval (5 s in serve)
// before trying again, so a fresh cluster had an empty member table for
// seconds after /readyz, and topics created then got no owners. It must
// retry quickly until it has registered once.
func TestWP10BHeartbeaterRegistersSoonAfterLeaderElected(t *testing.T) {
	store := wp10bNewStore(t, "narad-0")
	if store.IsLeader() {
		t.Skip("leader elected before the heartbeater started; nothing to observe")
	}
	member := metastore.Member{ID: "narad-0", Addr: "127.0.0.1:7943", Status: metastore.MemberAlive}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runMemberHeartbeater(ctx, store, member, 5*time.Second, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	waitForLeadership(t, store)
	elected := time.Now()
	deadline := elected.Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m, err := store.GetMember("narad-0"); err == nil && m.Status == metastore.MemberAlive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("member not registered within 2s of the leader election; the heartbeater waited out its 5s interval")
}

// wp10bSendLog records when heartbeatLoop called send and answers from a
// scripted list of results (the last one repeats).
type wp10bSendLog struct {
	mu      sync.Mutex
	results []error
	calls   []time.Time
}

func (l *wp10bSendLog) send() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, time.Now())
	i := min(len(l.calls)-1, len(l.results)-1)
	return l.results[i]
}

func (l *wp10bSendLog) snapshot() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Time(nil), l.calls...)
}

func (l *wp10bSendLog) waitCalls(t *testing.T, n int, within time.Duration) []time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if calls := l.snapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("send called %d times within %v, want at least %d", len(l.snapshot()), within, n)
	return nil
}

func wp10bRunLoop(t *testing.T, interval, retry time.Duration, l *wp10bSendLog) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		heartbeatLoop(ctx, interval, retry, l.send)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// Failures before the first success retry at the short interval; once
// registered, the loop keeps the full interval even across a failure.
func TestWP10BHeartbeatLoopRetriesFastOnlyUntilFirstSuccess(t *testing.T) {
	const interval, retry = 600 * time.Millisecond, 10 * time.Millisecond
	fail := errors.New("no leader yet")
	l := &wp10bSendLog{results: []error{fail, fail, fail, nil, fail, nil}}
	wp10bRunLoop(t, interval, retry, l)

	calls := l.waitCalls(t, 4, interval)
	for i := 1; i < 4; i++ {
		if gap := calls[i].Sub(calls[i-1]); gap >= interval/2 {
			t.Fatalf("retry %d came %v after the previous attempt, want the short retry interval", i, gap)
		}
	}

	calls = l.waitCalls(t, 6, 4*interval)
	for i := 4; i < 6; i++ {
		if gap := calls[i].Sub(calls[i-1]); gap < interval {
			t.Fatalf("attempt %d came %v after the previous one; after the first success the loop must wait the full %v", i+1, gap, interval)
		}
	}
}

// A removed member never retries fast: each attempt is a Raft entry on
// the leader that can only be refused.
func TestWP10BHeartbeatLoopRemovedMemberKeepsFullInterval(t *testing.T) {
	const interval, retry = 600 * time.Millisecond, 10 * time.Millisecond
	l := &wp10bSendLog{results: []error{errMemberRemoved}}
	wp10bRunLoop(t, interval, retry, l)

	l.waitCalls(t, 1, time.Second)
	time.Sleep(interval / 2)
	if n := len(l.snapshot()); n != 1 {
		t.Fatalf("send called %d times within half an interval of a removed-member answer, want 1", n)
	}
}

type wp10bGoneRegistrar struct{ calls int }

func (r *wp10bGoneRegistrar) RegisterMember(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error) {
	r.calls++
	return nodewire.Response{Status: http.StatusGone}, nil
}

// The leader answers 410 Gone to a member that decommission removed;
// registerMember must surface that as errMemberRemoved so the loop above
// can tell it from a transient failure.
func TestWP10BRegisterMemberMapsGoneToRemoved(t *testing.T) {
	ctx := context.Background()
	store := wp10bNewStore(t, "narad-0")
	waitForLeadership(t, store)
	if err := store.RegisterMember(ctx, metastore.Member{ID: "narad-0", Addr: "127.0.0.1:7943", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("register leader member: %v", err)
	}
	if err := store.RemoveMember(ctx, "narad-9", time.Now().Unix()); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}

	registrar := &wp10bGoneRegistrar{}
	err := registerMember(ctx, store, metastore.Member{ID: "narad-9", Addr: "127.0.0.1:7949", Status: metastore.MemberAlive}, registrar)
	if !errors.Is(err, errMemberRemoved) {
		t.Fatalf("registerMember for a removed member = %v, want errMemberRemoved", err)
	}
	if registrar.calls != 1 {
		t.Fatalf("forwarded %d registrations, want 1", registrar.calls)
	}
}
