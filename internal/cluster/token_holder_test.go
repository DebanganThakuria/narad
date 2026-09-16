package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// countingRegistrar is a demandRegistrar that only counts.
type countingRegistrar struct {
	mu         sync.Mutex
	registered int
	dropped    int
}

func (r *countingRegistrar) RegisterRemoteDemand(context.Context, string, brokermsg.RemoteDemand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registered++
	return nil
}

func (r *countingRegistrar) DropRemoteDemand(string, brokermsg.RemoteDemand) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropped++
}

func (h *tokenHolder) liveCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, byTopic := range h.live {
		n += len(byTopic)
	}
	return n
}

// TestExpiredTokenLeavesTheHolderIndex pins that a token whose TTL ran
// out is forgotten when the dispatcher discards it. Only spent tokens
// were forgotten before, so a peer that stopped re-registering left its
// last token indexed for good.
func TestExpiredTokenLeavesTheHolderIndex(t *testing.T) {
	reg := &countingRegistrar{}
	h := newTokenHolder(reg, fakePeerClient{}, "self.example:7942")
	h.ApplyDelta(context.Background(), nodewire.TokenDelta{
		From: "peer.example:7942",
		Add:  []nodewire.TokenRegistration{{Topic: "orders", TTLNanos: int64(time.Millisecond)}},
	})
	if h.liveCount() != 1 {
		t.Fatalf("live tokens = %d, want 1 after registering", h.liveCount())
	}
	h.mu.Lock()
	tok := h.live["peer.example:7942"]["orders"]
	h.mu.Unlock()

	time.Sleep(5 * time.Millisecond)
	if !tok.Expired() {
		t.Fatal("token with a lapsed TTL reported as live")
	}
	if h.liveCount() != 0 {
		t.Fatalf("live tokens = %d after expiry, want 0", h.liveCount())
	}
}

// TestReRegistrationReplacesTheToken pins the one-token-per-(peer, topic)
// rule the requester side is built around: a second registration
// retires the first at the dispatcher and takes its place.
func TestReRegistrationReplacesTheToken(t *testing.T) {
	reg := &countingRegistrar{}
	h := newTokenHolder(reg, fakePeerClient{}, "self.example:7942")
	for range 2 {
		h.ApplyDelta(context.Background(), nodewire.TokenDelta{
			From: "peer.example:7942",
			Add:  []nodewire.TokenRegistration{{Topic: "orders", TTLNanos: int64(time.Minute)}},
		})
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.registered != 2 || reg.dropped != 1 || h.liveCount() != 1 {
		t.Fatalf("registered %d, dropped %d, live %d; want 2, 1, 1", reg.registered, reg.dropped, h.liveCount())
	}
}
