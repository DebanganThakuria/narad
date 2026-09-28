package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The dispatcher stores its checkpoint as records commit, so the ingress
// backlog operators wait on before a rollback holds while the owner
// refuses commits and drains to 0 once they land.
func TestZZWP14DispatchDrainsTheIngressBacklog(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-remote")
	manager := newDispatchIngressManager(t)
	for range 3 {
		if _, err := manager.AcceptProduce(ctx, "orders", "k", 0, []byte(`{"id":1}`)); err != nil {
			t.Fatalf("AcceptProduce() error = %v", err)
		}
	}
	var ownerUp atomic.Bool
	peer := fakePeerClient{commitProduceBatchFn: func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		if !ownerUp.Load() {
			return nodewire.Response{}, errors.New("owner unreachable")
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	d := NewProduceDispatcher(manager, store, "node-self", &fakeProduceCommitter{}, peer, nil, ProduceDispatcherConfig{})

	_, _ = d.DispatchAvailable(ctx)
	if got := manager.DispatchBacklog(); got != 3 {
		t.Fatalf("backlog while the owner refuses commits = %d, want 3", got)
	}
	ownerUp.Store(true)
	for pass := 0; pass < 5 && manager.DispatchBacklog() > 0; pass++ {
		if _, err := d.DispatchAvailable(ctx); err != nil {
			t.Fatalf("DispatchAvailable() error = %v", err)
		}
	}
	if got := manager.DispatchBacklog(); got != 0 {
		t.Fatalf("backlog after the owner took every record = %d, want 0", got)
	}
}
