package cluster

// A remote child's delete and its parent's delete are the topic
// manager's own detach and delete, not writes around them.

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// recordingDeletes records the deletes and detaches the leader side
// asks the broker for.
type recordingDeletes struct {
	broker.Broker
	mu    sync.Mutex
	calls []string
}

func (b *recordingDeletes) DeleteTopic(ctx context.Context, name string) error {
	b.mu.Lock()
	b.calls = append(b.calls, "delete "+name)
	b.mu.Unlock()
	return b.Broker.DeleteTopic(ctx, name)
}

func (b *recordingDeletes) DetachChild(ctx context.Context, parent, child string) error {
	b.mu.Lock()
	b.calls = append(b.calls, "detach "+parent+"/"+child)
	b.mu.Unlock()
	return b.Broker.DetachChild(ctx, parent, child)
}

func (b *recordingDeletes) AttachRemoteChild(ctx context.Context, op metastore.AttachRemoteChildOp) error {
	return b.Broker.(broker.RemoteChildAttacher).AttachRemoteChild(ctx, op)
}

func (b *recordingDeletes) take() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.calls
	b.calls = nil
	return out
}

// A remote child's delete is the topic manager's detach, which deletes
// the stub with its link. A parent's delete is one topic delete, which
// takes its stubs with it in the same entry.
func TestRemoteChildDeletesGoThroughTheTopicManager(t *testing.T) {
	s := linksRig(t)
	rec := &recordingDeletes{Broker: s.links.d.Broker}
	s.links.d.Broker = rec
	attach := func() {
		t.Helper()
		if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
			t.Fatalf("attach: %d %s", res.Status, res.Body)
		}
	}

	attach()
	if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true}); res.Status != http.StatusNoContent {
		t.Fatalf("child delete: %d %s", res.Status, res.Body)
	}
	if got := rec.take(); !slices.Equal(got, []string{"detach orders/orders-to-b"}) {
		t.Fatalf("child delete asked the broker for %v, want the topic manager's detach", got)
	}
	parent, err := s.store.GetTopic(context.Background(), "orders")
	if err != nil || slices.Contains(parent.Children, "orders-to-b") || parent.IsParent() {
		t.Fatalf("parent after the stub delete: %+v %v", parent, err)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err == nil {
		t.Fatal("the stub outlived its detach")
	}

	attach()
	if res := s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "orders", "expect_remote": true, "force": true}); res.Status != http.StatusNoContent {
		t.Fatalf("parent delete: %d %s", res.Status, res.Body)
	}
	if got := rec.take(); !slices.Equal(got, []string{"delete orders"}) {
		t.Fatalf("parent delete asked the broker for %v, want the parent's delete alone", got)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err == nil {
		t.Fatal("the stub outlived its parent's delete")
	}
}
