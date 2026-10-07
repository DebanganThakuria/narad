package cluster

// A remote child's delete and its parent's delete are the topic
// manager's own detach and delete, not writes around them.

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
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

func (b *recordingDeletes) DeleteRemoteLinkedTopicID(ctx context.Context, name string, expect map[string]string) (string, error) {
	b.mu.Lock()
	b.calls = append(b.calls, "delete "+name)
	b.mu.Unlock()
	return b.Broker.(broker.RemoteLinkedDeleter).DeleteRemoteLinkedTopicID(ctx, name, expect)
}

func (b *recordingDeletes) DetachRemoteChild(ctx context.Context, parent, child, stubID string) error {
	b.mu.Lock()
	b.calls = append(b.calls, "detach "+parent+"/"+child)
	b.mu.Unlock()
	return b.Broker.(broker.RemoteLinkedDeleter).DetachRemoteChild(ctx, parent, child, stubID)
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

// rigBroker is the rig broker's surface a wrapper must keep: the
// optional remote child attach and the remote-aware delete included.
type rigBroker interface {
	broker.Broker
	broker.TopicIDDeleter
	broker.RemoteChildAttacher
	broker.RemoteLinkedDeleter
}

// attachDuringCheck attaches a second remote child the first time the
// unshipped check reads the cursor stats: an attach that applied after
// a delete fixed the set of stubs it checks, and before the delete.
type attachDuringCheck struct {
	rigBroker
	fired  atomic.Bool
	attach func()
}

func (b *attachDuringCheck) FanoutCursorStats(ctx context.Context, name string) ([]topic.FanoutCursorStat, error) {
	// Once, and not re-entered: the attach may run a check of its own.
	if b.fired.CompareAndSwap(false, true) {
		b.attach()
	}
	// Every child caught up on every partition: the check passes.
	p, err := b.GetTopic(ctx, name)
	if err != nil {
		return nil, err
	}
	var out []topic.FanoutCursorStat
	for _, child := range p.Children {
		for part := range p.Partitions {
			out = append(out, topic.FanoutCursorStat{Child: child, Partition: part, Node: "node-self"})
		}
	}
	return out, nil
}

// A parent's delete removes only the remote children it checked: one
// attached while the check ran makes the delete answer 409 and retry,
// forced or not, instead of taking the new stub's backlog with it.
func TestParentDeleteRefusesARemoteChildAttachedDuringItsCheck(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "checked", true: "forced"}[force], func(t *testing.T) {
			s := linksRig(t)
			if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
				t.Fatalf("attach: %d %s", res.Status, res.Body)
			}
			s.links.d.Broker = &attachDuringCheck{rigBroker: s.links.d.Broker.(rigBroker), attach: func() {
				res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-c", "remote": "b", "remote_topic": "orders-c", "from": "earliest"})
				if res.Status != http.StatusCreated {
					t.Errorf("attach during the check: %d %s", res.Status, res.Body)
				}
			}}
			res := s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "orders", "expect_remote": true, "force": force})
			if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "retry") {
				t.Fatalf("delete after an attach it did not check: %d %s, want 409 retry", res.Status, res.Body)
			}
			for _, name := range []string{"orders", "orders-to-b", "orders-to-c"} {
				if _, err := s.store.GetTopic(context.Background(), name); err != nil {
					t.Fatalf("%s after the refused delete: %v", name, err)
				}
			}
		})
	}
}

// A remote child's delete removes only the stub it checked: a stub
// deleted and attached again under the same name while the check ran
// is a different link, and the delete answers 409.
func TestRemoteChildDeleteRefusesAStubReplacedDuringItsCheck(t *testing.T) {
	s := linksRig(t)
	attach := func() {
		t.Helper()
		if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
			t.Fatalf("attach: %d %s", res.Status, res.Body)
		}
	}
	attach()
	before, err := s.store.GetTopic(context.Background(), "orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	s.links.d.Broker = &attachDuringCheck{rigBroker: s.links.d.Broker.(rigBroker), attach: func() {
		if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true}); res.Status != http.StatusNoContent {
			t.Errorf("delete during the check: %d %s", res.Status, res.Body)
		}
		attach()
	}}
	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true})
	if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "retry") {
		t.Fatalf("delete of a replaced stub: %d %s, want 409 retry", res.Status, res.Body)
	}
	after, err := s.store.GetTopic(context.Background(), "orders-to-b")
	if err != nil || after.ID == before.ID {
		t.Fatalf("stub after the refused delete: %+v %v, want the replacement", after, err)
	}
}
