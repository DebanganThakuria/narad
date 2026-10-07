package topics

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// remoteLinkedStore holds orders with one remote child, orders-to-b,
// as the leader's replica shows it after an attach the ingress node's
// replica had not applied yet.
func remoteLinkedStore() *fakeMetastore {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "p1", Partitions: 1, Role: topic.RoleParent, Children: []string{"orders-to-b"}}
	ms.topics["orders-to-b"] = topic.Topic{Name: "orders-to-b", ID: "s1", Role: topic.RoleChild, Parent: "orders", Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}}
	return ms
}

// A plain delete or detach that reaches the topic manager for a stub,
// or a parent with remote children, is refused under the topic's lock,
// whatever the caller's replica decided: only the remote-aware delete
// runs the unshipped check.
func TestPlainDeleteAndDetachRefuseRemoteLinkedTopics(t *testing.T) {
	ms := remoteLinkedStore()
	m := newTestManager(t, ms, nil)
	ctx := context.Background()
	for _, name := range []string{"orders", "orders-to-b"} {
		if _, err := m.DeleteTopicID(ctx, name); !errors.Is(err, errs.ErrRemoteAwareDeleteRequired) {
			t.Fatalf("plain delete of %s: %v, want the remote-aware delete required", name, err)
		}
	}
	if err := m.DetachChild(ctx, "orders", "orders-to-b"); !errors.Is(err, errs.ErrRemoteAwareDeleteRequired) {
		t.Fatalf("plain detach of the stub: %v, want the remote-aware delete required", err)
	}
	if _, ok := ms.topics["orders-to-b"]; !ok {
		t.Fatal("a refused delete removed the stub")
	}
}

// The remote-aware delete goes ahead only on the set of stubs its check
// covered.
func TestRemoteAwareDeleteNeedsTheCheckedStubs(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		expect map[string]string
	}{
		{"none checked", map[string]string{}},
		{"another incarnation", map[string]string{"orders-to-b": "s0"}},
		{"one more", map[string]string{"orders-to-b": "s1", "orders-to-c": "s2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := remoteLinkedStore()
			m := newTestManager(t, ms, nil)
			if _, err := m.DeleteRemoteLinkedTopicID(ctx, "orders", tc.expect); !errors.Is(err, errs.ErrTopicChanged) {
				t.Fatalf("delete expecting %v: %v, want topic changed", tc.expect, err)
			}
		})
	}
	ms := remoteLinkedStore()
	m := newTestManager(t, ms, nil)
	if err := m.DetachRemoteChild(ctx, "orders", "orders-to-b", "s0"); !errors.Is(err, errs.ErrTopicChanged) {
		t.Fatalf("detach of another stub incarnation: %v, want topic changed", err)
	}
	if err := m.DetachRemoteChild(ctx, "orders", "orders-to-b", "s1"); err != nil {
		t.Fatalf("detach of the checked stub: %v", err)
	}
}
