package topics

// The leader re-checks topic ownership. The HTTP ingress authorizes
// against its own replica before the Manager takes the name lock, so a
// request authorized against one topic could act on whatever topic held
// the name once the lock was granted (a delete racing a recreate, a
// lagging follower). These tests call the Manager as the ingress does
// once its own check has passed, with the request identity on the
// context, and expect the Manager to refuse under its lock, against the
// record as it stands.

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// as returns a context carrying username's identity with a create
// grant on every name, or the admin grant.
func as(username string, admin bool) context.Context {
	u := user.User{Username: username, Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}}}
	if admin {
		u.Grants = []user.Grant{{Action: user.ActionAdmin}}
	}
	return security.WithIdentity(context.Background(), u)
}

func TestNonOwnerCannotAlterOrDeleteThroughTheManager(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000, MaxInFlightPerPartition: 10, MaxAckedAheadPerPartition: 11, Owner: "alice"}
	m := newTestManager(t, ms, &fakeSchemaRegistry{})
	bob := as("bob", false)

	mutations := map[string]func(context.Context) error{
		"retention": func(ctx context.Context) error {
			_, err := m.UpdateTopicRetention(ctx, "orders", 7_200_000)
			return err
		},
		"caps": func(ctx context.Context) error {
			_, err := m.UpdateTopicCaps(ctx, "orders", 99, 99)
			return err
		},
		"partitions": func(ctx context.Context) error {
			_, err := m.IncreaseTopicPartitions(ctx, "orders", 6)
			return err
		},
		"schema": func(ctx context.Context) error {
			_, err := m.UpdateTopicSchema(ctx, "orders", []byte(`{"type":"object"}`), 0)
			return err
		},
		"delete": func(ctx context.Context) error {
			return m.DeleteTopic(ctx, "orders")
		},
	}
	for name, mutate := range mutations {
		if err := mutate(bob); !errors.Is(err, errs.ErrForbidden) {
			t.Errorf("bob's %s on alice's topic = %v, want ErrForbidden", name, err)
		}
	}
	got, ok := ms.topics["orders"]
	if !ok || got.RetentionMs != 3_600_000 || got.Partitions != 3 || got.MaxInFlightPerPartition != 10 || len(ms.schemas["orders"]) != 0 {
		t.Fatalf("a refused mutation changed alice's topic: %+v (exists %v, schemas %d)", got, ok, len(ms.schemas["orders"]))
	}

	// The owner, an admin and a caller with no identity (security off,
	// or an internal caller such as the purge broadcast) may.
	for _, ctx := range []context.Context{as("alice", false), as("root", true), context.Background()} {
		if _, err := m.UpdateTopicRetention(ctx, "orders", 7_200_000); err != nil {
			t.Fatalf("permitted alter: %v", err)
		}
	}
	if err := m.DeleteTopic(as("alice", false), "orders"); err != nil {
		t.Fatalf("the owner's delete: %v", err)
	}
}

// alice's delete passed the ingress against her topic, but by the time
// the leader takes the name lock the name was deleted and recreated by
// bob. The re-check runs against bob's topic and refuses.
func TestDeleteRaceWithRecreateRechecksTheNewOwner(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3, Owner: "alice"}
	m := newTestManager(t, ms, nil)

	// The ingress check would pass here; then the name changes hands.
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "0000000000000002", Partitions: 3, Owner: "bob"}

	if err := m.DeleteTopic(as("alice", false), "orders"); !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("alice's delete of bob's recreated topic = %v, want ErrForbidden", err)
	}
	if got, ok := ms.topics["orders"]; !ok || got.Owner != "bob" {
		t.Fatalf("bob's topic after alice's delete: %+v (exists %v)", got, ok)
	}
}

// A create-as-child copies every record of the parent into the child,
// so it needs manage rights on the parent as it stands under its lock.
func TestCreateAsChildRechecksParentOwner(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["payments"] = topic.Topic{Name: "payments", ID: "0000000000000001", Partitions: 3, Owner: "bob"}
	m := newTestManager(t, ms, nil)

	_, err := m.CreateTopic(as("mallory", false), CreateOpts{Name: "copy", Parent: "payments", Owner: "mallory"})
	if !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("mallory's create-as-child under bob's topic = %v, want ErrForbidden", err)
	}
	if _, ok := ms.topics["copy"]; ok {
		t.Fatal("the refused child was created")
	}
	if p := ms.topics["payments"]; len(p.Children) != 0 {
		t.Fatalf("bob's topic gained a child: %+v", p)
	}
	if _, err := m.CreateTopic(as("bob", false), CreateOpts{Name: "copy", Parent: "payments", Owner: "bob"}); err != nil {
		t.Fatalf("the parent owner's create-as-child: %v", err)
	}
}

// The leader also re-checks the create grant against the identity it
// holds: a caller without a grant matching the name is refused even if
// the ingress let the request through.
func TestCreateRechecksTheCreateGrant(t *testing.T) {
	ms := newFakeMetastore()
	m := newTestManager(t, ms, nil)
	narrow := security.WithIdentity(context.Background(), user.User{
		Username: "carol",
		Grants:   []user.Grant{{Action: user.ActionCreate, Patterns: []string{"carol-*"}}},
	})
	if _, err := m.CreateTopic(narrow, CreateOpts{Name: "orders", Owner: "carol"}); !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("create outside carol's grant = %v, want ErrForbidden", err)
	}
	if _, err := m.CreateTopic(narrow, CreateOpts{Name: "carol-orders", Owner: "carol"}); err != nil {
		t.Fatalf("create inside carol's grant: %v", err)
	}
}

// An attach needs manage rights on both sides (it rewrites the child's
// schema and pumps every parent record into it), a detach on either.
func TestAttachAndDetachRecheckOwners(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["payments"] = topic.Topic{Name: "payments", ID: "0000000000000001", Partitions: 3, Owner: "bob"}
	ms.topics["m-parent"] = topic.Topic{Name: "m-parent", ID: "0000000000000002", Partitions: 3, Owner: "mallory"}
	ms.topics["m-child"] = topic.Topic{Name: "m-child", ID: "0000000000000003", Partitions: 3, Owner: "mallory"}
	m := newTestManager(t, ms, nil)
	mallory := as("mallory", false)

	if err := m.AttachChild(mallory, "m-parent", "payments", 0); !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("attach bob's topic under mallory's = %v, want ErrForbidden", err)
	}
	if err := m.AttachChild(mallory, "payments", "m-child", 0); !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("attach mallory's topic under bob's = %v, want ErrForbidden", err)
	}
	if c := ms.topics["payments"]; c.Parent != "" {
		t.Fatalf("a refused attach linked bob's topic: %+v", c)
	}

	if err := m.AttachChild(as("root", true), "m-parent", "payments", 0); err != nil {
		t.Fatalf("admin attach: %v", err)
	}
	if err := m.DetachChild(as("eve", false), "m-parent", "payments"); !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("a stranger's detach = %v, want ErrForbidden", err)
	}
	if c := ms.topics["payments"]; c.Parent != "m-parent" {
		t.Fatalf("a refused detach unlinked the child: %+v", c)
	}
	if err := m.DetachChild(as("bob", false), "m-parent", "payments"); err != nil {
		t.Fatalf("the child owner's detach: %v", err)
	}
}
