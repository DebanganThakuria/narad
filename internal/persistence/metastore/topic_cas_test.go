package metastore_test

// Topic alters, schema changes, deletes, attaches and detaches apply
// only to the topic incarnation they were checked against. A
// just-elected leader whose replica has not applied the previous
// leader's last writes reads a stale record; staleReads stands for that
// lag: the next GetTopic of a name answers a copy taken earlier, which
// is what the lagging replica would return.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/security"
)

type staleReads struct {
	*metastore.Store
	mu    sync.Mutex
	stale map[string]topic.Topic
}

// serveOnce makes the next GetTopic of each topic's name answer it.
func (s *staleReads) serveOnce(ts ...topic.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stale == nil {
		s.stale = map[string]topic.Topic{}
	}
	for _, tp := range ts {
		s.stale[tp.Name] = tp
	}
}

func (s *staleReads) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	s.mu.Lock()
	stale, ok := s.stale[name]
	delete(s.stale, name)
	s.mu.Unlock()
	if ok {
		return stale, nil
	}
	return s.Store.GetTopic(ctx, name)
}

// staleStore is a store whose members report this release, behind a
// topic Manager that reads through staleReads.
func staleStore(t *testing.T) (*metastore.Store, *staleReads, *topics.Manager) {
	t.Helper()
	s := newTestStore(t)
	registerCurrentMembers(t, s, "a", "b", "c")
	wrapped := &staleReads{Store: s}
	return s, wrapped, topicManager(t, wrapped, s)
}

// as is a request identity that may create any topic.
func as(username string) context.Context {
	return security.WithIdentity(context.Background(), user.User{
		Username: username,
		Grants:   []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}},
	})
}

func mustGetTopic(t *testing.T, s *metastore.Store, name string) topic.Topic {
	t.Helper()
	tp, err := s.GetTopic(context.Background(), name)
	if err != nil {
		t.Fatalf("GetTopic(%s): %v", name, err)
	}
	return tp
}

// The previous leader acknowledged an increase to 12 partitions; a
// retention alter served from a replica that had not applied it must not
// shrink the topic back to 3.
func TestStaleAlterDoesNotShrinkPartitions(t *testing.T) {
	ctx := context.Background()
	s, wrapped, m := staleStore(t)
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	before := mustGetTopic(t, s, "orders")
	if _, err := m.IncreaseTopicPartitions(ctx, "orders", 12); err != nil {
		t.Fatal(err)
	}

	wrapped.serveOnce(before)
	if _, err := m.UpdateTopicRetention(ctx, "orders", 7_200_000); err != nil {
		t.Fatalf("retention alter: %v", err)
	}
	stored := mustGetTopic(t, s, "orders")
	if stored.Partitions != 12 || stored.RetentionMs != 7_200_000 {
		t.Fatalf("after the alter the topic has %d partitions and retention %d, want 12 and 7200000", stored.Partitions, stored.RetentionMs)
	}
	if rows := assignmentsByPartition(t, s, "orders"); len(rows) != stored.Partitions {
		t.Fatalf("%d assignment rows for a %d-partition topic", len(rows), stored.Partitions)
	}
}

// An alter read from a deleted incarnation must not rewrite the
// recreated topic's owner or partition count.
func TestStaleAlterDoesNotRewriteRecreatedTopic(t *testing.T) {
	ctx := context.Background()
	s, wrapped, m := staleStore(t)
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatal(err)
	}
	old := mustGetTopic(t, s, "orders")
	if err := m.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 6, Owner: "bob"}); err != nil {
		t.Fatal(err)
	}
	recreated := mustGetTopic(t, s, "orders")

	wrapped.serveOnce(old)
	if _, err := m.UpdateTopicRetention(ctx, "orders", 7_200_000); err != nil {
		t.Fatalf("retention alter: %v", err)
	}
	stored := mustGetTopic(t, s, "orders")
	if stored.ID != recreated.ID || stored.Owner != "bob" || stored.Partitions != 6 || stored.CreatedAt != recreated.CreatedAt {
		t.Fatalf("recreated topic after a stale alter: %+v, want id %s, owner bob, 6 partitions, created at %d",
			stored, recreated.ID, recreated.CreatedAt)
	}
}

// Alice's delete, authorized against her deleted topic, must not delete
// the topic bob created under the same name since.
func TestStaleDeleteSparesRecreatedTopic(t *testing.T) {
	s, wrapped, m := staleStore(t)
	if _, err := m.CreateTopic(as("alice"), topics.CreateOpts{Name: "orders", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatal(err)
	}
	old := mustGetTopic(t, s, "orders")
	if err := m.DeleteTopic(as("alice"), "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(as("bob"), topics.CreateOpts{Name: "orders", Partitions: 3, Owner: "bob"}); err != nil {
		t.Fatal(err)
	}
	recreated := mustGetTopic(t, s, "orders")

	wrapped.serveOnce(old)
	err := m.DeleteTopic(as("alice"), "orders")
	if !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("alice's stale delete = %v, want forbidden once the leader sees bob's topic", err)
	}
	if got, err := s.GetTopic(context.Background(), "orders"); err != nil || got.ID != recreated.ID {
		t.Fatalf("bob's topic after alice's stale delete: %+v, %v; want incarnation %s", got, err, recreated.ID)
	}
}

// Alice's schema change, authorized against her deleted topic, must not
// register a version on bob's recreated topic.
func TestStaleSchemaPutDoesNotLandOnRecreatedTopic(t *testing.T) {
	ctx := context.Background()
	s, wrapped, m := staleStore(t)
	if _, err := m.CreateTopic(as("alice"), topics.CreateOpts{Name: "orders", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatal(err)
	}
	old := mustGetTopic(t, s, "orders")
	if err := m.DeleteTopic(as("alice"), "orders"); err != nil {
		t.Fatal(err)
	}
	bobs := []byte(`{"type":"object","required":["id"]}`)
	if _, err := m.CreateTopic(as("bob"), topics.CreateOpts{Name: "orders", Partitions: 3, Owner: "bob", Schema: bobs}); err != nil {
		t.Fatal(err)
	}

	wrapped.serveOnce(old)
	_, err := m.UpdateTopicSchema(as("alice"), "orders", []byte(`{"type":"object"}`), 0)
	if !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("alice's stale schema change = %v, want forbidden once the leader sees bob's topic", err)
	}
	if v, raw, err := s.LatestSchema(ctx, "orders"); err != nil || v != 1 || string(raw) != string(bobs) {
		t.Fatalf("bob's schema after alice's stale change: version %d %s (%v); want version 1 %s", v, raw, err, bobs)
	}
}

// Alice's detach, authorized against her deleted parent and child, must
// not unlink the pair bob created under the same names since.
func TestStaleDetachDoesNotUnlinkRecreatedParent(t *testing.T) {
	s, wrapped, m := staleStore(t)
	link := func(owner string) {
		t.Helper()
		ctx := as(owner)
		for _, name := range []string{"orders", "orders-audit"} {
			if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: name, Partitions: 3, Owner: owner}); err != nil {
				t.Fatalf("%s creates %s: %v", owner, name, err)
			}
		}
		if err := m.AttachChild(ctx, "orders", "orders-audit", 0); err != nil {
			t.Fatalf("%s attaches: %v", owner, err)
		}
	}
	link("alice")
	oldParent, oldChild := mustGetTopic(t, s, "orders"), mustGetTopic(t, s, "orders-audit")
	for _, name := range []string{"orders-audit", "orders"} {
		if err := m.DeleteTopic(as("alice"), name); err != nil {
			t.Fatal(err)
		}
	}
	link("bob")

	wrapped.serveOnce(oldParent, oldChild)
	err := m.DetachChild(as("alice"), "orders", "orders-audit")
	if !errors.Is(err, errs.ErrForbidden) {
		t.Fatalf("alice's stale detach = %v, want forbidden once the leader sees bob's topics", err)
	}
	if child := mustGetTopic(t, s, "orders-audit"); !child.IsChild() || child.Parent != "orders" || child.Owner != "bob" {
		t.Fatalf("bob's child after alice's stale detach: %+v, want attached to orders", child)
	}
}

// A child whose schema history was copied from what the API serves
// (compacted) re-attaches to a parent whose history was stored with the
// client's formatting: the two histories hold the same JSON values.
func TestReattachComparesSchemaValues(t *testing.T) {
	ctx := context.Background()
	s, _, m := staleStore(t)
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	// A history stored before schemas were stored compacted.
	if err := s.PutSchema(ctx, "orders", 1, []byte("{\n  \"type\": \"object\",\n  \"required\": [ \"id\" ]\n}")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic(ctx, topics.CreateOpts{Name: "orders-copy", Partitions: 3, Schema: []byte(`{"type":"object","required":["id"]}`)}); err != nil {
		t.Fatal(err)
	}
	if err := m.AttachChild(ctx, "orders", "orders-copy", 0); err != nil {
		t.Fatalf("re-attach of a child holding the same schema values: %v", err)
	}
	if child := mustGetTopic(t, s, "orders-copy"); !child.IsChild() || child.Parent != "orders" {
		t.Fatalf("child after the attach: %+v, want attached to orders", child)
	}
}
