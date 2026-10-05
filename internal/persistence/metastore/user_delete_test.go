package metastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
)

// soloStore opens a one-voter store and waits until it leads with its
// replica caught up.
func soloStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	return openStore(t, cfg).s
}

func createOwnedTopic(t *testing.T, s *Store, name, owner string) {
	t.Helper()
	if err := s.CreateTopic(context.Background(), topic.Topic{Name: name, Partitions: 1, Owner: owner}); err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
}

func topicOwner(t *testing.T, s *Store, name string) string {
	t.Helper()
	got, err := s.GetTopic(context.Background(), name)
	if err != nil {
		t.Fatalf("GetTopic(%s): %v", name, err)
	}
	return got.Owner
}

// Topic ownership is a bare username. Deleting a user must clear it on
// every topic the user owned, so whoever is later created under the
// same name (a rotated service account, another team) does not inherit
// them; a cleared owner matches no user, and the topics fall to admins.
func TestDeleteUserReleasesTheTopicsItOwned(t *testing.T) {
	s := soloStore(t, singleNodeConfig(t))
	ctx := context.Background()
	for _, name := range []string{"alice", "bob"} {
		if err := s.CreateUser(ctx, user.User{Username: name}); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}
	createOwnedTopic(t, s, "bob-orders", "bob")
	createOwnedTopic(t, s, "bob-refunds", "bob")
	createOwnedTopic(t, s, "alice-orders", "alice")
	before := map[string]uint64{}
	for _, name := range []string{"bob-orders", "bob-refunds", "alice-orders"} {
		before[name] = s.TopicVersion(name)
	}

	if err := s.DeleteUser(ctx, "bob"); err != nil {
		t.Fatalf("DeleteUser(bob): %v", err)
	}
	if _, err := s.GetUser(ctx, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetUser(bob) after delete = %v, want ErrNotFound", err)
	}
	for _, name := range []string{"bob-orders", "bob-refunds"} {
		if owner := topicOwner(t, s, name); owner != "" {
			t.Fatalf("topic %s still owned by %q after its owner was deleted, want no owner", name, owner)
		}
		if s.TopicVersion(name) == before[name] {
			t.Fatalf("topic %s version did not move when its owner was cleared", name)
		}
	}
	if owner := topicOwner(t, s, "alice-orders"); owner != "alice" {
		t.Fatalf("alice-orders owner = %q, want alice (another user's topic is untouched)", owner)
	}
	if s.TopicVersion("alice-orders") != before["alice-orders"] {
		t.Fatal("alice-orders version moved, but nothing about it changed")
	}

	// A new user under the deleted name inherits nothing.
	if err := s.CreateUser(ctx, user.User{Username: "bob"}); err != nil {
		t.Fatalf("CreateUser(bob) again: %v", err)
	}
	if owner := topicOwner(t, s, "bob-orders"); owner == "bob" {
		t.Fatal("the user re-created under the deleted name owns its predecessor's topic")
	}
}

// A refused delete (the root admin, or a user that does not exist)
// changes nothing: no user removed, no topic released.
func TestDeleteUserReleaseTopicsRefusesRootAndMissingUsers(t *testing.T) {
	s := soloStore(t, singleNodeConfig(t))
	ctx := context.Background()
	if err := s.SeedRootUser(ctx, user.User{Username: "admin"}); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	createOwnedTopic(t, s, "admin-orders", "admin")
	createOwnedTopic(t, s, "ghost-orders", "ghost")
	usersBefore := s.UsersVersion()

	if err := s.DeleteUser(ctx, "admin"); !errors.Is(err, ErrRootProtected) {
		t.Fatalf("DeleteUser(admin) = %v, want ErrRootProtected", err)
	}
	if err := s.DeleteUser(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteUser(ghost) = %v, want ErrNotFound", err)
	}
	if _, err := s.GetUser(ctx, "admin"); err != nil {
		t.Fatalf("root admin gone after a refused delete: %v", err)
	}
	if owner := topicOwner(t, s, "admin-orders"); owner != "admin" {
		t.Fatalf("admin-orders owner = %q after a refused delete, want admin", owner)
	}
	if owner := topicOwner(t, s, "ghost-orders"); owner != "ghost" {
		t.Fatalf("ghost-orders owner = %q after a refused delete, want ghost", owner)
	}
	if s.UsersVersion() != usersBefore {
		t.Fatal("users version moved on refused deletes")
	}
}

// Releasing the topics takes a Raft entry type a 3.0.x member would
// skip, so while any member reports an older release the delete uses
// the old entry (the user is still removed: revocation never waits on
// an upgrade) and logs the topics left owned by the name and the member
// holding the new entry back. Once that member reports this release, a
// delete releases the topics.
func TestDeleteUserKeepsLegacyEntryWhileAMemberCannotApplyIt(t *testing.T) {
	logs := &lockedBuffer{}
	cfg := singleNodeConfig(t)
	cfg.Log = slog.New(slog.NewJSONHandler(logs, nil))
	s := soloStore(t, cfg)
	ctx := context.Background()
	// A member on 3.0.x: its heartbeat reports no build and no entry types.
	old := Member{ID: "old-node", Addr: "10.0.0.9:7942", Status: MemberAlive, LastHeartbeat: time.Now().Unix()}
	if err := s.RegisterMember(ctx, old); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	for _, name := range []string{"bob", "carol"} {
		if err := s.CreateUser(ctx, user.User{Username: name}); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}
	createOwnedTopic(t, s, "bob-orders", "bob")
	createOwnedTopic(t, s, "carol-orders", "carol")

	if err := s.DeleteUser(ctx, "bob"); err != nil {
		t.Fatalf("DeleteUser(bob): %v", err)
	}
	if _, err := s.GetUser(ctx, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetUser(bob) = %v; the user must be removed even while the release entry is held back", err)
	}
	if owner := topicOwner(t, s, "bob-orders"); owner != "bob" {
		t.Fatalf("bob-orders owner = %q; the old entry does not release topics", owner)
	}
	if newest, err := s.NewestAppliedEntryType(); err != nil || newest > legacyMaxEntryType {
		t.Fatalf("NewestAppliedEntryType = %d, %v; a 3.0.x member was sent an entry type it would skip", newest, err)
	}
	var warned map[string]any
	for _, line := range bytes.Split(logs.contents(), []byte("\n")) {
		var m map[string]any
		if json.Unmarshal(line, &m) == nil && strings.Contains(fmt.Sprint(m["msg"]), "topics still name it as owner") {
			warned = m
		}
	}
	if warned == nil {
		t.Fatalf("no warning about the topics left owned by the deleted user; log:\n%s", logs.contents())
	}
	if warned["level"] != "WARN" || warned["component"] != "audit" || warned["username"] != "bob" ||
		fmt.Sprint(warned["topics"]) != "[bob-orders]" || warned["topic_count"] != float64(1) ||
		!strings.Contains(fmt.Sprint(warned["reason"]), `"old-node"`) {
		t.Fatalf("warning = %v; want level WARN, component audit, username bob, topics [bob-orders], topic_count 1 and a reason naming old-node", warned)
	}

	// The member upgrades: its heartbeat now reports this release.
	old.Build, old.EntryTypes = "narad test", MaxEntryType
	if err := s.RegisterMember(ctx, old); err != nil {
		t.Fatalf("RegisterMember(upgraded): %v", err)
	}
	if err := s.DeleteUser(ctx, "carol"); err != nil {
		t.Fatalf("DeleteUser(carol): %v", err)
	}
	if owner := topicOwner(t, s, "carol-orders"); owner != "" {
		t.Fatalf("carol-orders owner = %q once every member applies the release entry, want none", owner)
	}
}

// The release entry is deterministic: every replica that applies the
// same log, and one restored from a snapshot of it, holds the same users
// and topics.
func TestDeleteUserReleaseTopicsReplaysToTheSameState(t *testing.T) {
	type entry struct {
		op      opCode
		payload any
	}
	log := []entry{
		{opCreateUser, user.User{Username: "bob"}},
		{opCreateUser, user.User{Username: "alice"}},
		{opCreateTopic, topic.Topic{Name: "z-bob", Partitions: 1, Owner: "bob"}},
		{opCreateTopic, topic.Topic{Name: "a-bob", Partitions: 2, Owner: "bob"}},
		{opCreateTopic, topic.Topic{Name: "m-alice", Partitions: 1, Owner: "alice"}},
		{opDeleteUserReleaseTopics, userDeletePayload{Username: "bob"}},
		// Refused, and so changes nothing: bob is already gone.
		{opDeleteUserReleaseTopics, userDeletePayload{Username: "bob"}},
	}
	replicas := []*fsmState{newTestFSM(t), newTestFSM(t)}
	for _, f := range replicas {
		for i, e := range log {
			err := applyEntry(t, f, uint64(i+1), e.op, e.payload)
			if i == len(log)-1 {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("second delete of bob = %v, want ErrNotFound", err)
				}
			} else if err != nil {
				t.Fatalf("entry %d (type %d): %v", i+1, e.op, err)
			}
		}
	}
	restored := newTestFSM(t)
	if err := restored.Restore(io.NopCloser(bytes.NewReader(snapshotImage(t, replicas[0])))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	want := bucketContents(t, replicas[0], bucketUsers, bucketTopics)
	for name, f := range map[string]*fsmState{"second replica": replicas[1], "restored replica": restored} {
		if got := bucketContents(t, f, bucketUsers, bucketTopics); !maps.Equal(got, want) {
			t.Fatalf("%s differs:\n got %v\nwant %v", name, got, want)
		}
	}
	for _, name := range []string{"a-bob", "z-bob"} {
		if owner := fsmGetTopic(t, replicas[0], name).Owner; owner != "" {
			t.Fatalf("%s owner = %q, want none", name, owner)
		}
	}
	if owner := fsmGetTopic(t, replicas[0], "m-alice").Owner; owner != "alice" {
		t.Fatalf("m-alice owner = %q, want alice", owner)
	}
}

// bucketContents returns every key and value of the named buckets of f,
// keyed by bucket and key.
func bucketContents(t *testing.T, f *fsmState, buckets ...[]byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := f.db.View(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if err := tx.Bucket(b).ForEach(func(k, v []byte) error {
				out[string(b)+"/"+string(k)] = string(v)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read buckets: %v", err)
	}
	return out
}

// contents returns a copy of what was written to b so far.
func (b *lockedBuffer) contents() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}
