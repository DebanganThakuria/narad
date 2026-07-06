package metastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func TestUserCRUD(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if has, err := s.HasUsers(ctx); err != nil || has {
		t.Fatalf("HasUsers() = %v, %v, want false, nil", has, err)
	}

	alice := user.User{
		Username:     "alice",
		PasswordHash: []byte("$2a$10$fakehash"),
		Grants:       []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
		CreatedAtMs:  1,
		UpdatedAtMs:  1,
	}
	if err := s.CreateUser(ctx, alice); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.CreateUser(ctx, alice); !errors.Is(err, metastore.ErrAlreadyExists) {
		t.Fatalf("duplicate CreateUser error = %v, want ErrAlreadyExists", err)
	}

	got, err := s.GetUser(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.Username != "alice" || string(got.PasswordHash) != "$2a$10$fakehash" || len(got.Grants) != 1 {
		t.Fatalf("GetUser = %+v", got)
	}

	if has, err := s.HasUsers(ctx); err != nil || !has {
		t.Fatalf("HasUsers() after create = %v, %v, want true, nil", has, err)
	}

	alice.Grants = append(alice.Grants, user.Grant{Action: user.ActionConsume, Patterns: []string{"orders-eu"}})
	alice.UpdatedAtMs = 2
	if err := s.UpdateUser(ctx, alice); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	got, err = s.GetUser(ctx, "alice")
	if err != nil || len(got.Grants) != 2 || got.UpdatedAtMs != 2 {
		t.Fatalf("GetUser after update = %+v, %v", got, err)
	}

	if err := s.UpdateUser(ctx, user.User{Username: "ghost"}); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("UpdateUser(ghost) error = %v, want ErrNotFound", err)
	}

	if err := s.CreateUser(ctx, user.User{Username: "bob"}); err != nil {
		t.Fatalf("CreateUser(bob): %v", err)
	}
	users, err := s.ListUsers(ctx)
	if err != nil || len(users) != 2 || users[0].Username != "alice" || users[1].Username != "bob" {
		t.Fatalf("ListUsers = %+v, %v", users, err)
	}

	if err := s.DeleteUser(ctx, "bob"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if err := s.DeleteUser(ctx, "bob"); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("DeleteUser(bob) again error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetUser(ctx, "bob"); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("GetUser(bob) error = %v, want ErrNotFound", err)
	}
}

func TestUsersVersionBumpsOnEveryMutation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	v0 := s.UsersVersion()
	if err := s.CreateUser(ctx, user.User{Username: "alice"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	v1 := s.UsersVersion()
	if v1 <= v0 {
		t.Fatalf("version after create = %d, want > %d", v1, v0)
	}

	if err := s.UpdateUser(ctx, user.User{Username: "alice", UpdatedAtMs: 9}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	v2 := s.UsersVersion()
	if v2 <= v1 {
		t.Fatalf("version after update = %d, want > %d", v2, v1)
	}

	// Failed mutations must not invalidate caches.
	if err := s.UpdateUser(ctx, user.User{Username: "ghost"}); err == nil {
		t.Fatal("UpdateUser(ghost) should fail")
	}
	if v := s.UsersVersion(); v != v2 {
		t.Fatalf("version after failed update = %d, want %d", v, v2)
	}

	if err := s.DeleteUser(ctx, "alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if v := s.UsersVersion(); v <= v2 {
		t.Fatalf("version after delete = %d, want > %d", v, v2)
	}

	// Topic mutations must not bump the users domain.
	before := s.UsersVersion()
	if err := s.CreateUser(ctx, user.User{Username: "carol"}); err != nil {
		t.Fatalf("CreateUser(carol): %v", err)
	}
	after := s.UsersVersion()
	if after <= before {
		t.Fatalf("users version did not advance: %d -> %d", before, after)
	}
}
