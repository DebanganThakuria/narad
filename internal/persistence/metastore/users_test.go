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

func TestRootProtectionEnforcedInFSM(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// CreateUser must never persist a root account, even if the caller
	// sets Root=true (defends the leader-forwarded RPC path).
	if err := s.CreateUser(ctx, user.User{
		Username: "sneaky", Root: true,
		Grants: []user.Grant{{Action: user.ActionAdmin}},
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	got, _ := s.GetUser(ctx, "sneaky")
	if got.Root {
		t.Fatal("CreateUser persisted a root account — escalation possible")
	}

	// Only SeedRootUser makes a root account, and it is idempotent.
	if err := s.SeedRootUser(ctx, user.User{
		Username: "admin", Root: true, PasswordHash: []byte("h1"),
		Grants: []user.Grant{{Action: user.ActionAdmin}},
	}); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	root, _ := s.GetUser(ctx, "admin")
	if !root.Root {
		t.Fatal("SeedRootUser did not create a root account")
	}
	if err := s.SeedRootUser(ctx, user.User{Username: "admin", Root: true, PasswordHash: []byte("h2")}); !errors.Is(err, metastore.ErrAlreadyExists) {
		t.Fatalf("second SeedRootUser = %v, want ErrAlreadyExists (no clobber)", err)
	}
	if again, _ := s.GetUser(ctx, "admin"); string(again.PasswordHash) != "h1" {
		t.Fatal("SeedRootUser clobbered an existing root password")
	}

	// Update cannot escalate a non-root user to root.
	if err := s.UpdateUser(ctx, user.User{
		Username: "sneaky", Root: true,
		Grants: []user.Grant{{Action: user.ActionAdmin}},
	}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if esc, _ := s.GetUser(ctx, "sneaky"); esc.Root {
		t.Fatal("UpdateUser escalated a user to root")
	}

	// Update cannot tamper with root's grants, but may change its password.
	if err := s.UpdateUser(ctx, user.User{
		Username: "admin", PasswordHash: []byte("h3"),
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"x"}}},
	}); err != nil {
		t.Fatalf("UpdateUser(admin password): %v", err)
	}
	updated, _ := s.GetUser(ctx, "admin")
	if !updated.Root || !updated.IsAdmin() || string(updated.PasswordHash) != "h3" {
		t.Fatalf("root update wrong: root=%v admin=%v hash=%s", updated.Root, updated.IsAdmin(), updated.PasswordHash)
	}

	// Root cannot be deleted.
	if err := s.DeleteUser(ctx, "admin"); !errors.Is(err, metastore.ErrRootProtected) {
		t.Fatalf("DeleteUser(root) = %v, want ErrRootProtected", err)
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

// Field-scoped updates replace exactly one field of the record the FSM
// currently holds, never the copy the proposer read.
func TestFieldScopedUserUpdates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	alice := user.User{
		Username:     "alice",
		PasswordHash: []byte("h1"),
		Grants:       []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders-*"}}},
		CreatedAtMs:  1,
		UpdatedAtMs:  1,
	}
	if err := s.CreateUser(ctx, alice); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if err := s.SetUserGrants(ctx, "alice", nil, 2); err != nil {
		t.Fatalf("SetUserGrants: %v", err)
	}
	got, _ := s.GetUser(ctx, "alice")
	if len(got.Grants) != 0 || string(got.PasswordHash) != "h1" || got.UpdatedAtMs != 2 || got.CreatedAtMs != 1 {
		t.Fatalf("after SetUserGrants: %+v (grants must be cleared, hash and created_at untouched)", got)
	}

	if err := s.SetUserPassword(ctx, "alice", []byte("h2"), 3); err != nil {
		t.Fatalf("SetUserPassword: %v", err)
	}
	got, _ = s.GetUser(ctx, "alice")
	if string(got.PasswordHash) != "h2" || len(got.Grants) != 0 || got.UpdatedAtMs != 3 {
		t.Fatalf("after SetUserPassword: %+v (hash must change, grants stay revoked)", got)
	}

	if err := s.SetUserPassword(ctx, "ghost", []byte("h"), 1); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("SetUserPassword(ghost) = %v, want ErrNotFound", err)
	}
	if err := s.SetUserGrants(ctx, "ghost", nil, 1); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("SetUserGrants(ghost) = %v, want ErrNotFound", err)
	}

	// Root's grants are immutable at the FSM, not just in the HTTP layer;
	// root's password may still change.
	if err := s.SeedRootUser(ctx, user.User{Username: "admin", PasswordHash: []byte("r1"), Grants: []user.Grant{{Action: user.ActionAdmin}}}); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := s.SetUserGrants(ctx, "admin", nil, 4); !errors.Is(err, metastore.ErrRootProtected) {
		t.Fatalf("SetUserGrants(root) = %v, want ErrRootProtected", err)
	}
	if err := s.SetUserPassword(ctx, "admin", []byte("r2"), 5); err != nil {
		t.Fatalf("SetUserPassword(root): %v", err)
	}
	root, _ := s.GetUser(ctx, "admin")
	if !root.Root || !root.IsAdmin() || string(root.PasswordHash) != "r2" {
		t.Fatalf("root after password change: %+v", root)
	}

	// ApplyUserUpdate dispatches by field; an empty field is the legacy
	// whole-record replace and an unknown field is rejected.
	if err := s.ApplyUserUpdate(ctx, metastore.UserUpdate{Field: metastore.UserUpdatePassword, User: user.User{Username: "alice", PasswordHash: []byte("h3"), UpdatedAtMs: 6}}); err != nil {
		t.Fatalf("ApplyUserUpdate(password): %v", err)
	}
	if err := s.ApplyUserUpdate(ctx, metastore.UserUpdate{Field: metastore.UserUpdateGrants, User: user.User{Username: "alice", Grants: alice.Grants, UpdatedAtMs: 7}}); err != nil {
		t.Fatalf("ApplyUserUpdate(grants): %v", err)
	}
	got, _ = s.GetUser(ctx, "alice")
	if string(got.PasswordHash) != "h3" || len(got.Grants) != 1 || got.UpdatedAtMs != 7 {
		t.Fatalf("after dispatched updates: %+v", got)
	}
	if err := s.ApplyUserUpdate(ctx, metastore.UserUpdate{Field: "bogus", User: user.User{Username: "alice"}}); err == nil {
		t.Fatal("ApplyUserUpdate(bogus field) = nil, want error")
	}
	legacy := got
	legacy.PasswordHash = []byte("h4")
	if err := s.ApplyUserUpdate(ctx, metastore.UserUpdate{User: legacy}); err != nil {
		t.Fatalf("ApplyUserUpdate(legacy): %v", err)
	}
	if got, _ = s.GetUser(ctx, "alice"); string(got.PasswordHash) != "h4" {
		t.Fatalf("legacy whole-record update did not apply: %+v", got)
	}
}

// The scenario the field-scoped ops exist for: a proposer holds a stale
// copy of the record (a follower replica that has not applied a grant
// revocation yet) and changes the password. With whole-record updates
// the stale grants rode along and undid the revocation; with
// SetUserPassword only the hash moves.
func TestPasswordChangeFromStaleReadKeepsRevokedGrants(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, user.User{
		Username: "erin", PasswordHash: []byte("old"),
		Grants: []user.Grant{{Action: user.ActionAdmin}},
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	stale, _ := s.GetUser(ctx, "erin") // the lagging follower's view

	// The admin revokes erin's admin grant on the leader.
	if err := s.SetUserGrants(ctx, "erin", []user.Grant{{Action: user.ActionConsume, Patterns: []string{"logs"}}}, 2); err != nil {
		t.Fatalf("SetUserGrants: %v", err)
	}

	// erin's password change, proposed from the stale view.
	if err := s.SetUserPassword(ctx, stale.Username, []byte("new"), 3); err != nil {
		t.Fatalf("SetUserPassword: %v", err)
	}
	got, _ := s.GetUser(ctx, "erin")
	if got.IsAdmin() {
		t.Fatalf("revoked admin grant resurrected by a password change: %+v", got)
	}
	if string(got.PasswordHash) != "new" || len(got.Grants) != 1 || got.Grants[0].Action != user.ActionConsume {
		t.Fatalf("after password change: %+v", got)
	}
}
