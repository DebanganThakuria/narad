package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func encodeUpdateUserReq(t *testing.T, username string, body []byte) []byte {
	t.Helper()
	payload, err := nodewire.EncodeUserRequest(nodewire.OpUpdateUser, nodewire.UserRequest{Username: username, Body: body})
	if err != nil {
		t.Fatalf("EncodeUserRequest: %v", err)
	}
	return payload
}

// A forwarded field-scoped update touches only that field on the
// leader, even though the body (as an older follower would send it)
// carries stale values for the others.
func TestRPCServerUpdateUserAppliesFieldScopedBody(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateUser(ctx, user.User{
		Username: "erin", PasswordHash: []byte("old"),
		Grants: []user.Grant{{Action: user.ActionAdmin}},
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	stale, _ := store.GetUser(ctx, "erin")
	if err := store.SetUserGrants(ctx, "erin", nil, 2); err != nil {
		t.Fatalf("SetUserGrants: %v", err)
	}

	s := &RPCServer{store: store, logger: discardLogger()}
	stale.PasswordHash = []byte("new")
	body, _ := json.Marshal(metastore.UserUpdate{Field: metastore.UserUpdatePassword, User: stale})
	res := s.handleUpdateUser(encodeUpdateUserReq(t, "erin", body))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", res.Status, res.Body)
	}
	got, _ := store.GetUser(ctx, "erin")
	if string(got.PasswordHash) != "new" || got.IsAdmin() {
		t.Fatalf("after forwarded password update: %+v (stale admin grant must not come back)", got)
	}
	// The response is the committed record, redacted.
	var out user.User
	if err := json.Unmarshal(res.Body, &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.IsAdmin() || len(out.PasswordHash) != 0 {
		t.Fatalf("response = %+v, want committed grants and no hash", out)
	}
}

// A body from an older build (a bare user.User, no field) still applies
// as a whole-record replace, so a rolling upgrade keeps user writes
// working in both directions.
func TestRPCServerUpdateUserAcceptsLegacyWholeRecordBody(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateUser(ctx, user.User{Username: "bob", PasswordHash: []byte("old")}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	body, _ := json.Marshal(user.User{
		Username: "bob", PasswordHash: []byte("new"),
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"x"}}}, UpdatedAtMs: 9,
	})
	res := newUsersRPCServer(store).handleUpdateUser(encodeUpdateUserReq(t, "bob", body))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", res.Status, res.Body)
	}
	got, _ := store.GetUser(ctx, "bob")
	if string(got.PasswordHash) != "new" || len(got.Grants) != 1 || got.UpdatedAtMs != 9 {
		t.Fatalf("legacy body did not apply as whole-record replace: %+v", got)
	}
}

func newUsersRPCServer(store *metastore.Store) *RPCServer {
	return &RPCServer{store: store, logger: discardLogger()}
}

func TestRPCServerUpdateUserMapsErrors(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.SeedRootUser(ctx, user.User{Username: "admin", Grants: []user.Grant{{Action: user.ActionAdmin}}}); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	s := newUsersRPCServer(store)

	body, _ := json.Marshal(metastore.UserUpdate{Field: metastore.UserUpdateGrants, User: user.User{Username: "admin"}})
	if res := s.handleUpdateUser(encodeUpdateUserReq(t, "admin", body)); res.Status != http.StatusForbidden {
		t.Fatalf("root grants update status = %d, want 403", res.Status)
	}
	body, _ = json.Marshal(metastore.UserUpdate{Field: metastore.UserUpdatePassword, User: user.User{Username: "ghost", PasswordHash: []byte("h")}})
	if res := s.handleUpdateUser(encodeUpdateUserReq(t, "ghost", body)); res.Status != http.StatusNotFound {
		t.Fatalf("missing user status = %d, want 404", res.Status)
	}
	body, _ = json.Marshal(metastore.UserUpdate{Field: "bogus", User: user.User{Username: "admin"}})
	if res := s.handleUpdateUser(encodeUpdateUserReq(t, "admin", body)); res.Status != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", res.Status)
	}
}

// The RPC-side alter validation mirrors the HTTP handler: a negative
// partition count is rejected up front with a message that explains 0
// leaves the count unchanged.
func TestRPCAlterBodyRejectsNegativePartitions(t *testing.T) {
	err := rpcAlterTopicBody{Partitions: -1}.validate()
	if err == nil || !strings.Contains(err.Error(), "unchanged") {
		t.Fatalf("validate() = %v, want a negative-partitions error mentioning unchanged", err)
	}
	if err := (rpcAlterTopicBody{Partitions: 6}).validate(); err != nil {
		t.Fatalf("validate(6) = %v, want nil", err)
	}
}
