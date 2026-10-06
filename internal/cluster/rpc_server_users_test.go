package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

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
	newHash := bcryptHash(t, "new")
	stale.PasswordHash = newHash
	body, _ := json.Marshal(metastore.UserUpdate{Field: metastore.UserUpdatePassword, User: stale})
	res := s.handleUpdateUser(encodeUpdateUserReq(t, "erin", body))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", res.Status, res.Body)
	}
	got, _ := store.GetUser(ctx, "erin")
	if string(got.PasswordHash) != string(newHash) || got.IsAdmin() {
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
	newHash := bcryptHash(t, "new")
	body, _ := json.Marshal(user.User{
		Username: "bob", PasswordHash: newHash,
		Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"x"}}}, UpdatedAtMs: 9,
	})
	res := newUsersRPCServer(store).handleUpdateUser(encodeUpdateUserReq(t, "bob", body))
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", res.Status, res.Body)
	}
	got, _ := store.GetUser(ctx, "bob")
	if string(got.PasswordHash) != string(newHash) || len(got.Grants) != 1 || got.UpdatedAtMs != 9 {
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
	body, _ = json.Marshal(metastore.UserUpdate{Field: metastore.UserUpdatePassword, User: user.User{Username: "ghost", PasswordHash: bcryptHash(t, "h")}})
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

// A member's heartbeat is stamped on the leader's clock when the
// registration is applied; the sender's own timestamp is ignored. A
// member with a slow clock used to be declared dead while healthy, and
// one with a fast clock was never declared dead after it died.
func TestRPCServerRegisterMemberStampsHeartbeatOnLeaderClock(t *testing.T) {
	store := newTestStore(t)
	leaderNow := time.Unix(1_700_000_000, 0)
	s := &RPCServer{store: store, logger: discardLogger(), now: func() time.Time { return leaderNow }}

	for _, sent := range []int64{0, 1, leaderNow.Unix() - 3600, leaderNow.Unix() + 3600} {
		payload, err := nodewire.EncodeMemberRequest(nodewire.MemberRequest{ID: "narad-9", Addr: "narad-9:7942", Status: "alive", LastHeartbeat: sent})
		if err != nil {
			t.Fatalf("EncodeMemberRequest: %v", err)
		}
		if res := roundTripRPC(t, s, payload); res.Status != http.StatusNoContent {
			t.Fatalf("register member status = %d: %s", res.Status, res.Body)
		}
		m, err := store.GetMember("narad-9")
		if err != nil {
			t.Fatalf("GetMember: %v", err)
		}
		if m.LastHeartbeat != leaderNow.Unix() {
			t.Fatalf("sent LastHeartbeat=%d: stored %d, want the leader's clock %d", sent, m.LastHeartbeat, leaderNow.Unix())
		}
	}
}

// bcryptHash is a real bcrypt hash at the minimum cost: the forwarded
// user RPC refuses a password hash that is not one.
func bcryptHash(t *testing.T, password string) []byte {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func encodeCreateUserReq(t *testing.T, u user.User) []byte {
	t.Helper()
	body, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := nodewire.EncodeUserRequest(nodewire.OpCreateUser, nodewire.UserRequest{Body: body})
	if err != nil {
		t.Fatalf("EncodeUserRequest: %v", err)
	}
	return payload
}

// The leader-forwarded user create validates what it proposes, as the
// HTTP API does: a name the ServeMux cleans (svc/../ghost, "..") would
// authenticate but could never be fetched, changed or deleted through
// the API, and a hash that is not bcrypt is a principal no password
// can sign in as. Each is a 400 and nothing is proposed.
func TestRPCServerCreateUserRefusesInvalidUsers(t *testing.T) {
	store := newTestStore(t)
	s := newUsersRPCServer(store)
	good := bcryptHash(t, "pw")
	produceAll := []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}
	cases := map[string]user.User{
		"path the mux cleans":  {Username: "svc/../ghost", PasswordHash: good, Grants: produceAll},
		"dot-dot":              {Username: "..", PasswordHash: good},
		"dot":                  {Username: ".", PasswordHash: good},
		"space":                {Username: "a b", PasswordHash: good},
		"empty name":           {Username: "", PasswordHash: good},
		"too long":             {Username: strings.Repeat("u", 65), PasswordHash: good},
		"unknown action":       {Username: "svc", PasswordHash: good, Grants: []user.Grant{{Action: "superuser"}}},
		"admin with patterns":  {Username: "svc", PasswordHash: good, Grants: []user.Grant{{Action: user.ActionAdmin, Patterns: []string{"x"}}}},
		"grant without topics": {Username: "svc", PasswordHash: good, Grants: []user.Grant{{Action: user.ActionConsume}}},
		"bad pattern":          {Username: "svc", PasswordHash: good, Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"a/b*"}}}},
		"no hash":              {Username: "svc"},
		"not bcrypt":           {Username: "svc", PasswordHash: []byte("eA==")},
		"plaintext as hash":    {Username: "svc", PasswordHash: []byte("hunter2")},
	}
	for name, u := range cases {
		res := s.handleCreateUser(encodeCreateUserReq(t, u))
		if res.Status != http.StatusBadRequest {
			t.Errorf("%s: create %q answered %d (%s), want 400", name, u.Username, res.Status, res.Body)
		}
	}
	users, err := store.ListUsers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 0 {
		names := make([]string, 0, len(users))
		for _, u := range users {
			names = append(names, u.Username)
		}
		t.Fatalf("invalid users were created over the RPC: %q", names)
	}

	res := s.handleCreateUser(encodeCreateUserReq(t, user.User{Username: "svc-ok", PasswordHash: good, Grants: produceAll}))
	if res.Status != http.StatusCreated {
		t.Fatalf("a valid create answered %d (%s), want 201", res.Status, res.Body)
	}
}

// A forwarded update checks the one field it replaces, and a legacy
// whole-record body the grants and hash it installs; the stored record
// is unchanged by a refused update.
func TestRPCServerUpdateUserRefusesInvalidFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	hash := bcryptHash(t, "pw")
	if err := store.CreateUser(ctx, user.User{Username: "dana", PasswordHash: hash}); err != nil {
		t.Fatal(err)
	}
	s := newUsersRPCServer(store)
	bad := map[string]metastore.UserUpdate{
		"grants: unknown action": {Field: metastore.UserUpdateGrants, User: user.User{Username: "dana", Grants: []user.Grant{{Action: "root"}}}},
		"grants: empty pattern":  {Field: metastore.UserUpdateGrants, User: user.User{Username: "dana", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{""}}}}},
		"password: not bcrypt":   {Field: metastore.UserUpdatePassword, User: user.User{Username: "dana", PasswordHash: []byte("x")}},
		"password: empty":        {Field: metastore.UserUpdatePassword, User: user.User{Username: "dana"}},
		"legacy: not bcrypt":     {User: user.User{Username: "dana", PasswordHash: []byte("x")}},
		"legacy: unknown action": {User: user.User{Username: "dana", PasswordHash: hash, Grants: []user.Grant{{Action: "root"}}}},
	}
	for name, upd := range bad {
		body, _ := json.Marshal(upd)
		if res := s.handleUpdateUser(encodeUpdateUserReq(t, "dana", body)); res.Status != http.StatusBadRequest {
			t.Errorf("%s: answered %d (%s), want 400", name, res.Status, res.Body)
		}
	}
	got, err := store.GetUser(ctx, "dana")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.PasswordHash) != string(hash) || len(got.Grants) != 0 {
		t.Fatalf("a refused update changed the stored record: %+v", got)
	}
}
