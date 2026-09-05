package users_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpusers "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/users"
)

func newStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{
		NodeID: "users-0", DataDir: t.TempDir(),
		BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateUser(context.Background(), user.User{Username: "__probe__"}); err == nil {
			_ = s.DeleteUser(context.Background(), "__probe__")
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

// stubBroker satisfies handlers.New's non-nil Broker requirement; the
// user handlers only touch Metastore, so its methods are never called
// (embedding the interface makes any accidental call panic).
type stubBroker struct{ broker.Broker }

func newSet(t *testing.T, s *metastore.Store) *handlers.Set {
	t.Helper()
	return handlers.New(handlers.Deps{
		Broker:    stubBroker{},
		Metastore: s,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func seedUser(t *testing.T, s *metastore.Store, u user.User, password string) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	u.PasswordHash = hash
	create := s.CreateUser
	if u.Root {
		create = s.SeedRootUser
	}
	if err := create(context.Background(), u); err != nil {
		t.Fatalf("seed %s: %v", u.Username, err)
	}
}

func asUser(r *http.Request, u user.User) *http.Request {
	return r.WithContext(security.WithIdentity(r.Context(), u))
}

func TestCreateUserAdminOnlyAndNoEscalation(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)

	// Non-admin caller is rejected.
	req := asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"x","password":"p"}`)),
		user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}})
	res := httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("non-admin create: status = %d, want 403", res.Code)
	}

	admin := user.User{Username: "root", Root: true}
	req = asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"alice","password":"pw","grants":[{"action":"produce","patterns":["orders-*"]}]}`)),
		admin)
	res = httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("admin create: status = %d, body %s", res.Code, res.Body)
	}

	// Response must not leak the password hash.
	if bytes.Contains(res.Body.Bytes(), []byte("password_hash")) || bytes.Contains(res.Body.Bytes(), []byte("$2")) {
		t.Fatalf("create response leaked hash: %s", res.Body)
	}

	// The stored user has a hash and the right grants.
	stored, err := s.GetUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if len(stored.PasswordHash) == 0 || !stored.Allowed(user.ActionProduce, "orders-eu") {
		t.Fatalf("stored user wrong: %+v", stored)
	}
}

func TestOnlyRootMayConferAdmin(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)

	// A non-root admin cannot mint another admin.
	nonRootAdmin := user.User{Username: "team-lead", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	req := asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"evil","password":"pw","grants":[{"action":"admin"}]}`)),
		nonRootAdmin)
	res := httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("non-root granting admin: status = %d, want 403 (body %s)", res.Code, res.Body)
	}

	// Root may.
	root := user.User{Username: "root", Root: true}
	req = asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"deputy","password":"pw","grants":[{"action":"admin"}]}`)),
		root)
	res = httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("root granting admin: status = %d, want 201 (body %s)", res.Code, res.Body)
	}
}

func TestDeleteRootAndSelfForbidden(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "root", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}, "rootpw")
	admin := user.User{Username: "root", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}

	// Root cannot be deleted.
	req := asUser(httptest.NewRequest(http.MethodDelete, "/v1/users/root", nil), admin)
	req.SetPathValue("username", "root")
	res := httptest.NewRecorder()
	httpusers.Delete(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("delete root: status = %d, want 403", res.Code)
	}

	// A user cannot delete themselves.
	seedUser(t, s, user.User{Username: "carol", Grants: []user.Grant{{Action: user.ActionAdmin}}}, "pw")
	self := user.User{Username: "carol", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	req = asUser(httptest.NewRequest(http.MethodDelete, "/v1/users/carol", nil), self)
	req.SetPathValue("username", "carol")
	res = httptest.NewRecorder()
	httpusers.Delete(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("self delete: status = %d, want 403", res.Code)
	}
}

func TestUpdateGrantsRejectsSelfAndRoot(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "root", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}, "pw")
	admin := user.User{Username: "root", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}

	// Cannot edit root's grants.
	req := asUser(httptest.NewRequest(http.MethodPut, "/v1/users/root/grants",
		bytes.NewBufferString(`{"grants":[{"action":"produce","patterns":["x"]}]}`)), admin)
	req.SetPathValue("username", "root")
	res := httptest.NewRecorder()
	httpusers.UpdateGrants(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("edit root grants: status = %d, want 403 (%s)", res.Code, res.Body)
	}

	// Cannot edit your own grants.
	seedUser(t, s, user.User{Username: "dave", Grants: []user.Grant{{Action: user.ActionAdmin}}}, "pw")
	self := user.User{Username: "dave", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/dave/grants",
		bytes.NewBufferString(`{"grants":[{"action":"produce","patterns":["x"]}]}`)), self)
	req.SetPathValue("username", "dave")
	res = httptest.NewRecorder()
	httpusers.UpdateGrants(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("self grant edit: status = %d, want 403 (%s)", res.Code, res.Body)
	}
}

func TestSelfServicePasswordChange(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "erin", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"logs"}}}}, "oldpw")
	erin, _ := s.GetUser(context.Background(), "erin")

	// Wrong current password is rejected.
	req := asUser(httptest.NewRequest(http.MethodPut, "/v1/users/erin/password",
		bytes.NewBufferString(`{"current_password":"nope","new_password":"newpw"}`)), erin)
	req.SetPathValue("username", "erin")
	res := httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("wrong current password: status = %d, want 403", res.Code)
	}

	// Correct current password succeeds and the new hash verifies.
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/erin/password",
		bytes.NewBufferString(`{"current_password":"oldpw","new_password":"newpw"}`)), erin)
	req.SetPathValue("username", "erin")
	res = httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("password change: status = %d, want 204 (%s)", res.Code, res.Body)
	}
	updated, _ := s.GetUser(context.Background(), "erin")
	if bcrypt.CompareHashAndPassword(updated.PasswordHash, []byte("newpw")) != nil {
		t.Fatal("new password does not verify after change")
	}
	// Grants must be untouched by a password change.
	if !updated.Allowed(user.ActionConsume, "logs") {
		t.Fatal("password change clobbered grants")
	}
}

func TestRootPasswordChangeRestrictedToRoot(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "admin", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}, "rootpw")
	seedUser(t, s, user.User{Username: "ops", Grants: []user.Grant{{Action: user.ActionAdmin}}}, "opspw")

	// A non-root admin may NOT reset the root password (escalation guard).
	ops := user.User{Username: "ops", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	req := asUser(httptest.NewRequest(http.MethodPut, "/v1/users/admin/password",
		bytes.NewBufferString(`{"new_password":"pwned"}`)), ops)
	req.SetPathValue("username", "admin")
	res := httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("non-root admin resetting root: status = %d, want 403 (%s)", res.Code, res.Body)
	}
	// Root's password must be unchanged.
	root, _ := s.GetUser(context.Background(), "admin")
	if bcrypt.CompareHashAndPassword(root.PasswordHash, []byte("rootpw")) != nil {
		t.Fatal("root password was altered by a non-root admin")
	}

	// Root may change its own password.
	rootID := user.User{Username: "admin", Root: true, Grants: []user.Grant{{Action: user.ActionAdmin}}}
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/admin/password",
		bytes.NewBufferString(`{"new_password":"newrootpw"}`)), rootID)
	req.SetPathValue("username", "admin")
	res = httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("root changing own password: status = %d, want 204 (%s)", res.Code, res.Body)
	}
	root, _ = s.GetUser(context.Background(), "admin")
	if bcrypt.CompareHashAndPassword(root.PasswordHash, []byte("newrootpw")) != nil {
		t.Fatal("root self password change did not take effect")
	}
}

func TestListRedactsHashes(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "u1"}, "pw")
	admin := user.User{Username: "root", Root: true}

	req := asUser(httptest.NewRequest(http.MethodGet, "/v1/users", nil), admin)
	res := httptest.NewRecorder()
	httpusers.List(set).ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("list: status = %d", res.Code)
	}
	var out []map[string]json.RawMessage
	if err := json.Unmarshal(res.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, u := range out {
		if _, ok := u["password_hash"]; ok {
			t.Fatal("list leaked password_hash")
		}
	}
}

// forwardingRouter is a handlers.Router whose RouteUpdateUser captures
// the body an ingress node would send to the leader. Every other method
// panics on the embedded nil interface, which is fine: only user
// updates are exercised.
type forwardingRouter struct {
	handlers.Router
	forwarded []byte
}

func (f *forwardingRouter) RouteUpdateUser(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, body []byte) bool {
	f.forwarded = append([]byte(nil), body...)
	w.WriteHeader(http.StatusNoContent)
	return true
}

// The lagging-follower scenario end to end: erin changes her own password
// on a follower whose replica still shows the admin grant an admin has
// since revoked on the leader. The forwarded update must carry only the
// new hash, so applying it on the leader (whose replica already has the
// revocation) leaves the grants revoked.
func TestSelfServicePasswordChangeOnLaggingFollowerKeepsRevocation(t *testing.T) {
	leader := newStore(t)
	ctx := context.Background()
	seedUser(t, leader, user.User{Username: "erin", Grants: []user.Grant{{Action: user.ActionAdmin}}}, "oldpw")

	// The follower's stale view of erin: admin grant still present.
	follower := newStore(t)
	stale, _ := leader.GetUser(ctx, "erin")
	if err := follower.CreateUser(ctx, stale); err != nil {
		t.Fatalf("seed follower replica: %v", err)
	}

	// Meanwhile the admin revokes erin's admin grant on the leader.
	if err := leader.SetUserGrants(ctx, "erin", []user.Grant{{Action: user.ActionConsume, Patterns: []string{"logs"}}}, time.Now().UnixMilli()); err != nil {
		t.Fatalf("SetUserGrants: %v", err)
	}

	// erin's password change lands on the follower, which forwards it.
	router := &forwardingRouter{}
	set := handlers.New(handlers.Deps{
		Broker:    stubBroker{},
		Metastore: follower,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router:    router,
	})
	erin := stale // the identity the follower authenticated, admin grant and all
	req := asUser(httptest.NewRequest(http.MethodPut, "/v1/users/erin/password",
		bytes.NewBufferString(`{"current_password":"oldpw","new_password":"newpw"}`)), erin)
	req.SetPathValue("username", "erin")
	res := httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("password change: status = %d, want 204 (%s)", res.Code, res.Body)
	}
	if router.forwarded == nil {
		t.Fatal("password change was not forwarded to the leader")
	}

	// The leader applies the forwarded body.
	var upd metastore.UserUpdate
	if err := json.Unmarshal(router.forwarded, &upd); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	if upd.Field != metastore.UserUpdatePassword {
		t.Fatalf("forwarded field = %q, want %q", upd.Field, metastore.UserUpdatePassword)
	}
	if len(upd.Grants) != 0 {
		t.Fatalf("forwarded body carries grants %+v; a password change must not send grants at all", upd.Grants)
	}
	if err := leader.ApplyUserUpdate(ctx, upd); err != nil {
		t.Fatalf("leader ApplyUserUpdate: %v", err)
	}

	got, _ := leader.GetUser(ctx, "erin")
	if got.IsAdmin() {
		t.Fatalf("revoked admin grant resurrected by a password change from a lagging follower: %+v", got.Grants)
	}
	if bcrypt.CompareHashAndPassword(got.PasswordHash, []byte("newpw")) != nil {
		t.Fatal("new password does not verify on the leader")
	}
	if !got.Allowed(user.ActionConsume, "logs") {
		t.Fatalf("grants after password change = %+v, want the admin's revocation to stand", got.Grants)
	}
}

// A grants update sends only grants: the password hash on the leader is
// untouched even when the follower's copy of the hash is stale, and the
// response reflects the committed record.
func TestUpdateGrantsIsFieldScoped(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "frank"}, "pw1")
	ctx := context.Background()
	admin := user.User{Username: "root", Root: true}

	req := asUser(httptest.NewRequest(http.MethodPut, "/v1/users/frank/grants",
		bytes.NewBufferString(`{"grants":[{"action":"produce","patterns":["x"]}]}`)), admin)
	req.SetPathValue("username", "frank")
	res := httptest.NewRecorder()
	httpusers.UpdateGrants(set).ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("grants update: status = %d (%s)", res.Code, res.Body)
	}
	var out struct {
		Grants      []user.Grant `json:"grants"`
		UpdatedAtMs int64        `json:"updated_at_ms"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, _ := s.GetUser(ctx, "frank")
	if len(out.Grants) != 1 || out.UpdatedAtMs != got.UpdatedAtMs {
		t.Fatalf("response %+v does not match committed record %+v", out, got)
	}
	if bcrypt.CompareHashAndPassword(got.PasswordHash, []byte("pw1")) != nil {
		t.Fatal("grants update clobbered the password hash")
	}
}

// bcrypt rejects passwords over 72 bytes. That must surface as the
// client's 400 before any hashing, on create and on both password-change
// paths, never as a 500 "hash password" with a server error log line.
func TestPasswordLengthIsValidatedUpFront(t *testing.T) {
	s := newStore(t)
	set := newSet(t, s)
	seedUser(t, s, user.User{Username: "gina"}, "pw")
	admin := user.User{Username: "root", Root: true}
	tooLong := strings.Repeat("x", user.MaxPasswordBytes+1)
	maxLen := strings.Repeat("y", user.MaxPasswordBytes)

	// Create: 73 bytes is 400, 72 bytes is 201.
	req := asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"h","password":"`+tooLong+`"}`)), admin)
	res := httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("create with 73-byte password: status = %d, want 400 (%s)", res.Code, res.Body)
	}
	req = asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"h","password":"`+maxLen+`"}`)), admin)
	res = httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("create with 72-byte password: status = %d, want 201 (%s)", res.Code, res.Body)
	}

	// Admin reset.
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/gina/password",
		bytes.NewBufferString(`{"new_password":"`+tooLong+`"}`)), admin)
	req.SetPathValue("username", "gina")
	res = httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("admin reset with 73-byte password: status = %d, want 400 (%s)", res.Code, res.Body)
	}

	// Self-service: validated before the current-password check, so a
	// bad new password does not cost a bcrypt compare either.
	gina, _ := s.GetUser(context.Background(), "gina")
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/gina/password",
		bytes.NewBufferString(`{"current_password":"pw","new_password":"`+tooLong+`"}`)), gina)
	req.SetPathValue("username", "gina")
	res = httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("self-service with 73-byte password: status = %d, want 400 (%s)", res.Code, res.Body)
	}
	if bcrypt.CompareHashAndPassword(gina.PasswordHash, []byte("pw")) != nil {
		t.Fatal("rejected password change altered the stored hash")
	}
}

// countingHasher stands in for the authenticator: every bcrypt run the
// handlers do must go through it, so the process-wide bound applies.
type countingHasher struct {
	hashes, compares int
}

func (c *countingHasher) HashPassword(_ context.Context, password string) ([]byte, error) {
	c.hashes++
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
}

func (c *countingHasher) ComparePassword(_ context.Context, hash []byte, password string) error {
	c.compares++
	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}

func TestPasswordWorkGoesThroughDepsPasswords(t *testing.T) {
	s := newStore(t)
	hasher := &countingHasher{}
	set := handlers.New(handlers.Deps{
		Broker:    stubBroker{},
		Metastore: s,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Passwords: hasher,
	})
	admin := user.User{Username: "root", Root: true}

	req := asUser(httptest.NewRequest(http.MethodPost, "/v1/users",
		bytes.NewBufferString(`{"username":"hal","password":"pw1"}`)), admin)
	res := httptest.NewRecorder()
	httpusers.Create(set).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("create: status = %d (%s)", res.Code, res.Body)
	}
	if hasher.hashes != 1 {
		t.Fatalf("create hashed %d times through Deps.Passwords, want 1", hasher.hashes)
	}

	hal, _ := s.GetUser(context.Background(), "hal")
	req = asUser(httptest.NewRequest(http.MethodPut, "/v1/users/hal/password",
		bytes.NewBufferString(`{"current_password":"pw1","new_password":"pw2"}`)), hal)
	req.SetPathValue("username", "hal")
	res = httptest.NewRecorder()
	httpusers.UpdatePassword(set).ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("self-service change: status = %d (%s)", res.Code, res.Body)
	}
	if hasher.compares != 1 || hasher.hashes != 2 {
		t.Fatalf("self-service change: compares = %d hashes = %d, want 1 and 2 (both under the bound)", hasher.compares, hasher.hashes)
	}
}
