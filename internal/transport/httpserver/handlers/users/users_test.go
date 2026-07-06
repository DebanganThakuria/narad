package users_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	if err := s.CreateUser(context.Background(), u); err != nil {
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
