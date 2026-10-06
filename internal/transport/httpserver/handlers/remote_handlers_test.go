package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/security"
)

func remoteTestSet(log *slog.Logger) *Set {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return New(Deps{Broker: &fakeBroker{}, Logger: log})
}

func errorOf(t *testing.T, body []byte) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("error body %q: %v", body, err)
	}
	return e.Error
}

func TestRequireSecuredAdmin(t *testing.T) {
	s := remoteTestSet(nil)
	cases := []struct {
		name   string
		id     *user.User
		ok     bool
		status int
		msg    string
	}{
		{"no identity (security off)", nil, false, http.StatusForbidden, "remotes require security"},
		{"not an admin", &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}}, false, http.StatusForbidden, "admin privileges required"},
		{"admin", &user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionAdmin}}}, true, 0, ""},
		{"root", &user.User{Username: "admin", Root: true}, true, 0, ""},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/v1/remotes", nil)
		if c.id != nil {
			r = r.WithContext(security.WithIdentity(r.Context(), *c.id))
		}
		w := httptest.NewRecorder()
		got, ok := s.RequireSecuredAdmin(w, r)
		if ok != c.ok {
			t.Fatalf("%s: ok = %v, want %v", c.name, ok, c.ok)
		}
		if !ok {
			if w.Code != c.status || errorOf(t, w.Body.Bytes()) != c.msg {
				t.Fatalf("%s: %d %q, want %d %q", c.name, w.Code, w.Body.String(), c.status, c.msg)
			}
			continue
		}
		if got.Username != c.id.Username || w.Body.Len() != 0 {
			t.Fatalf("%s: user %q, body %q", c.name, got.Username, w.Body.String())
		}
	}
}

func TestRemoteChildConflictIs409(t *testing.T) {
	s := remoteTestSet(nil)
	for _, err := range []error{
		errs.ErrRemoteChildConflict,
		errs.ErrRemoteChildLocal,
		errs.ErrRemoteStubImmutable,
		fmt.Errorf("remote child %q lives on remote b; consume it there: %w", "orders-to-b", errs.ErrRemoteChildLocal),
	} {
		w := httptest.NewRecorder()
		s.WriteBrokerError(w, "produce", err)
		if w.Code != http.StatusConflict {
			t.Fatalf("WriteBrokerError(%v) = %d, want 409", err, w.Code)
		}
	}
}

func TestWriteRemoteResponseIsNeverCached(t *testing.T) {
	s := remoteTestSet(nil)
	for _, res := range []nodewire.Response{
		{Status: http.StatusCreated, ContentType: nodewire.ContentTypeJSON, Body: []byte(`{"name":"b"}` + "\n")},
		{Status: http.StatusNoContent},
		{Status: http.StatusConflict, ContentType: nodewire.ContentTypeJSON, Body: []byte(`{"error":"remote already exists"}`)},
	} {
		w := httptest.NewRecorder()
		s.WriteRemoteResponse(w, res)
		if w.Code != res.Status || w.Header().Get("Cache-Control") != "no-store" || !bytes.Equal(w.Body.Bytes(), res.Body) {
			t.Fatalf("WriteRemoteResponse(%d) = %d %q %q", res.Status, w.Code, w.Header().Get("Cache-Control"), w.Body.String())
		}
	}
}

func TestARemoteRequestIsAuditedLikeEveryAdminMutation(t *testing.T) {
	var buf bytes.Buffer
	s := remoteTestSet(slog.New(slog.NewJSONHandler(&buf, nil)))
	r := httptest.NewRequest(http.MethodPost, "/v1/remotes", nil)
	r = r.WithContext(security.WithIdentity(r.Context(), user.User{Username: "alice", Root: true}))
	aw := NewAuditWriter(httptest.NewRecorder())
	aw.WriteHeader(http.StatusForbidden)
	aw.Audit(s, r, "remote.create", "b", "request_id", "0123456789abcdef", slog.String("host", "narad-b.example"))
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("audit line %q: %v", buf.String(), err)
	}
	want := map[string]any{
		"component": "audit", "event": "remote.create", "actor": "alice",
		"request_id": "0123456789abcdef", "target": "b", "outcome": AuditDenied, "host": "narad-b.example",
	}
	for k, v := range want {
		if line[k] != v {
			t.Fatalf("audit line %s = %v, want %v (%s)", k, line[k], v, strings.TrimSpace(buf.String()))
		}
	}
}
