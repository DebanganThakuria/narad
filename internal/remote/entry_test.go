package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
)

// tlsServer starts an httptest TLS server and returns it with its CA
// bundle, which a static entry pins as its only root.
func tlsServer(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	return srv, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
}

func staticEntryFor(t *testing.T, url, caPEM, username, password string) *Entry {
	t.Helper()
	e, err := NewStaticEntry(StaticEntryConfig{
		Name: "b", RemoteID: "a41c07d9e25b3f60", URL: url, Username: username,
		Password: domremote.NewSecret([]byte(password)), CAPEM: caPEM, CredentialVersion: 3,
	})
	if err != nil {
		t.Fatalf("NewStaticEntry: %v", err)
	}
	return e
}

func TestStaticEntrySendsTheHeaderOverVerifiedTLS(t *testing.T) {
	var got atomic.Value
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Clone())
		if r.URL.Path != "/prefix/v1/topics/orders/produce/batch" || r.URL.RawQuery != "" {
			t.Errorf("path = %q query = %q", r.URL.Path, r.URL.RawQuery)
		}
		if r.ProtoMajor != 1 {
			t.Errorf("proto = %s, want HTTP/1.1", r.Proto)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	e := staticEntryFor(t, srv.URL+"/prefix/", ca, "repl-a", "pw-1")
	path, err := TopicPath("orders", "produce", "batch")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.Do(context.Background(), Outbound{Method: http.MethodPost, Path: path, Body: []byte(`{"messages":[]}`), ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_, _ = ReadBody(resp, MaxProduceAnswerBytes)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	h := got.Load().(http.Header)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("repl-a:pw-1"))
	if h.Get("Authorization") != want {
		t.Fatalf("Authorization = %q, want %q", h.Get("Authorization"), want)
	}
	if !strings.HasPrefix(h.Get("X-Narad-Client"), "narad-replicator/") || h.Get("User-Agent") != h.Get("X-Narad-Client") {
		t.Fatalf("client headers = %q / %q", h.Get("X-Narad-Client"), h.Get("User-Agent"))
	}
	if h.Get("Content-Type") != "application/json" || h.Get("Expect") != "" || h.Get("Idempotency-Key") != "" {
		t.Fatalf("headers = %v", h)
	}
}

func TestStaticEntryRefusesAnUnknownCA(t *testing.T) {
	srv, _ := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	_, otherCA := selfSignedTLS(t)
	e := staticEntryFor(t, srv.URL, otherCA, "u", "p")
	_, err := e.Do(context.Background(), Outbound{Method: http.MethodGet, Path: UsersPath()})
	if err == nil || !IsTLSError(err) {
		t.Fatalf("Do against a server outside the pinned CA: err = %v, want a TLS error", err)
	}
}

func TestStaticEntryNeverFollowsARedirect(t *testing.T) {
	var elsewhere atomic.Int64
	other, _ := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhere.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/v1/users", http.StatusFound)
	}))
	e := staticEntryFor(t, srv.URL, ca, "u", "p")
	for _, out := range []Outbound{
		{Method: http.MethodGet, Path: UsersPath()},
		{Method: http.MethodPost, Path: "/v1/topics/orders/produce/batch", Body: []byte(`{}`), ContentType: "application/json"},
	} {
		resp, err := e.Do(context.Background(), out)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		_, _ = ReadBody(resp, 1024)
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("status = %d, want the 302 as is", resp.StatusCode)
		}
	}
	if n := elsewhere.Load(); n != 0 {
		t.Fatalf("the Location host got %d requests, want 0", n)
	}
}

func TestEntryPrintsItsNameOnly(t *testing.T) {
	srv, ca := tlsServer(t, http.NotFoundHandler())
	e := staticEntryFor(t, srv.URL, ca, "repl-canary-user", "canary-password-value")
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	log.Info("entry", "e", e)
	printed := fmt.Sprintf("%v %+v %#v %s %q %x", e, e, e, e, e, e) + buf.String()
	for _, secret := range []string{"canary-password-value", "repl-canary-user", "Basic", base64.StdEncoding.EncodeToString([]byte("repl-canary-user:canary-password-value"))} {
		if strings.Contains(printed, secret) {
			t.Fatalf("printed entry contains %q: %s", secret, printed)
		}
	}
	if !strings.Contains(printed, "b") {
		t.Fatalf("printed entry lacks the name: %s", printed)
	}
}

func TestDoRefusesRequestsTheSendPathNeverBuilds(t *testing.T) {
	var hits atomic.Int64
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits.Add(1) }))
	e := staticEntryFor(t, srv.URL, ca, "u", "p")
	for _, out := range []Outbound{
		{Method: http.MethodDelete, Path: "/v1/users"},
		{Method: http.MethodGet, Path: "/v2/users"},
		{Method: http.MethodGet, Path: "/v1/topics/../users"},
		{Method: http.MethodGet, Path: "/v1/topics/a?key=x"},
		{Method: http.MethodGet, Path: "/v1/topics/a#f"},
		{Method: http.MethodGet, Path: "/v1//users"},
		{Method: http.MethodPost, Path: "/v1/users", ContentEncoding: "gzip"},
	} {
		if _, err := e.Do(context.Background(), out); err == nil {
			t.Fatalf("Do(%+v) = nil error, want a refusal", out)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("server saw %d requests, want 0", hits.Load())
	}
}

func TestNewStaticEntryRefusesBadURLsAndBundles(t *testing.T) {
	for _, raw := range []string{"http://h", "https://", "https://u:p@h", "https://h?x=1", "https://h#f", "ftp://h"} {
		if _, err := NewStaticEntry(StaticEntryConfig{Name: "b", URL: raw}); err == nil {
			t.Fatalf("NewStaticEntry(%q) = nil error", raw)
		}
	}
	for _, ca := range []string{"not pem", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"} {
		if _, err := NewStaticEntry(StaticEntryConfig{Name: "b", URL: "https://h", CAPEM: ca}); err == nil {
			t.Fatalf("NewStaticEntry(ca %q) = nil error", ca)
		}
	}
}
