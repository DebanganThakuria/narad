package main

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// remoteAPI records the requests the CLI sends.
type remoteAPI struct {
	mu       sync.Mutex
	requests []recorded
	answer   func(r *http.Request) (int, string)
}

type recorded struct {
	method, path, query string
	body                map[string]any
	user                string
}

func (a *remoteAPI) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		user, _, _ := r.BasicAuth()
		a.mu.Lock()
		a.requests = append(a.requests, recorded{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, body: body, user: user})
		a.mu.Unlock()
		status, out := http.StatusOK, `{}`
		if a.answer != nil {
			status, out = a.answer(r)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(out))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (a *remoteAPI) last(t *testing.T) recorded {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) == 0 {
		t.Fatal("no request reached the API")
	}
	return a.requests[len(a.requests)-1]
}

func (a *remoteAPI) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests)
}

func resetRemoteCLI(t *testing.T) {
	t.Helper()
	withTempConfigDir(t)
	clearConnEnv(t)
	prev := cliStdin
	t.Cleanup(func() {
		cliStdin = prev
		flagServer, flagUser, flagPassword, flagPasswordStdin, flagCtx = "", "", "", false, ""
	})
}

func TestRemoteAddReadsThePasswordFromStdinOnly(t *testing.T) {
	resetRemoteCLI(t)
	api := &remoteAPI{}
	srv := api.serve(t)
	cliStdin = strings.NewReader("the-remote-password-0123456789\n")
	err := route([]string{
		"remote", "add", "b", "--server", srv.URL, "--user", "alice", "--password", "pw",
		"--url", "https://narad-b.example", "--username", "repl", "--remote-password-stdin", "--max-in-flight", "48", "--compression", "zstd",
	})
	if err != nil {
		t.Fatalf("remote add: %v", err)
	}
	req := api.last(t)
	limits, _ := req.body["limits"].(map[string]any)
	if req.method != http.MethodPost || req.path != "/v1/remotes" || req.body["password"] != "the-remote-password-0123456789" ||
		req.body["url"] != "https://narad-b.example" || limits["max_in_flight"] != 48.0 || limits["compression"] != "zstd" || req.user != "alice" {
		t.Fatalf("request = %+v", req)
	}

	// There is no argv flag for the remote password.
	if err := route([]string{"remote", "add", "c", "--server", srv.URL, "--url", "https://x", "--username", "u", "--remote-password", "p"}); err == nil {
		t.Fatal("a remote password on argv was accepted")
	}
	// Without --remote-password-stdin nothing is sent.
	n := api.count()
	if err := route([]string{"remote", "add", "c", "--server", srv.URL, "--url", "https://x", "--username", "u"}); err == nil || !strings.Contains(err.Error(), "--remote-password-stdin is required") {
		t.Fatalf("add without the password: %v", err)
	}
	// The caller's own --password-stdin reads the same stdin: refused.
	cliStdin = strings.NewReader("x\n")
	if err := route([]string{"remote", "add", "c", "--server", srv.URL, "--password-stdin", "--url", "https://x", "--username", "u", "--remote-password-stdin"}); err == nil || !strings.Contains(err.Error(), "both read stdin") {
		t.Fatalf("both stdin flags: %v", err)
	}
	if api.count() != n {
		t.Fatal("a refused command reached the API")
	}
}

func TestRemotePasswordRefusedOverPlainHTTPOffBox(t *testing.T) {
	resetRemoteCLI(t)
	cliStdin = strings.NewReader("the-remote-password-0123456789\n")
	err := route([]string{"remote", "add", "b", "--server", "http://10.0.0.5:7942", "--url", "https://narad-b.example", "--username", "repl", "--remote-password-stdin"})
	if err == nil || !strings.Contains(err.Error(), "plain http") {
		t.Fatalf("remote password over plain http off-box: %v", err)
	}
	cliStdin = strings.NewReader("the-remote-password-0123456789\n")
	err = route([]string{"remote", "set", "b", "--server", "http://narad.internal:7942", "--remote-password-stdin"})
	if err == nil || !strings.Contains(err.Error(), "plain http") {
		t.Fatalf("set over plain http off-box: %v", err)
	}
	for _, server := range []string{"http://127.0.0.1:1", "http://localhost:1", "http://[::1]:1", "https://narad.example"} {
		if plaintextOffBox(server) {
			t.Fatalf("%s refused", server)
		}
	}
}

func TestRemoteSetNeedsThePasswordForBoundFields(t *testing.T) {
	resetRemoteCLI(t)
	api := &remoteAPI{}
	srv := api.serve(t)
	for _, args := range [][]string{{"--url", "https://y"}, {"--username", "u2"}, {"--ca-file", "ca.pem"}, {"--no-ca"}} {
		err := route(append([]string{"remote", "set", "b", "--server", srv.URL}, args...))
		if err == nil || !strings.Contains(err.Error(), "--remote-password-stdin") {
			t.Fatalf("set %v without the password: %v", args, err)
		}
	}
	if api.count() != 0 {
		t.Fatal("a refused set reached the API")
	}
	if err := route([]string{"remote", "set", "b", "--server", srv.URL, "--max-in-flight", "64"}); err != nil {
		t.Fatalf("limits-only set: %v", err)
	}
	req := api.last(t)
	if req.method != http.MethodPatch || req.path != "/v1/remotes/b" || req.body["password"] != nil || req.body["limits"].(map[string]any)["max_in_flight"] != 64.0 {
		t.Fatalf("request = %+v", req)
	}
	cliStdin = strings.NewReader("new-remote-password-0123456789\n")
	if err := route([]string{"remote", "set", "b", "--server", srv.URL, "--no-ca", "--remote-password-stdin"}); err != nil {
		t.Fatalf("set --no-ca with the password: %v", err)
	}
	if req = api.last(t); req.body["ca_pem"] != "" || req.body["password"] != "new-remote-password-0123456789" {
		t.Fatalf("request = %+v", req)
	}
}

func TestRemoteRmLsTestReencrypt(t *testing.T) {
	resetRemoteCLI(t)
	api := &remoteAPI{answer: func(r *http.Request) (int, string) {
		if strings.HasSuffix(r.URL.Path, "/test") {
			var body map[string]any
			if r.URL.Path == "/v1/remotes/bad/test" {
				return http.StatusOK, `{"remote":"bad","result":"fail","class":"auth_failed","checks":[]}`
			}
			_ = body
			return http.StatusOK, `{"remote":"b","result":"pass","checks":[]}`
		}
		if r.Method == http.MethodDelete {
			return http.StatusNoContent, ``
		}
		return http.StatusOK, `{}`
	}}
	srv := api.serve(t)
	steps := []struct {
		args          []string
		method, path  string
		query         string
		wantErrSubstr string
	}{
		{[]string{"remote", "rm", "b", "--force"}, http.MethodDelete, "/v1/remotes/b", "force=true", ""},
		{[]string{"remote", "ls", "--no-nodes"}, http.MethodGet, "/v1/remotes", "nodes=false", ""},
		{[]string{"remote", "test", "b", "--topic", "orders", "--source", "orders"}, http.MethodPost, "/v1/remotes/b/test", "", ""},
		{[]string{"remote", "test", "bad", "--topic", "orders"}, http.MethodPost, "/v1/remotes/bad/test", "", "auth_failed"},
		{[]string{"remote", "reencrypt"}, http.MethodPost, "/v1/cluster/reencrypt-remotes", "", ""},
	}
	for _, s := range steps {
		err := route(append(s.args, "--server", srv.URL))
		if s.wantErrSubstr == "" && err != nil || s.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), s.wantErrSubstr)) {
			t.Fatalf("%v: err = %v", s.args, err)
		}
		req := api.last(t)
		if req.method != s.method || req.path != s.path || req.query != s.query {
			t.Fatalf("%v: request = %+v", s.args, req)
		}
	}
}

// --ctx picks a context for one command without changing the selected
// one.
func TestCtxFlagSelectsAContextForOneCommand(t *testing.T) {
	resetRemoteCLI(t)
	a, b := &remoteAPI{}, &remoteAPI{}
	srvA, srvB := a.serve(t), b.serve(t)
	if err := route([]string{"ctx", "add", "a", "--server", srvA.URL, "--user", "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := route([]string{"ctx", "add", "b", "--server", srvB.URL, "--user", "bob"}); err != nil {
		t.Fatal(err)
	}
	if err := route([]string{"remote", "ls", "--ctx", "b"}); err != nil {
		t.Fatalf("remote ls --ctx b: %v", err)
	}
	if b.count() != 1 || a.count() != 0 || b.last(t).user != "bob" {
		t.Fatalf("--ctx b reached a=%d b=%d", a.count(), b.count())
	}
	flagCtx = ""
	if err := route([]string{"remote", "ls"}); err != nil {
		t.Fatal(err)
	}
	if a.count() != 1 {
		t.Fatal("the selected context did not stay a")
	}
	// A missing context is an error, not a silent fall back to the
	// selected one (cliClient reports it and exits, so ask directly).
	flagCtx = "missing"
	if _, err := cliConnection(); err == nil || !strings.Contains(err.Error(), "no context") {
		t.Fatalf("--ctx missing: %v", err)
	}
}

// --ctx with NARAD_ADDR, NARAD_USER or NARAD_PASS in the environment is
// refused: the variable would send a command meant for one cluster to
// the other, or one cluster's password to the other. Nothing is sent.
func TestCtxFlagRefusesConnectionEnvironment(t *testing.T) {
	resetRemoteCLI(t)
	passA, passB := rand.Text(), rand.Text()
	a, b := &remoteAPI{}, &remoteAPI{}
	srvA, srvB := a.serve(t), b.serve(t)
	cliStdin = strings.NewReader(passA + "\n")
	if err := route([]string{"ctx", "add", "a", "--server", srvA.URL, "--user", "admin-a", "--password-stdin"}); err != nil {
		t.Fatal(err)
	}
	cliStdin = strings.NewReader(passB + "\n")
	if err := route([]string{"ctx", "add", "b", "--server", srvB.URL, "--user", "admin-b", "--password-stdin"}); err != nil {
		t.Fatal(err)
	}
	if err := route([]string{"ctx", "select", "a"}); err != nil {
		t.Fatal(err)
	}
	for _, env := range []struct{ key, value string }{
		{"NARAD_ADDR", srvA.URL},
		{"NARAD_USER", "admin-a"},
		{"NARAD_PASS", passA},
	} {
		t.Run(env.key, func(t *testing.T) {
			clearConnEnv(t)
			t.Setenv(env.key, env.value)
			flagCtx = "b"
			defer func() { flagCtx = "" }()
			if _, err := cliConnection(); err == nil || !strings.Contains(err.Error(), env.key) {
				t.Fatalf("--ctx b with %s set: %v, want a refusal naming it", env.key, err)
			}
			// Without --ctx the variable still overrides the selected context.
			flagCtx = ""
			c, err := cliConnection()
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{"NARAD_ADDR": c.Server, "NARAD_USER": c.User, "NARAD_PASS": c.Password}[env.key]
			if got != env.value {
				t.Fatalf("%s without --ctx resolved to %q, want %q", env.key, got, env.value)
			}
		})
	}
	if a.count() != 0 || b.count() != 0 {
		t.Fatalf("a refused command reached a server: a=%d b=%d", a.count(), b.count())
	}
	// With a clean environment --ctx b is b's server and b's credentials.
	clearConnEnv(t)
	flagCtx = "b"
	c, err := cliConnection()
	flagCtx = ""
	if err != nil || c.Server != srvB.URL || c.User != "admin-b" || c.Password != passB {
		t.Fatalf("--ctx b = %+v, %v", c, err)
	}
}

func TestRemoteLsWarnsAboutMembersThatDidNotAnswer(t *testing.T) {
	if w := silentMembersWarning([]byte(`{"remotes":[],"lingering":[],"not_answering":["narad-2"]}`)); !strings.Contains(w, "narad-2 did not answer") {
		t.Fatalf("warning = %q", w)
	}
	if w := silentMembersWarning([]byte(`{"remotes":[],"lingering":[],"not_answering":[]}`)); w != "" {
		t.Fatalf("warning with every member answering = %q", w)
	}
}
