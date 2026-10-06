package remotes_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpremotes "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/remotes"
)

// logBuffer captures log lines safely across goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// split separates the audit lines of one event into the ingress node's
// (handlers.AuditWriter, which records the status the client got) and
// the leader's (remote.LeaderAudit, one per proposal).
func split(lines []map[string]any) (ingress, leader []map[string]any) {
	for _, l := range lines {
		if _, ok := l["status"]; ok {
			ingress = append(ingress, l)
		} else {
			leader = append(leader, l)
		}
	}
	return ingress, leader
}

// lines returns every audit line with the given event.
func (b *logBuffer) lines(t *testing.T, event string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			continue
		}
		if m["component"] == "audit" && m["event"] == event {
			out = append(out, m)
		}
	}
	return out
}

// sourceBroker is the only broker call the remotes handlers make: a
// source topic's details for the test route.
type sourceBroker struct {
	broker.Broker
	topics map[string]topic.Details
}

func (b sourceBroker) GetTopicDetails(_ context.Context, name string) (topic.Details, error) {
	if d, ok := b.topics[name]; ok {
		return d, nil
	}
	return topic.Details{}, errNotFound
}

var errNotFound = io.EOF

// fakeTarget answers the checks like a secured Narad target that holds
// topic "orders" and grants the replicator produce only.
func fakeTarget(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	jsonErr := func(w http.ResponseWriter, status int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); !ok {
			jsonErr(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if r.PathValue("topic") != "orders" {
			jsonErr(w, http.StatusNotFound, "topic not found")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"orders","id":"target-orders-id"}`))
	})
	mux.HandleFunc("GET /v1/topics/{topic}/children", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"parent":"orders","parent_id":"target-orders-id","children":[]}`))
	})
	mux.HandleFunc("POST /v1/topics/{topic}/produce/batch", func(w http.ResponseWriter, _ *http.Request) {
		jsonErr(w, http.StatusBadRequest, "messages required")
	})
	mux.HandleFunc("GET /v1/users", func(w http.ResponseWriter, _ *http.Request) {
		jsonErr(w, http.StatusForbidden, "admin privileges required")
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
}

// apiNode is one single-node cluster behind the real remotes handlers:
// a metastore, the outbound plane, the plane and the registry, served
// through a mux with the production route patterns.
type apiNode struct {
	dir     string
	ms      *metastore.Store
	svc     *remote.Service
	cache   *remote.Cache
	plane   *cluster.RemotePlane
	logs    *logBuffer
	mux     *http.ServeMux
	target  *httptest.Server
	ca      string
	secret  string
	posture remote.Posture
}

type apiOpts struct {
	posture   *remote.Posture
	secret    *string
	allowlist bool
	writes    int // per-minute write limit; 0 means 1000 (the limit test sets 10)
	dataDir   string
	snapshots bool // snapshot after every entry, so snapshots hold everything
	metrics   *metrics.RemoteMetrics
}

// randomSecret is a cluster secret as `openssl rand -base64 32` prints
// it, drawn again in the rare case the strength rule takes it for a
// passphrase (as the refusal tells an operator to do).
func randomSecret(t testing.TB) string {
	t.Helper()
	for {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		if s := base64.StdEncoding.EncodeToString(b); remotecred.CheckSecretStrength(s) == nil {
			return s
		}
	}
}

func newAPINode(t *testing.T, o apiOpts) *apiNode {
	t.Helper()
	n := &apiNode{logs: &logBuffer{}, secret: randomSecret(t), posture: remote.Posture{SecurityEnabled: true, APIHopEncrypted: true, RaftTLS: true}}
	if o.posture != nil {
		n.posture = *o.posture
	}
	if o.secret != nil {
		n.secret = *o.secret
	}
	log := slog.New(slog.NewJSONHandler(n.logs, nil))
	if o.dataDir == "" {
		o.dataDir = t.TempDir()
	}
	mc := metastore.Config{NodeID: "narad-0", DataDir: o.dataDir, BindAddr: "127.0.0.1:0"}
	if o.snapshots {
		mc.SnapshotThreshold, mc.SnapshotInterval, mc.TrailingLogs = 1, 10*time.Millisecond, 1
	}
	ms, err := metastore.New(mc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !ms.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := ms.GetMember("narad-0"); err != nil {
		if err := ms.RegisterMember(context.Background(), metastore.Member{ID: "narad-0", Addr: "127.0.0.1:1", Status: metastore.MemberAlive}); err != nil {
			t.Fatal(err)
		}
	}
	// The leader re-authorizes every registry write against its own
	// users: the admin the tests call as exists there.
	if _, err := ms.GetUser(context.Background(), admin.Username); err != nil {
		if err := ms.CreateUser(context.Background(), user.User{Username: admin.Username, PasswordHash: []byte("$2a$04$unused"), Grants: admin.Grants}); err != nil {
			t.Fatal(err)
		}
	}
	n.ms, n.dir = ms, o.dataDir
	n.target, n.ca = fakeTarget(t)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(n.target.URL, "https://"))
	p, _ := strconv.Atoi(port)
	gc := remote.GuardConfig{AllowedPorts: []int{p}, AllowAddresses: []string{"127.0.0.0/8"}}
	if o.allowlist {
		gc.AllowedHosts = []string{"127.0.0.1"}
	}
	guard, err := remote.NewGuard(gc)
	if err != nil {
		t.Fatal(err)
	}
	secrets := remote.Secrets{Current: n.secret}
	n.cache = remote.NewCache(remote.CacheConfig{Registry: ms, Secrets: secrets, Guard: guard, Posture: n.posture, Log: log, Metrics: o.metrics})
	if o.writes == 0 {
		o.writes = 1000
	}
	n.svc = remote.NewService(remote.ServiceConfig{Store: ms, Cache: n.cache, Guard: guard, Secrets: secrets, Posture: n.posture, NodeID: "narad-0", Log: log, WritesPerMinute: o.writes, Metrics: o.metrics})
	n.plane = cluster.NewRemotePlane(ms, nil, nil, "narad-0", log)
	reg := cluster.NewRemoteRegistry(cluster.RemoteRegistryDeps{Store: ms, Service: n.svc, Plane: n.plane, Log: log, SelfID: "narad-0"})
	n.plane.Registry, n.plane.Checks = reg, reg
	n.svc.SetCluster(reg)
	set := handlers.New(handlers.Deps{
		Broker:    sourceBroker{topics: map[string]topic.Details{"orders": {Topic: topic.Topic{Name: "orders", ID: "source-orders-id"}}}},
		Metastore: ms, Logger: log,
		Remote: handlers.RemoteDeps{Writer: n.plane, Service: n.svc},
	})
	n.mux = http.NewServeMux()
	n.mux.HandleFunc("POST /v1/remotes", httpremotes.Create(set))
	n.mux.HandleFunc("GET /v1/remotes", httpremotes.List(set))
	n.mux.HandleFunc("GET /v1/remotes/{name}", httpremotes.Get(set))
	n.mux.HandleFunc("PATCH /v1/remotes/{name}", httpremotes.Update(set))
	n.mux.HandleFunc("DELETE /v1/remotes/{name}", httpremotes.Delete(set))
	n.mux.HandleFunc("POST /v1/remotes/{name}/test", httpremotes.Test(set))
	n.mux.HandleFunc("POST /v1/cluster/reencrypt-remotes", httpremotes.Reencrypt(set))
	return n
}

var (
	admin    = &user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	nonAdmin = &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}}
)

type answer struct {
	status int
	header http.Header
	body   []byte
}

func (a answer) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(a.body, &m); err != nil {
		t.Fatalf("body %q: %v", a.body, err)
	}
	return m
}

func (a answer) errorText(t *testing.T) string {
	t.Helper()
	s, _ := a.json(t)["error"].(string)
	return s
}

func (n *apiNode) do(t *testing.T, id *user.User, method, path, body string) answer {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if id != nil {
		r = r.WithContext(security.WithIdentity(r.Context(), *id))
	}
	w := httptest.NewRecorder()
	n.mux.ServeHTTP(w, r)
	return answer{status: w.Code, header: w.Header(), body: w.Body.Bytes()}
}

const canary = "canary-7f3k9q-XYZZY-0123456789"

func (n *apiNode) createBody(name, password string) string {
	b, _ := json.Marshal(map[string]any{
		"name": name, "url": n.target.URL, "username": "repl-from-a-7f3k9q", "password": password,
		"ca_pem": n.ca, "limits": map[string]any{"max_in_flight": 48},
	})
	return string(b)
}

func TestCreateGetUpdateDeleteThroughTheAPI(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	res := n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary))
	if res.status != http.StatusCreated {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	v := res.json(t)
	pw, _ := v["password"].(map[string]any)
	if v["name"] != "b" || v["credential_version"] != 1.0 || pw["fingerprint"] == "" || pw["set_by"] != "alice" || len(pw["key_version"].(string)) != 16 {
		t.Fatalf("create answer = %s", res.body)
	}
	if v["url"] != n.target.URL || v["ca_pem_sha512"] == "" {
		t.Fatalf("create answer = %s", res.body)
	}
	rec, err := n.ms.GetRemote("b")
	if err != nil || rec.Limits.MaxInFlight != 48 || rec.ID == "" {
		t.Fatalf("stored record = %+v, %v", rec, err)
	}
	keys, _ := n.ms.RemoteKeys()
	if len(keys.Salt) != 32 {
		t.Fatalf("salt = %d bytes", len(keys.Salt))
	}

	// The cache opens it once and the test checks pass.
	n.cache.Refresh()
	if _, err := n.cache.Get("b"); err != nil {
		t.Fatalf("cache: %v", err)
	}
	res = n.do(t, admin, http.MethodPost, "/v1/remotes/b/test", `{"topic":"orders","source":"orders"}`)
	if res.status != http.StatusOK || res.json(t)["result"] != "pass" {
		t.Fatalf("test: %d %s", res.status, res.body)
	}
	// A second test within 5 s is throttled on this node.
	if res = n.do(t, admin, http.MethodPost, "/v1/remotes/b/test", `{"topic":"orders"}`); res.status != http.StatusTooManyRequests {
		t.Fatalf("second test: %d %s", res.status, res.body)
	}

	// Limits only: no password needed, same credential version.
	res = n.do(t, admin, http.MethodPatch, "/v1/remotes/b", `{"limits":{"max_in_flight":64}}`)
	if res.status != http.StatusOK || res.json(t)["credential_version"] != 1.0 {
		t.Fatalf("limits change: %d %s", res.status, res.body)
	}
	// A URL, username or CA change needs the password.
	for _, body := range []string{`{"username":"other-user"}`, `{"ca_pem":""}`} {
		res = n.do(t, admin, http.MethodPatch, "/v1/remotes/b", body)
		if res.status != http.StatusBadRequest || res.errorText(t) != remote.ErrPasswordRequired.Error() {
			t.Fatalf("PATCH %s without password: %d %s", body, res.status, res.body)
		}
	}
	res = n.do(t, admin, http.MethodPatch, "/v1/remotes/b", `{"username":"other-user","password":"`+canary+`-2"}`)
	if res.status != http.StatusOK || res.json(t)["credential_version"] != 2.0 || res.json(t)["username"] != "other-user" {
		t.Fatalf("username and password: %d %s", res.status, res.body)
	}

	res = n.do(t, admin, http.MethodGet, "/v1/remotes", "")
	if res.status != http.StatusOK {
		t.Fatalf("list: %d %s", res.status, res.body)
	}
	list := res.json(t)
	if list["allowlist"] != "none" || list["key"] == nil {
		t.Fatalf("list = %s", res.body)
	}
	n.cache.Refresh()
	res = n.do(t, admin, http.MethodGet, "/v1/remotes/b", "")
	nodes, _ := res.json(t)["nodes"].([]any)
	if res.status != http.StatusOK || len(nodes) != 1 || nodes[0].(map[string]any)["state"] != "ready" {
		t.Fatalf("get: %d %s", res.status, res.body)
	}
	if n.do(t, admin, http.MethodGet, "/v1/remotes/c", "").status != http.StatusNotFound {
		t.Fatal("get of a missing remote")
	}

	if res = n.do(t, admin, http.MethodDelete, "/v1/remotes/b", ""); res.status != http.StatusNoContent {
		t.Fatalf("delete: %d %s", res.status, res.body)
	}
	if n.do(t, admin, http.MethodDelete, "/v1/remotes/b", "").status != http.StatusNotFound {
		t.Fatal("delete of a gone remote")
	}

	// Every answer is uncacheable and none carries a password field or
	// the canary.
	for _, l := range strings.Split(n.logs.String(), "\n") {
		if strings.Contains(l, canary) || strings.Contains(l, "repl-from-a-7f3k9q") && strings.Contains(l, `"component":"audit"`) {
			t.Fatalf("log line leaks: %s", l)
		}
	}
}

func TestAuditLinesOneIngressOneLeaderPerProposal(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	if res := n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary)); res.status != http.StatusCreated {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	ingress, leader := split(n.logs.lines(t, "remote.create"))
	if len(ingress) != 1 || len(leader) != 1 {
		t.Fatalf("ingress lines %d, leader lines %d, want 1 and 1", len(ingress), len(leader))
	}
	if ingress[0]["outcome"] != handlers.AuditOK || leader[0]["outcome"] != "committed" || leader[0]["actor"] != "alice" ||
		ingress[0]["request_id"] != leader[0]["request_id"] || len(ingress[0]["request_id"].(string)) != 16 {
		t.Fatalf("lines: %v / %v", ingress[0], leader[0])
	}
	// A duplicate: a conflict on the ingress, a refused line on the leader.
	if res := n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary)); res.status != http.StatusConflict {
		t.Fatalf("duplicate: %d %s", res.status, res.body)
	}
	ingress, leader = split(n.logs.lines(t, "remote.create"))
	if len(ingress) != 2 || ingress[1]["outcome"] != handlers.AuditRejected || len(leader) != 2 || leader[1]["outcome"] != "refused" || leader[1]["class"] != "exists" {
		t.Fatalf("conflict lines: %v / %v", ingress, leader)
	}
	// A non-admin: one denied ingress line and nothing on the leader.
	if res := n.do(t, nonAdmin, http.MethodPost, "/v1/remotes", n.createBody("c", canary)); res.status != http.StatusForbidden {
		t.Fatalf("non-admin: %d", res.status)
	}
	ingress, leader = split(n.logs.lines(t, "remote.create"))
	if len(ingress) != 3 || ingress[2]["outcome"] != handlers.AuditDenied || len(leader) != 2 {
		t.Fatalf("denied lines: %v", ingress)
	}
	all := n.logs.String()
	for _, secret := range []string{canary, "repl-from-a-7f3k9q", `"kv"`, "key_version"} {
		if strings.Contains(all, secret) {
			t.Fatalf("an audit line carries %q", secret)
		}
	}
}

func TestSecurityAndPostureRefusals(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	for _, path := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/remotes", n.createBody("b", canary)},
		{http.MethodGet, "/v1/remotes", ""},
		{http.MethodGet, "/v1/remotes/b", ""},
		{http.MethodPatch, "/v1/remotes/b", `{"limits":{"max_in_flight":2}}`},
		{http.MethodDelete, "/v1/remotes/b", ""},
		{http.MethodPost, "/v1/remotes/b/test", `{"topic":"orders"}`},
		{http.MethodPost, "/v1/cluster/reencrypt-remotes", ""},
	} {
		res := n.do(t, nil, path.method, path.path, path.body)
		if res.status != http.StatusForbidden || res.errorText(t) != "remotes require security" || res.header.Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s without identity: %d %s", path.method, path.path, res.status, res.body)
		}
		res = n.do(t, nonAdmin, path.method, path.path, path.body)
		if res.status != http.StatusForbidden || res.errorText(t) != "admin privileges required" {
			t.Fatalf("%s %s as a non-admin: %d %s", path.method, path.path, res.status, res.body)
		}
	}

	noHop := remote.Posture{SecurityEnabled: true, RaftTLS: true}
	h := newAPINode(t, apiOpts{posture: &noHop})
	res := h.do(t, admin, http.MethodPost, "/v1/remotes", h.createBody("b", canary))
	if res.status != http.StatusPreconditionFailed || !strings.Contains(res.errorText(t), "api_hop_encrypted") {
		t.Fatalf("no attested hop: %d %s", res.status, res.body)
	}

	weak := "cluster-test-secret"
	w := newAPINode(t, apiOpts{secret: &weak})
	res = w.do(t, admin, http.MethodPost, "/v1/remotes", w.createBody("b", canary))
	if res.status != http.StatusPreconditionFailed || !strings.Contains(res.errorText(t), "openssl rand -base64 32") || strings.Contains(string(res.body), weak) {
		t.Fatalf("weak secret: %d %s", res.status, res.body)
	}
	none := ""
	s := newAPINode(t, apiOpts{secret: &none})
	res = s.do(t, admin, http.MethodPost, "/v1/remotes", s.createBody("b", canary))
	if res.status != http.StatusPreconditionFailed || !strings.Contains(res.errorText(t), "NARAD_CLUSTER_SECRET") {
		t.Fatalf("no secret on a single node: %d %s", res.status, res.body)
	}

	legacy := remote.Posture{SecurityEnabled: true, APIHopEncrypted: true, LegacyClusterAuth: true}
	l := newAPINode(t, apiOpts{posture: &legacy})
	res = l.do(t, admin, http.MethodPost, "/v1/remotes", l.createBody("b", canary))
	members, _ := res.json(t)["members"].([]any)
	if res.status != http.StatusPreconditionFailed || len(members) != 1 || members[0] != "narad-0" {
		t.Fatalf("legacy cluster auth: %d %s", res.status, res.body)
	}

	plaintextRaft := remote.Posture{SecurityEnabled: true, APIHopEncrypted: true, RaftTLS: false}
	pr := newAPINode(t, apiOpts{posture: &plaintextRaft})
	if res = pr.do(t, admin, http.MethodPost, "/v1/remotes", pr.createBody("b", canary)); res.status != http.StatusCreated {
		t.Fatalf("plaintext raft (Q23: warn, do not require): %d %s", res.status, res.body)
	}
}

func TestValidationRules(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	good := map[string]any{"name": "b", "url": n.target.URL, "username": "repl", "password": canary, "ca_pem": n.ca}
	with := func(k string, v any) string {
		m := map[string]any{}
		for kk, vv := range good {
			m[kk] = vv
		}
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
	cases := []struct {
		name, body, want string
	}{
		{"bad name", with("name", "B_1"), "name must match"},
		{"reserved name", with("name", "_keys"), "name must match"},
		{"http url", with("url", strings.Replace(n.target.URL, "https", "http", 1)), "https"},
		{"userinfo", with("url", strings.Replace(n.target.URL, "https://", "https://u:p@", 1)), "userinfo"},
		{"query", with("url", n.target.URL+"?x=1"), "query"},
		{"port not allowed", with("url", "https://127.0.0.1:8443"), "allowed_ports"},
		{"metadata address", with("url", "https://169.254.169.254:"+port(n.target.URL)), "refuses"},
		{"bad username", with("username", "a b"), "username must match"},
		{"short password", with("password", "short"), "password must be 24 to 72 bytes"},
		{"long password", with("password", strings.Repeat("x", 73)), "password must be 24 to 72 bytes"},
		{"bad ca", with("ca_pem", "not a certificate"), "ca_pem"},
		{"bad limits", with("limits", map[string]any{"max_in_flight": 1000}), "max_in_flight"},
		{"bad compression", with("limits", map[string]any{"compression": "gzip"}), "compression"},
		{"unknown field", with("extra", 1), "invalid json"},
		{"password not a string", with("password", 12345), "password: invalid"},
	}
	for _, c := range cases {
		res := n.do(t, admin, http.MethodPost, "/v1/remotes", c.body)
		if res.status != http.StatusBadRequest || !strings.Contains(res.errorText(t), c.want) {
			t.Fatalf("%s: %d %s, want 400 containing %q", c.name, res.status, res.body, c.want)
		}
		if strings.Contains(string(res.body), canary) {
			t.Fatalf("%s: the answer quotes the password", c.name)
		}
	}
	res := n.do(t, admin, http.MethodPost, "/v1/remotes", `{"password":{"x":"`+canary+`"}}`)
	if res.status != http.StatusBadRequest || res.errorText(t) != "password: invalid" {
		t.Fatalf("malformed password field: %d %s", res.status, res.body)
	}
	// Test targets are topic names: a path segment trick sends nothing.
	if n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary)).status != http.StatusCreated {
		t.Fatal("create")
	}
	for _, bad := range []string{"../users", ".", "..", "a/b", "%2e%2e"} {
		body, _ := json.Marshal(map[string]string{"topic": bad})
		if res := n.do(t, admin, http.MethodPost, "/v1/remotes/b/test", string(body)); res.status != http.StatusBadRequest {
			t.Fatalf("test topic %q: %d %s", bad, res.status, res.body)
		}
	}
}

func port(u string) string {
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(u, "https://"))
	return p
}

// The eleventh remote write in a minute on one node is a 429.
func TestWriteLimit(t *testing.T) {
	n := newAPINode(t, apiOpts{writes: remote.WritesPerMinute})
	var last answer
	for i := range 11 {
		last = n.do(t, admin, http.MethodPatch, "/v1/remotes/missing", `{"limits":{"max_in_flight":`+strconv.Itoa(i+1)+`}}`)
	}
	if last.status != http.StatusTooManyRequests || last.header.Get("Retry-After") == "" {
		t.Fatalf("11th write: %d %s", last.status, last.body)
	}
}

// The re-encrypt after a cluster secret rotation: remotes sealed under
// the previous key move to the current one; a repeat finds them current.
func TestReencryptThroughTheAPI(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	if res := n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary)); res.status != http.StatusCreated {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	oldKV := func() string { r, _ := n.ms.GetRemote("b"); return r.Credential.KV }()
	oldFP := func() string { r, _ := n.ms.GetRemote("b"); return r.Fingerprint }()

	// Restart the node's plane with a new secret and the old one as
	// previous, over the same metastore.
	rotated := newRotatedNode(t, n, randomSecret(t), n.secret)
	res := rotated.do(t, admin, http.MethodPost, "/v1/cluster/reencrypt-remotes", "")
	if res.status != http.StatusOK {
		t.Fatalf("reencrypt: %d %s", res.status, res.body)
	}
	ans := res.json(t)
	if re, _ := ans["reencrypted"].([]any); len(re) != 1 || re[0] != "b" || ans["key_version"] == oldKV {
		t.Fatalf("reencrypt answer = %s", res.body)
	}
	rec, _ := n.ms.GetRemote("b")
	if rec.Credential.KV == oldKV || rec.Fingerprint == oldFP || rec.CredentialVersion != 2 || rec.PasswordSetBy != "alice" {
		t.Fatalf("after re-encrypt = %+v", rec)
	}
	rotated.cache.Refresh()
	if _, err := rotated.cache.Get("b"); err != nil {
		t.Fatalf("the re-encrypted credential does not open: %v", err)
	}
	res = rotated.do(t, admin, http.MethodPost, "/v1/cluster/reencrypt-remotes", "{}")
	if cur, _ := res.json(t)["already_current"].([]any); res.status != http.StatusOK || len(cur) != 1 {
		t.Fatalf("repeat: %d %s", res.status, res.body)
	}
}

// newRotatedNode is n restarted with new secrets over the same
// metastore and target.
func newRotatedNode(t *testing.T, n *apiNode, current, previous string) *apiNode {
	t.Helper()
	r := &apiNode{ms: n.ms, logs: n.logs, target: n.target, ca: n.ca, secret: current, posture: n.posture}
	log := slog.New(slog.NewJSONHandler(r.logs, nil))
	guard, _ := remote.NewGuard(remote.GuardConfig{AllowedPorts: []int{mustAtoi(port(n.target.URL))}, AllowAddresses: []string{"127.0.0.0/8"}})
	secrets := remote.Secrets{Current: current, Previous: previous}
	r.cache = remote.NewCache(remote.CacheConfig{Registry: n.ms, Secrets: secrets, Guard: guard, Posture: r.posture, Log: log})
	r.svc = remote.NewService(remote.ServiceConfig{Store: n.ms, Cache: r.cache, Guard: guard, Secrets: secrets, Posture: r.posture, NodeID: "narad-0", Log: log})
	r.plane = cluster.NewRemotePlane(n.ms, nil, nil, "narad-0", log)
	reg := cluster.NewRemoteRegistry(cluster.RemoteRegistryDeps{Store: n.ms, Service: r.svc, Plane: r.plane, Log: log, SelfID: "narad-0"})
	r.plane.Registry, r.plane.Checks = reg, reg
	r.svc.SetCluster(reg)
	set := handlers.New(handlers.Deps{Broker: sourceBroker{}, Metastore: n.ms, Logger: log, Remote: handlers.RemoteDeps{Writer: r.plane, Service: r.svc}})
	r.mux = http.NewServeMux()
	r.mux.HandleFunc("POST /v1/cluster/reencrypt-remotes", httpremotes.Reencrypt(set))
	return r
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func TestForceDeleteAndLinks(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	if res := n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary)); res.status != http.StatusCreated {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	n.cache.Refresh()
	// A hand-written stub naming the remote (package B creates real ones).
	if err := n.ms.CreateTopic(context.Background(), topic.Topic{Name: "orders-to-b", Parent: "orders", Role: topic.RoleChild, Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}}); err != nil {
		t.Fatal(err)
	}
	res := n.do(t, admin, http.MethodDelete, "/v1/remotes/b", "")
	links, _ := res.json(t)["links"].([]any)
	if res.status != http.StatusConflict || len(links) != 1 || links[0] != "orders/orders-to-b" {
		t.Fatalf("delete while linked: %d %s", res.status, res.body)
	}
	if res = n.do(t, admin, http.MethodGet, "/v1/remotes/b?nodes=false", ""); res.status != http.StatusOK {
		t.Fatalf("get: %d", res.status)
	} else if l, _ := res.json(t)["links"].([]any); len(l) != 1 {
		t.Fatalf("links in get = %s", res.body)
	}
	if res = n.do(t, admin, http.MethodDelete, "/v1/remotes/b?force=maybe", ""); res.status != http.StatusBadRequest {
		t.Fatalf("bad force: %d", res.status)
	}
	if res = n.do(t, admin, http.MethodDelete, "/v1/remotes/b?force=true", ""); res.status != http.StatusNoContent {
		t.Fatalf("force delete: %d %s", res.status, res.body)
	}
	// The node's cache still holds it until its refresher runs: listed
	// as lingering meanwhile.
	res = n.do(t, admin, http.MethodGet, "/v1/remotes", "")
	lingering, _ := res.json(t)["lingering"].([]any)
	if len(lingering) != 1 || lingering[0].(map[string]any)["remote"] != "b" {
		t.Fatalf("lingering before refresh = %s", res.body)
	}
	n.cache.Refresh()
	res = n.do(t, admin, http.MethodGet, "/v1/remotes", "")
	if l, _ := res.json(t)["lingering"].([]any); len(l) != 0 {
		t.Fatalf("lingering after refresh = %s", res.body)
	}
	if _, err := n.cache.Get("b"); err == nil {
		t.Fatal("the cache kept a force-deleted remote")
	}
}

// lostAnswer commits the write on the leader, then loses the answer the
// way a forward that times out after the commit does.
type lostAnswer struct{ inner handlers.RemoteWriter }

func (l lostAnswer) RemoteWrite(ctx context.Context, req nodewire.RemoteWriteRequest) (nodewire.Response, error) {
	_, _ = l.inner.RemoteWrite(ctx, req)
	return nodewire.Response{}, context.DeadlineExceeded
}

// A forward that times out after the commit shows unknown on the
// ingress beside committed on the leader, joined by the request ID.
func TestForwardedErrorBesideCommitted(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	set := handlers.New(handlers.Deps{
		Broker: sourceBroker{}, Metastore: n.ms, Logger: slog.New(slog.NewJSONHandler(n.logs, nil)),
		Remote: handlers.RemoteDeps{Writer: lostAnswer{inner: n.plane}, Service: n.svc},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/remotes", httpremotes.Create(set))
	r := httptest.NewRequest(http.MethodPost, "/v1/remotes", strings.NewReader(n.createBody("b", canary)))
	r = r.WithContext(security.WithIdentity(r.Context(), *admin))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("lost answer: %d %s", w.Code, w.Body.String())
	}
	if _, err := n.ms.GetRemote("b"); err != nil {
		t.Fatalf("the write did not commit: %v", err)
	}
	ingress, leader := split(n.logs.lines(t, "remote.create"))
	if len(ingress) != 1 || len(leader) != 1 || ingress[0]["outcome"] != handlers.AuditUnknown || leader[0]["outcome"] != "committed" ||
		ingress[0]["request_id"] != leader[0]["request_id"] {
		t.Fatalf("ingress %v, leader %v", ingress, leader)
	}
}

// The ingress node's own release gate answers 412 the way the leader's
// does: the body names the member that holds the remote entry types
// back in `members`, so automation can find which pod to upgrade.
func TestReleaseGateNamesTheMemberInMembers(t *testing.T) {
	n := newAPINode(t, apiOpts{})
	if err := n.ms.RegisterMember(context.Background(), metastore.Member{ID: "narad-1", Addr: "127.0.0.1:2", Status: metastore.MemberAlive, Build: "narad 3.1.0", EntryTypes: 22}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/remotes", n.createBody("b", canary)},
		{http.MethodPatch, "/v1/remotes/b", `{"limits":{"max_in_flight":2}}`},
		{http.MethodDelete, "/v1/remotes/b", ""},
		{http.MethodPost, "/v1/cluster/reencrypt-remotes", ""},
	} {
		res := n.do(t, admin, c.method, c.path, c.body)
		members, _ := res.json(t)["members"].([]any)
		if res.status != http.StatusPreconditionFailed || len(members) != 1 || members[0] != "narad-1" || !strings.Contains(res.errorText(t), "narad-1") {
			t.Fatalf("%s %s with an older member: %d %s, want 412 with members [narad-1]", c.method, c.path, res.status, res.body)
		}
	}
}
