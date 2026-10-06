package remote_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/remote"
)

var secured = remote.Posture{SecurityEnabled: true}

// checker builds a checker whose cache holds one entry for the target
// with the given credential.
func checker(t *testing.T, url, ca, username, password string) *remote.Checker {
	t.Helper()
	e, err := remote.NewStaticEntry(remote.StaticEntryConfig{
		Name: "b", RemoteID: "id-b", URL: url, Username: username,
		Password: domremote.NewSecret([]byte(password)), CAPEM: ca, CredentialVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &remote.Checker{Lookup: remote.NewStaticLookup(e), Posture: secured, NodeID: "narad-0"}
}

func run(t *testing.T, c *remote.Checker, topicName string, mod func(*remote.CheckRequest)) remote.NodeReport {
	t.Helper()
	req := remote.CheckRequest{Remote: "b", Topic: topicName, Source: "orders", SourceID: "source-id", CredentialVersion: 1}
	if mod != nil {
		mod(&req)
	}
	return c.Run(context.Background(), req)
}

func produceOnly(topics ...string) user.Grant {
	return user.Grant{Action: user.ActionProduce, Patterns: topics}
}

// Every check against the real router over TLS.
func TestChecksAgainstTheRealRouter(t *testing.T) {
	tg := newRealTarget(t, true)
	tg.createTopic("orders", nil)
	pass := tg.createUser("repl-from-a", produceOnly("orders", "stub", "delayed", "schemaed", "missing"))
	c := checker(t, tg.url(), tg.caPEM, "repl-from-a", pass)

	rep := run(t, c, "orders", nil)
	if rep.Result != remote.ResultPass || rep.TargetID == "" || rep.CredentialVersion != 1 || rep.Node != "narad-0" {
		t.Fatalf("a produce-only credential on a secured target: %+v", rep)
	}
	if rep.RTTMs != nil || rep.LaneCapacityPerS != nil {
		t.Fatal("rtt reported without a host allowlist")
	}
	// A target on this release serves parent_id in its children
	// listing: loop and recreate detection are on, and nothing warns.
	if rep.CertNotAfter == "" || !rep.TargetServesIDs || slices.ContainsFunc(rep.Warnings, func(w string) bool { return strings.Contains(w, "parent_id") }) {
		t.Fatalf("cert expiry missing, or a same-release target reported as serving no IDs: %+v", rep)
	}

	cases := []struct {
		name  string
		setup func()
		topic string
		mod   func(*remote.CheckRequest)
		class string
	}{
		{"target topic missing (JSON 404)", nil, "missing", nil, topic.RemoteStateTargetMissing},
		{"target equal to the source", nil, "orders", func(r *remote.CheckRequest) { r.SourceID = rep.TargetID }, remote.ClassTargetIsSource},
		{"delay child", func() {
			tg.createTopic("delayed-parent", nil)
			tg.createTopic("delayed", nil)
			if st := tg.adminDo(http.MethodPost, "/v1/topics/delayed-parent/children", map[string]any{"child": "delayed", "delay_ms": 60_000}); st != http.StatusOK && st != http.StatusCreated {
				t.Fatalf("attach delay child: %d", st)
			}
		}, "delayed", nil, remote.ClassTargetIsDelayChild},
		{"stub", func() {
			if err := tg.ms.CreateTopic(context.Background(), topic.Topic{Name: "stub", ID: "stub-id", Partitions: 3, Remote: &topic.RemoteLink{Name: "c", Topic: "orders"}}); err != nil {
				t.Fatal(err)
			}
		}, "stub", nil, remote.ClassTargetIsStub},
		{"schema mismatch", func() {
			tg.createTopic("schemaed", map[string]any{"schema": json.RawMessage(`{"type":"object"}`)})
		}, "schemaed", func(r *remote.CheckRequest) { r.SourceSchema = json.RawMessage(`{"type":"string"}`) }, remote.ClassSchemaMismatch},
		{"stale credential version", nil, "orders", func(r *remote.CheckRequest) { r.CredentialVersion = 2 }, remote.ClassStale},
		{"unknown remote", nil, "orders", func(r *remote.CheckRequest) { r.Remote = "c" }, topic.RemoteStateRemoteMissing},
	}
	for _, tc := range cases {
		if tc.setup != nil {
			tc.setup()
		}
		got := run(t, c, tc.topic, tc.mod)
		if got.Result != remote.ResultFail || got.Class != tc.class {
			t.Fatalf("%s: %s %s, want fail %s", tc.name, got.Result, got.Class, tc.class)
		}
	}
	// A schema only the target has warns and passes; the same schema passes.
	got := run(t, c, "schemaed", nil)
	if got.Result != remote.ResultPass || !slices.ContainsFunc(got.Warnings, func(w string) bool { return strings.Contains(w, "schema") }) {
		t.Fatalf("target-only schema: %+v", got)
	}
	got = run(t, c, "schemaed", func(r *remote.CheckRequest) { r.SourceSchema = json.RawMessage("{ \"type\" : \"object\" }") })
	if got.Result != remote.ResultPass {
		t.Fatalf("identical schema: %+v", got)
	}

	// Credentials: a wrong password, a user with no grant, an admin.
	if got := run(t, checker(t, tg.url(), tg.caPEM, "repl-from-a", "wrong-password-0123456789"), "orders", nil); got.Class != topic.RemoteStateAuthFailed {
		t.Fatalf("wrong password: %+v", got)
	}
	noGrant := tg.createUser("nobody")
	if got := run(t, checker(t, tg.url(), tg.caPEM, "nobody", noGrant), "orders", nil); got.Class != topic.RemoteStateForbidden {
		t.Fatalf("no grant: %+v", got)
	}
	consumeOnly := tg.createUser("reader", user.Grant{Action: user.ActionConsume, Patterns: []string{"orders"}})
	if got := run(t, checker(t, tg.url(), tg.caPEM, "reader", consumeOnly), "orders", nil); got.Class != topic.RemoteStateForbidden {
		t.Fatalf("no produce grant (check 6): %+v", got)
	}
	if got := run(t, checker(t, tg.url(), tg.caPEM, tg.adminUser, tg.adminPass), "orders", nil); got.Class != remote.ClassAdminCredential {
		t.Fatalf("admin credential: %+v", got)
	}
	// A path segment that is not a topic name sends nothing and fails.
	for _, bad := range []string{"../users", ".", "..", "a/b", "%2e%2e"} {
		if got := run(t, c, bad, nil); got.Result != remote.ResultFail {
			t.Fatalf("topic %q: %+v", bad, got)
		}
	}
}

func TestCheckSecurityOffOnTheTarget(t *testing.T) {
	tg := newRealTarget(t, false)
	tg.createTopic("orders", nil)
	got := run(t, checker(t, tg.url(), tg.caPEM, "repl", "anything-0123456789012345"), "orders", nil)
	if got.Class != remote.ClassTargetSecurityOff {
		t.Fatalf("security off on the target: %+v", got)
	}
}

func TestCheckWarnsWithoutAUsersRoute(t *testing.T) {
	tg := newRealTargetWith(t, true, false)
	tg.createTopic("orders", nil)
	pass := tg.createUser("repl", produceOnly("orders"))
	got := run(t, checker(t, tg.url(), tg.caPEM, "repl", pass), "orders", nil)
	if got.Result != remote.ResultPass || !slices.ContainsFunc(got.Warnings, func(w string) bool { return strings.Contains(w, "users route") }) {
		t.Fatalf("no users route: %+v", got)
	}
}

// fakeNarad answers like a Narad target in Narad's shapes, for the
// cases the real router cannot stage: an older target without the batch
// route (a bare ServeMux, so the 404 is Go's own), a target with a
// remote child (the loop rule), and an edge in front of the target.
func fakeNarad(t *testing.T, withBatch bool, children string, edge404 bool) *httptest.Server {
	t.Helper()
	jsonErr := func(w http.ResponseWriter, status int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
	}
	authed := func(r *http.Request) bool { _, _, ok := r.BasicAuth(); return ok }
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		if !authed(r) {
			jsonErr(w, http.StatusUnauthorized, "authentication required")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"orders","id":"target-id","partitions":1}`))
	})
	mux.HandleFunc("GET /v1/topics/{topic}/children", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(children))
	})
	if withBatch {
		mux.HandleFunc("POST /v1/topics/{topic}/produce/batch", func(w http.ResponseWriter, r *http.Request) {
			if edge404 {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("<html>not here</html>"))
				return
			}
			jsonErr(w, http.StatusBadRequest, "messages required")
		})
	}
	mux.HandleFunc("GET /v1/users", func(w http.ResponseWriter, r *http.Request) {
		jsonErr(w, http.StatusForbidden, "admin privileges required")
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func caOf(srv *httptest.Server) string { return string(pemOf(srv.Certificate().Raw)) }

func TestChecksAgainstOlderAndEdgeTargets(t *testing.T) {
	plain := `{"parent":"orders","parent_id":"target-id","children":[{"name":"c1"}]}`
	looped := `{"parent":"orders","parent_id":"target-id","children":[{"name":"to-a","remote":{"name":"a","topic":"orders"}}]}`
	cases := []struct {
		name      string
		withBatch bool
		children  string
		edge404   bool
		class     string
	}{
		{"missing batch route", false, plain, false, topic.RemoteStateNoBatchProduce},
		{"target with a remote child", true, looped, false, topic.RemoteStateTargetHasRemoteChildren},
		{"an edge's 404 on the batch route", true, plain, true, topic.RemoteClassEdge},
	}
	for _, tc := range cases {
		srv := fakeNarad(t, tc.withBatch, tc.children, tc.edge404)
		got := run(t, checker(t, srv.URL, caOf(srv), "repl", "pw-0123456789012345678901"), "orders", nil)
		if got.Class != tc.class {
			t.Fatalf("%s: %+v, want %s", tc.name, got, tc.class)
		}
	}
	srv := fakeNarad(t, true, plain, false)
	got := run(t, checker(t, srv.URL, caOf(srv), "repl", "pw-0123456789012345678901"), "orders", nil)
	if got.Result != remote.ResultPass || !got.TargetServesIDs || got.TargetID != "target-id" {
		t.Fatalf("a target serving IDs: %+v", got)
	}
}

// A target whose children listing has no parent_id (a release before
// remote children) passes with a warning: it cannot hold a remote child,
// so loop and chain detection start once it is upgraded; recreate
// detection reads the id from the describe answer instead.
func TestChecksWarnWhenTheTargetServesNoIDs(t *testing.T) {
	srv := fakeNarad(t, true, `{"parent":"orders","children":[{"name":"c1"}]}`, false)
	got := run(t, checker(t, srv.URL, caOf(srv), "repl", "pw-0123456789012345678901"), "orders", nil)
	if got.Result != remote.ResultPass || got.TargetServesIDs {
		t.Fatalf("a target without IDs: %+v, want a pass that serves no IDs", got)
	}
	if !slices.ContainsFunc(got.Warnings, func(w string) bool { return strings.Contains(w, "parent_id") }) {
		t.Fatalf("the no-IDs warning is missing: %+v", got)
	}
}

func TestChecksReportRTTOnlyWithAnAllowlist(t *testing.T) {
	srv := fakeNarad(t, true, `{"parent":"orders","parent_id":"x","children":[]}`, false)
	c := checker(t, srv.URL, caOf(srv), "repl", "pw-0123456789012345678901")
	g, err := remote.NewGuard(remote.GuardConfig{AllowedHosts: []string{"127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	c.Guard = g
	got := run(t, c, "orders", nil)
	if got.Result != remote.ResultPass || got.RTTMs == nil || got.LaneCapacityPerS == nil || *got.LaneCapacityPerS <= 0 {
		t.Fatalf("with an allowlist: %+v", got)
	}
}

func TestChecksRefuseAnInsecurePosture(t *testing.T) {
	srv := fakeNarad(t, true, `{"children":[]}`, false)
	c := checker(t, srv.URL, caOf(srv), "repl", "pw-0123456789012345678901")
	c.Posture = remote.Posture{SecurityEnabled: true, LegacyClusterAuth: true}
	if got := run(t, c, "orders", nil); got.Class != topic.RemoteStateNodeInsecure {
		t.Fatalf("legacy cluster auth: %+v", got)
	}
}
