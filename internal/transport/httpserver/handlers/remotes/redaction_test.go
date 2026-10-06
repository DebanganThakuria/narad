package remotes_test

import (
	"bytes"
	"encoding/base64"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
)

// canaryNeedles are the forms a leaked credential could take: the
// password, the whole Basic header value, and every base64 alignment of
// "username:password" (a header copied at any offset still matches).
func canaryNeedles(username, password string) []string {
	plain := username + ":" + password
	needles := []string{password, "Basic " + base64.StdEncoding.EncodeToString([]byte(plain))}
	for k := range 3 {
		enc := base64.StdEncoding.EncodeToString(append(bytes.Repeat([]byte{0}, k), plain...))
		// Drop the groups a pad byte or the tail can change.
		needles = append(needles, enc[4:len(enc)-4])
	}
	return needles
}

func assertNoCanary(t *testing.T, where string, data []byte, needles []string) {
	t.Helper()
	for _, n := range needles {
		if bytes.Contains(data, []byte(n)) {
			t.Fatalf("%s carries a credential form (%d bytes matched)", where, len(n))
		}
	}
}

// The redaction canary, registry side: a canary password through
// create, change, test, re-encrypt, a panic in a handler and a restart
// appears in none of the logs, audit lines, metrics, API answers, error
// bodies, goroutine dump, heap profile, raft.db, fsm.db or snapshots.
func TestRedactionCanary(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	n := newAPINode(t, apiOpts{dataDir: dir, snapshots: true, metrics: m.Remote})
	const user = "repl-from-a-7f3k9q"
	canary1 := "canary-first-" + randomSecret(t)[:20]
	canary2 := "canary-second-" + randomSecret(t)[:20]
	needles := append(canaryNeedles(user, canary1), canaryNeedles(user, canary2)...)
	var answers bytes.Buffer
	keep := func(a answer) answer { answers.Write(a.body); answers.WriteString("\n"); return a }

	if a := keep(n.do(t, admin, http.MethodPost, "/v1/remotes", n.createBody("b", canary1))); a.status != http.StatusCreated {
		t.Fatalf("create: %d %s", a.status, a.body)
	}
	n.cache.Refresh()
	keep(n.do(t, admin, http.MethodPost, "/v1/remotes/b/test", `{"topic":"orders"}`))
	if a := keep(n.do(t, admin, http.MethodPatch, "/v1/remotes/b", `{"password":"`+canary2+`"}`)); a.status != http.StatusOK {
		t.Fatalf("change: %d %s", a.status, a.body)
	}
	n.cache.Refresh()
	// Refused writes carrying the canary: a bad field, a malformed body.
	keep(n.do(t, admin, http.MethodPost, "/v1/remotes", `{"name":"c","url":"http://x","username":"`+user+`","password":"`+canary1+`"}`))
	keep(n.do(t, admin, http.MethodPost, "/v1/remotes", `{"name":"c","password":"`+canary1+"\x01"+`"}`))
	keep(n.do(t, admin, http.MethodPost, "/v1/remotes", `{"password":["`+canary1+`"]}`))
	keep(n.do(t, admin, http.MethodGet, "/v1/remotes", ""))
	keep(n.do(t, admin, http.MethodGet, "/v1/remotes/b", ""))

	// A panic in a handler that holds the decoded request.
	panicking := httpserver.Recover(slog.New(slog.NewJSONHandler(n.logs, nil)))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		var req remote.CreateRequest
		req.Username = user
		_ = req.Password.UnmarshalJSON([]byte(`"` + canary2 + `"`))
		panic(req)
	}))
	w := httptest.NewRecorder()
	panicking.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/remotes", nil))
	answers.Write(w.Body.Bytes())

	// A re-encrypt under a rotated secret.
	rotated := newRotatedNode(t, n, randomSecret(t), n.secret)
	keep(rotated.do(t, admin, http.MethodPost, "/v1/cluster/reencrypt-remotes", ""))
	rotated.cache.Refresh()

	// Metrics, a goroutine dump and a heap profile while the caches hold
	// the header.
	var metricsText bytes.Buffer
	if mfs, err := reg.Gather(); err == nil {
		for _, mf := range mfs {
			_, _ = expfmt.MetricFamilyToText(&metricsText, mf)
		}
	}
	var dump, heap bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&dump, 2)
	_ = pprof.WriteHeapProfile(&heap)

	time.Sleep(200 * time.Millisecond) // let snapshots land
	_ = n.ms.Close()
	// Restart over the same directory.
	restarted := newAPINode(t, apiOpts{dataDir: dir})
	restarted.cache.Refresh()
	keep(restarted.do(t, admin, http.MethodGet, "/v1/remotes/b", ""))
	_ = restarted.ms.Close()

	assertNoCanary(t, "the logs and audit lines", []byte(n.logs.String()), needles)
	assertNoCanary(t, "the API answers and error bodies", answers.Bytes(), needles)
	assertNoCanary(t, "/metrics", metricsText.Bytes(), needles)
	for _, family := range []string{"narad_remote_credential_decrypts_total", "narad_remote_seals_total", "narad_remote_credential_state"} {
		if !strings.Contains(metricsText.String(), family) {
			t.Fatalf("/metrics lacks %s: the canary search covered nothing", family)
		}
	}
	for _, label := range []string{user, "key_version", n.target.URL} {
		if strings.Contains(metricsText.String(), label) {
			t.Fatalf("/metrics carries %q", label)
		}
	}
	assertNoCanary(t, "the goroutine dump", dump.Bytes(), needles)
	assertNoCanary(t, "the heap profile", heap.Bytes(), needles)
	files := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		assertNoCanary(t, filepath.Base(path), data, needles)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawRaft, sawFSM, sawSnapshot bool
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, _ error) error {
		switch {
		case strings.HasSuffix(path, "raft.db"):
			sawRaft = true
		case strings.HasSuffix(path, "fsm.db"):
			sawFSM = true
		case strings.Contains(path, "snapshots"):
			sawSnapshot = sawSnapshot || !d.IsDir()
		}
		return nil
	})
	if !sawRaft || !sawFSM || !sawSnapshot {
		t.Fatalf("searched %d files; raft.db %v, fsm.db %v, a snapshot %v", files, sawRaft, sawFSM, sawSnapshot)
	}
}
