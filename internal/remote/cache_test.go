package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// fakeRegistry is the metastore as the cache reads it, for one or
// several simulated nodes: every write bumps the version.
type fakeRegistry struct {
	mu      sync.Mutex
	version atomic.Uint64
	records map[string]domremote.Record
	keys    domremote.Keys
}

func newFakeRegistry(salt []byte) *fakeRegistry {
	r := &fakeRegistry{records: map[string]domremote.Record{}, keys: domremote.Keys{Salt: salt}}
	return r
}

func (r *fakeRegistry) RemotesVersion() uint64 { return r.version.Load() }

func (r *fakeRegistry) ListRemotes() ([]domremote.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domremote.Record, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec)
	}
	return out, nil
}

func (r *fakeRegistry) RemoteKeys() (domremote.Keys, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keys, nil
}

func (r *fakeRegistry) put(rec domremote.Record) {
	r.mu.Lock()
	r.records[rec.Name] = rec
	r.mu.Unlock()
	r.version.Add(1)
}

func (r *fakeRegistry) del(name string) {
	r.mu.Lock()
	delete(r.records, name)
	r.mu.Unlock()
	r.version.Add(1)
}

func (r *fakeRegistry) get(name string) domremote.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.records[name]
}

// countingOpener wraps an opener, counts opens, and can be swapped for
// one that fails the test: after that, any decrypt is a bug.
type countingOpener struct {
	inner  Opener
	opens  atomic.Int64
	forbid atomic.Pointer[testing.TB]
}

func (o *countingOpener) Open(env domremote.Envelope, ad remotecred.AssociatedData) ([]byte, error) {
	if tb := o.forbid.Load(); tb != nil {
		(*tb).Errorf("a credential was decrypted after the first send: the send path must never decrypt")
	}
	o.opens.Add(1)
	return o.inner.Open(env, ad)
}

func (o *countingOpener) Fingerprint(kv, id string, pw []byte) (string, error) {
	return o.inner.Fingerprint(kv, id, pw)
}

func (o *countingOpener) KeyClass(kv string) string { return o.inner.KeyClass(kv) }

func randomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// sealer seals records the way the ingress does.
type sealer struct {
	t    testing.TB
	ring *remotecred.Keyring
}

func (s sealer) record(name, url, username, password, caPEM string, cv uint64) domremote.Record {
	s.t.Helper()
	r := domremote.Record{
		Name: name, ID: "id-" + name, URL: url, Username: username, CAPEM: caPEM,
		CredentialVersion: cv, Revision: cv, Limits: domremote.DefaultLimits(), PasswordSetAtMs: time.Now().UnixMilli(),
	}
	return s.reseal(r, password)
}

func (s sealer) reseal(r domremote.Record, password string) domremote.Record {
	s.t.Helper()
	tuple, err := domremote.TupleOf(r)
	if err != nil {
		s.t.Fatal(err)
	}
	env, err := s.ring.Seal([]byte(password), remotecred.ADFor(tuple))
	if err != nil {
		s.t.Fatal(err)
	}
	r.Credential = env
	r.Fingerprint, _ = s.ring.Fingerprint(env.KV, r.ID, []byte(password))
	return r
}

// cacheNode is one simulated node: a cache over the shared registry.
type cacheNode struct {
	cache  *Cache
	opener *countingOpener
}

func newCacheNode(t testing.TB, reg Registry, secrets Secrets, m *metrics.RemoteMetrics) *cacheNode {
	t.Helper()
	n := &cacheNode{opener: &countingOpener{}}
	n.cache = NewCache(CacheConfig{
		Registry: reg, Secrets: secrets, Metrics: m,
		Posture: Posture{SecurityEnabled: true},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Opener: func(salt []byte) (Opener, error) {
			k, err := remotecred.NewKeyring(secrets.Current, secrets.Previous, salt)
			if err != nil {
				return nil, err
			}
			n.opener.inner = k
			return n.opener, nil
		},
	})
	return n
}

// countingTarget is a TLS server that counts requests, the
// Authorization values it sees, and connections opened and closed.
type countingTarget struct {
	srv      *httptest.Server
	ca       string
	requests atomic.Int64
	opened   atomic.Int64
	closed   atomic.Int64
	mu       sync.Mutex
	authz    map[string]int
}

func newCountingTarget(t testing.TB, cert *tls.Certificate, caPEM string) *countingTarget {
	t.Helper()
	ct := &countingTarget{authz: map[string]int{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.requests.Add(1)
		ct.mu.Lock()
		ct.authz[r.Header.Get("Authorization")]++
		ct.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			ct.opened.Add(1)
		case http.StateClosed, http.StateHijacked:
			ct.closed.Add(1)
		}
	}
	if cert != nil {
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{*cert}}
		ct.ca = caPEM
	}
	srv.StartTLS()
	if cert == nil {
		ct.ca = string(pemCert(srv.Certificate().Raw))
	}
	t.Cleanup(srv.Close)
	ct.srv = srv
	return ct
}

func (ct *countingTarget) sawAuthz(v string) int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.authz[v]
}

func send(t testing.TB, l Lookup, name string) (*http.Response, error) {
	t.Helper()
	e, err := l.Get(name)
	if err != nil {
		return nil, err
	}
	resp, err := e.Do(context.Background(), Outbound{Method: http.MethodPost, Path: "/v1/topics/orders/produce/batch", Body: []byte(`{"messages":[]}`), ContentType: "application/json"})
	if err == nil {
		_, _ = ReadBody(resp, 64)
	}
	return resp, err
}

func basic(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

// The decrypt-once proof: three nodes over one registry, 10,000 sends
// in all. Each node decrypts exactly once; once every node has sent,
// the opener fails the test on any call, and the sends keep working.
// A limits change decrypts nothing and builds a new client; a password
// change and a re-encrypt each decrypt once more per node; a delete
// drops the entry and closes its idle connections.
func TestCacheDecryptsOncePerCredentialVersion(t *testing.T) {
	salt := randomBytes(t, 32)
	oldSecret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	ring, _ := remotecred.NewKeyring(oldSecret, "", salt)
	seal := sealer{t: t, ring: ring}
	target := newCountingTarget(t, nil, "")
	reg := newFakeRegistry(salt)
	reg.put(seal.record("b", target.srv.URL, "repl", "password-one-0123456789ab", target.ca, 1))

	reg0 := prometheus.NewRegistry()
	m := metrics.New(reg0)
	nodes := []*cacheNode{
		newCacheNode(t, reg, Secrets{Current: oldSecret}, m.Remote),
		newCacheNode(t, reg, Secrets{Current: oldSecret}, nil),
		newCacheNode(t, reg, Secrets{Current: oldSecret}, nil),
	}
	for i := range 10_000 {
		n := nodes[i%3]
		if _, err := send(t, n.cache, "b"); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if i == 2 {
			tb := testing.TB(t)
			for _, n := range nodes {
				n.opener.forbid.Store(&tb)
			}
		}
	}
	for i, n := range nodes {
		if got := n.opener.opens.Load(); got != 1 {
			t.Fatalf("node %d decrypted %d times for one credential version, want 1", i, got)
		}
	}
	if got := testutil.ToFloat64(m.Remote.CredentialDecryptsTotal.WithLabelValues("b")); got != 1 {
		t.Fatalf("narad_remote_credential_decrypts_total = %v, want 1", got)
	}
	if target.sawAuthz(basic("repl", "password-one-0123456789ab")) != 10_000 {
		t.Fatalf("target saw %d authenticated requests", target.sawAuthz(basic("repl", "password-one-0123456789ab")))
	}
	for _, n := range nodes {
		n.opener.forbid.Store(nil)
	}

	refreshAll := func() {
		for _, n := range nodes {
			n.cache.Refresh()
		}
	}
	opens := func() (sum int64) {
		for _, n := range nodes {
			sum += n.opener.opens.Load()
		}
		return sum
	}

	// Limits only: zero decrypts, a new client, the same header.
	before, _ := nodes[0].cache.Get("b")
	rec := reg.get("b")
	rec.Limits.MaxInFlight = 48
	rec.Revision++
	reg.put(rec)
	refreshAll()
	after, _ := nodes[0].cache.Get("b")
	if opens() != 3 {
		t.Fatalf("a limits change decrypted: %d opens", opens())
	}
	if after == before || after.client == before.client || &after.authz[0] != &before.authz[0] || after.Limits().MaxInFlight != 48 {
		t.Fatal("a limits change did not rebuild the client around the same header")
	}

	// A password change: one more decrypt per node.
	rec = seal.reseal(reg.get("b"), "password-two-0123456789ab")
	rec.CredentialVersion++
	reg.put(rec)
	refreshAll()
	if opens() != 6 {
		t.Fatalf("a password change: %d opens, want 6", opens())
	}
	if _, err := send(t, nodes[1].cache, "b"); err != nil || target.sawAuthz(basic("repl", "password-two-0123456789ab")) != 1 {
		t.Fatalf("the new password did not reach the target: %v", err)
	}

	// A cluster secret rotation and a re-encrypt: nodes restart with the
	// new secret and the previous one (one decrypt each), then the
	// re-encrypt costs one more per node.
	newSecret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	for i := range nodes {
		nodes[i] = newCacheNode(t, reg, Secrets{Current: newSecret, Previous: oldSecret}, nil)
	}
	if opens() != 3 {
		t.Fatalf("restart with the previous secret: %d opens, want 3", opens())
	}
	newRing, _ := remotecred.NewKeyring(newSecret, oldSecret, salt)
	old := reg.get("b")
	tuple, _ := domremote.TupleOf(old)
	plain, err := newRing.Open(old.Credential, remotecred.ADFor(tuple))
	if err != nil {
		t.Fatal(err)
	}
	rec = sealer{t: t, ring: newRing}.reseal(old, string(plain))
	rec.CredentialVersion++
	reg.put(rec)
	refreshAll()
	if opens() != 6 {
		t.Fatalf("a re-encrypt: %d opens, want 6", opens())
	}

	// Delete: the entry is gone and its idle connections close.
	closedBefore := target.closed.Load()
	openedBefore := target.opened.Load()
	for _, n := range nodes {
		if _, err := send(t, n.cache, "b"); err != nil {
			t.Fatal(err)
		}
	}
	idle := target.opened.Load() - openedBefore
	reg.del("b")
	refreshAll()
	for _, n := range nodes {
		if _, err := n.cache.Get("b"); !errors.Is(err, ErrRemoteMissing) {
			t.Fatalf("Get after delete = %v, want ErrRemoteMissing", err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for target.closed.Load()-closedBefore < idle {
		if time.Now().After(deadline) {
			t.Fatalf("idle connections closed %d, want %d", target.closed.Load()-closedBefore, idle)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A Raft-level change of the URL, the username or the CA that keeps
// the id, the credential version and the ciphertext never reuses the
// header: zero requests reach the new destination, and the node shows
// credential_unreadable.
func TestCacheNeverReusesAHeaderForAMovedRecord(t *testing.T) {
	salt := randomBytes(t, 32)
	secret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	ring, _ := remotecred.NewKeyring(secret, "", salt)
	seal := sealer{t: t, ring: ring}
	home := newCountingTarget(t, nil, "")
	swappedCert, swappedCA := selfSignedTLS(t)
	evil := newCountingTarget(t, &swappedCert, swappedCA)

	moves := map[string]func(r domremote.Record) domremote.Record{
		"url":      func(r domremote.Record) domremote.Record { r.URL = evil.srv.URL; return r },
		"username": func(r domremote.Record) domremote.Record { r.Username = "admin"; return r },
		// The swapped CA is the one the second host's certificate chains
		// to, and the URL points there too: only the sealed CA stops it.
		"ca_pem": func(r domremote.Record) domremote.Record { r.CAPEM = evil.ca; r.URL = evil.srv.URL; return r },
	}
	for name, move := range moves {
		t.Run(name, func(t *testing.T) {
			reg := newFakeRegistry(salt)
			reg.put(seal.record("b", home.srv.URL, "repl", "password-one-0123456789ab", home.ca, 1))
			n := newCacheNode(t, reg, Secrets{Current: secret}, nil)
			if _, err := send(t, n.cache, "b"); err != nil {
				t.Fatal(err)
			}
			evilBefore := evil.requests.Load()
			reg.put(move(reg.get("b"))) // a snapshot install or a Raft-port write
			n.cache.Refresh()
			if _, err := send(t, n.cache, "b"); !errors.Is(err, ErrCredentialUnreadable) {
				t.Fatalf("send after the move = %v, want ErrCredentialUnreadable", err)
			}
			if got := evil.requests.Load() - evilBefore; got != 0 {
				t.Fatalf("%d requests reached the moved destination", got)
			}
			if st := n.cache.Status(); len(st) != 1 || st[0].State != StateCredentialUnreadable || st[0].Fingerprint != "" {
				t.Fatalf("status = %+v", st)
			}
		})
	}

	// A delete and a re-put with a copied id and ciphertext and another
	// URL inside one refresh window: the same.
	reg := newFakeRegistry(salt)
	reg.put(seal.record("b", home.srv.URL, "repl", "password-one-0123456789ab", home.ca, 1))
	n := newCacheNode(t, reg, Secrets{Current: secret}, nil)
	copied := reg.get("b")
	reg.del("b")
	copied.URL = evil.srv.URL
	copied.CAPEM = evil.ca
	reg.put(copied)
	n.cache.Refresh()
	evilBefore := evil.requests.Load()
	if _, err := send(t, n.cache, "b"); !errors.Is(err, ErrCredentialUnreadable) {
		t.Fatalf("send after delete and re-put = %v", err)
	}
	if evil.requests.Load() != evilBefore {
		t.Fatal("a request reached the re-put destination")
	}
}

func TestCachePostureAndSecrets(t *testing.T) {
	salt := randomBytes(t, 32)
	secret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	ring, _ := remotecred.NewKeyring(secret, "", salt)
	target := newCountingTarget(t, nil, "")
	reg := newFakeRegistry(salt)
	reg.put(sealer{t: t, ring: ring}.record("b", target.srv.URL, "repl", "password-one-0123456789ab", target.ca, 1))
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, p := range []Posture{{SecurityEnabled: false}, {SecurityEnabled: true, LegacyClusterAuth: true}} {
		c := NewCache(CacheConfig{Registry: reg, Secrets: Secrets{Current: secret}, Posture: p, Log: quiet})
		if _, err := c.Get("b"); !errors.Is(err, ErrNodeInsecure) {
			t.Fatalf("posture %+v: Get = %v, want ErrNodeInsecure", p, err)
		}
		if _, err := c.Get("absent"); !errors.Is(err, ErrNodeInsecure) {
			t.Fatalf("posture %+v: Get(absent) = %v", p, err)
		}
	}
	secured := Posture{SecurityEnabled: true}
	for _, s := range []Secrets{{}, {Current: base64.StdEncoding.EncodeToString(randomBytes(t, 32))}} {
		c := NewCache(CacheConfig{Registry: reg, Secrets: s, Posture: secured, Log: quiet})
		if _, err := c.Get("b"); !errors.Is(err, ErrCredentialUnreadable) {
			t.Fatalf("secrets %v: Get = %v, want ErrCredentialUnreadable", s.Current != "", err)
		}
	}
	c := NewCache(CacheConfig{Registry: reg, Secrets: Secrets{Current: secret}, Posture: secured, Log: quiet})
	if _, err := c.Get("absent"); !errors.Is(err, ErrRemoteMissing) {
		t.Fatalf("Get(absent) = %v", err)
	}
	ch := c.Changed()
	reg.put(reg.get("b"))
	c.Refresh()
	select {
	case <-ch:
	default:
		t.Fatal("a rebuild did not close Changed")
	}
	st := c.Status()
	if len(st) != 1 || st[0].State != StateReady || st[0].Fingerprint != reg.get("b").Fingerprint || st[0].LastError != "none" {
		t.Fatalf("status = %+v", st)
	}
}

// Every conn_max_age_ms the cache closes an entry's idle connections,
// so a kept-alive pool re-resolves.
func TestCacheRecyclesConnectionsAfterConnMaxAge(t *testing.T) {
	salt := randomBytes(t, 32)
	secret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	ring, _ := remotecred.NewKeyring(secret, "", salt)
	target := newCountingTarget(t, nil, "")
	reg := newFakeRegistry(salt)
	rec := sealer{t: t, ring: ring}.record("b", target.srv.URL, "repl", "password-one-0123456789ab", target.ca, 1)
	rec.Limits.ConnMaxAgeMs = domremote.MinConnMaxAgeMs
	reg.put(rec)
	c := NewCache(CacheConfig{Registry: reg, Secrets: Secrets{Current: secret}, Posture: Posture{SecurityEnabled: true}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	now := time.Now()
	c.now = func() time.Time { return now }
	for range 5 {
		if _, err := send(t, c, "b"); err != nil {
			t.Fatal(err)
		}
	}
	if target.opened.Load() != 1 {
		t.Fatalf("sequential sends opened %d connections", target.opened.Load())
	}
	c.Refresh() // not yet due
	if _, err := send(t, c, "b"); err != nil || target.opened.Load() != 1 {
		t.Fatalf("recycled early: %d connections", target.opened.Load())
	}
	now = now.Add(time.Duration(domremote.MinConnMaxAgeMs) * time.Millisecond)
	c.Refresh()
	if _, err := send(t, c, "b"); err != nil || target.opened.Load() != 2 {
		t.Fatalf("after conn_max_age: %d connections, want 2", target.opened.Load())
	}
}

func pemCert(der []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("-----BEGIN CERTIFICATE-----\n")
	enc := base64.StdEncoding.EncodeToString(der)
	for len(enc) > 64 {
		buf.WriteString(enc[:64] + "\n")
		enc = enc[64:]
	}
	buf.WriteString(enc + "\n-----END CERTIFICATE-----\n")
	return buf.Bytes()
}

func TestCacheMetrics(t *testing.T) {
	salt := randomBytes(t, 32)
	secret := base64.StdEncoding.EncodeToString(randomBytes(t, 32))
	ring, _ := remotecred.NewKeyring(secret, "", salt)
	target := newCountingTarget(t, nil, "")
	reg := newFakeRegistry(salt)
	rec := sealer{t: t, ring: ring}.record("b", target.srv.URL, "repl", "password-one-0123456789ab", target.ca, 1)
	rec.PasswordSetAtMs = time.Now().Add(-time.Hour).UnixMilli()
	reg.put(rec)
	reg.keys.Versions = map[string]domremote.KeyVersion{rec.Credential.KV: {Seals: 7, FirstSealedAtMs: time.Now().Add(-2 * time.Hour).UnixMilli()}}
	m := metrics.New(prometheus.NewRegistry()).Remote
	c := NewCache(CacheConfig{Registry: reg, Secrets: Secrets{Current: secret}, Posture: Posture{SecurityEnabled: true}, Metrics: m, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	c.lastAges = time.Time{}
	c.Refresh()
	if testutil.ToFloat64(m.CredentialState.WithLabelValues("b", StateReady)) != 1 || testutil.ToFloat64(m.CredentialState.WithLabelValues("b", StateCredentialUnreadable)) != 0 {
		t.Fatal("credential state gauge")
	}
	if testutil.ToFloat64(m.CredentialKeyCurrent.WithLabelValues("b")) != 1 || testutil.ToFloat64(m.KeySeals.WithLabelValues("current")) != 7 {
		t.Fatal("key gauges")
	}
	if age := testutil.ToFloat64(m.KeyAgeSeconds.WithLabelValues("current")); age < 7100 || age > 7300 {
		t.Fatalf("key age = %v", age)
	}
	if age := testutil.ToFloat64(m.CredentialAgeSeconds.WithLabelValues("b")); age < 3500 || age > 3700 {
		t.Fatalf("credential age = %v", age)
	}
	reg.del("b")
	c.Refresh()
	if n := testutil.CollectAndCount(m.CredentialState); n != 0 {
		t.Fatalf("a deleted remote left %d state series", n)
	}
}
