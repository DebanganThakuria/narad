package metastore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const day = 24 * time.Hour

// caWithDates returns a self-signed CA valid from notBefore to notAfter.
func caWithDates(t *testing.T, notBefore, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return ca, key
}

// leafWithDates issues a node certificate signed by ca, valid from
// notBefore to notAfter. Its Leaf field is left nil, as a key pair
// loaded without parsing has it.
func leafWithDates(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "narad-node"},
		DNSNames:     []string{ClusterCertDNSName},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// tlsWithDates returns a TLSConfig whose leaf runs from leafFrom to
// leafUntil and whose bundle holds one CA per entry of caUntil (each
// valid from an hour ago), the leaf signed by the first.
func tlsWithDates(t *testing.T, leafFrom, leafUntil time.Time, caUntil ...time.Time) *TLSConfig {
	t.Helper()
	pool := x509.NewCertPool()
	cfg := &TLSConfig{CAs: pool}
	var signer *x509.Certificate
	var signerKey *ecdsa.PrivateKey
	for i, until := range caUntil {
		ca, key := caWithDates(t, time.Now().Add(-time.Hour).Truncate(time.Second), until)
		if i == 0 {
			signer, signerKey = ca, key
		}
		pool.AddCert(ca)
		cfg.CACertificates = append(cfg.CACertificates, ca)
	}
	cfg.Certificate = leafWithDates(t, signer, signerKey, leafFrom, leafUntil)
	return cfg
}

// tlsLog keeps every record logged through it.
type tlsLog struct {
	mu      sync.Mutex
	records []slog.Record
}

func (l *tlsLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *tlsLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *tlsLog) WithGroup(string) slog.Handler            { return l }

func (l *tlsLog) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Clone())
	return nil
}

// messages returns the messages logged at level that contain part.
func (l *tlsLog) messages(level slog.Level, part string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if r.Level == level && strings.Contains(r.Message, part) {
			out = append(out, r.Message)
		}
	}
	return out
}

// notAfterSeries returns narad_raft_tls_cert_not_after_seconds by kind.
func notAfterSeries(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "narad_raft_tls_cert_not_after_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "kind" {
					out[l.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	return out
}

// The node exports when its Raft certificate and its CA bundle expire,
// so an alert can fire weeks ahead. "ca" is the earliest CA in the
// bundle: a CA rotation keeps two, and the old one runs out first.
func TestRaftTLSExpiryGaugeReportsLeafAndCANotAfter(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cfg := tlsWithDates(t, now.Add(-time.Hour), now.Add(90*day), now.Add(400*day), now.Add(200*day))
	reg := prometheus.NewRegistry()
	s, err := New(Config{NodeID: "tls-gauge", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0", TLS: cfg, Registerer: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = s.Close()
		}
	})

	got := notAfterSeries(t, reg)
	want := map[string]float64{"leaf": float64(now.Add(90 * day).Unix()), "ca": float64(now.Add(200 * day).Unix())}
	if len(got) != 2 || got["leaf"] != want["leaf"] || got["ca"] != want["ca"] {
		t.Fatalf("narad_raft_tls_cert_not_after_seconds = %v, want %v", got, want)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	if got := notAfterSeries(t, reg); len(got) != 0 {
		t.Fatalf("series still exported after Close: %v", got)
	}

	// Without a parsed bundle only the leaf is known.
	cfg.CACertificates = nil
	reg = prometheus.NewRegistry()
	s2, err := New(Config{NodeID: "tls-gauge-2", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0", TLS: cfg, Registerer: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if got := notAfterSeries(t, reg); len(got) != 1 || got["leaf"] != want["leaf"] {
		t.Fatalf("series without a parsed CA bundle = %v, want the leaf only", got)
	}
}

// The expiry is announced ahead, once per threshold: a warning 30 days
// and 7 days before, an error 1 day before, and an error once expired,
// repeated every 24 hours. Every line says that renewing needs a
// restart, since Narad reads the files only at startup.
func TestRaftTLSExpiryWarnsAheadOfExpiry(t *testing.T) {
	notAfter := time.Now().Add(31 * day).Truncate(time.Second)
	cfg := tlsWithDates(t, time.Now().Add(-time.Hour).Truncate(time.Second), notAfter, notAfter.Add(365*day))
	logs := &tlsLog{}
	w := newTLSExpiryWatch(cfg, slog.New(logs))

	steps := []struct {
		at          time.Duration // before notAfter (negative: after)
		warns, errs int
	}{
		{31 * day, 0, 0},
		{29 * day, 1, 0},
		{28 * day, 1, 0},
		{8 * day, 1, 0},
		{6 * day, 2, 0},
		{2 * day, 2, 0},
		{day / 2, 2, 1},
		{day / 4, 2, 1},
		{-time.Minute, 2, 2},
		{-23 * time.Hour, 2, 2},
		{-24*time.Hour - 2*time.Minute, 2, 3},
		{-30 * time.Hour, 2, 3},
	}
	for _, step := range steps {
		w.check(notAfter.Add(-step.at))
		warns := logs.messages(slog.LevelWarn, "raft TLS certificate")
		errs := logs.messages(slog.LevelError, "raft TLS certificate")
		if len(warns) != step.warns || len(errs) != step.errs {
			t.Fatalf("at %v before expiry: %d warnings %q and %d errors %q, want %d and %d",
				step.at, len(warns), warns, len(errs), errs, step.warns, step.errs)
		}
	}
	for _, msg := range append(logs.messages(slog.LevelWarn, ""), logs.messages(slog.LevelError, "")...) {
		if !strings.Contains(msg, "only at startup") || !strings.Contains(msg, "restart") {
			t.Fatalf("expiry line %q does not say that a renewed certificate needs a restart", msg)
		}
	}
	if n := len(logs.messages(slog.LevelError, "has expired")); n != 2 {
		t.Fatalf("expired lines = %d, want 2 (at expiry and 24 h later)", n)
	}
}

// A CA in the bundle that runs out is announced like the leaf, under
// its own name.
func TestRaftTLSExpiryWarnsAheadOfCAExpiry(t *testing.T) {
	caUntil := time.Now().Add(10 * day).Truncate(time.Second)
	cfg := tlsWithDates(t, time.Now().Add(-time.Hour).Truncate(time.Second), time.Now().Add(300*day), caUntil)
	logs := &tlsLog{}
	w := newTLSExpiryWatch(cfg, slog.New(logs))
	w.check(caUntil.Add(-6 * day))
	if got := logs.messages(slog.LevelWarn, "raft TLS CA certificate"); len(got) != 1 {
		t.Fatalf("CA warnings 6 days before it expires = %q, want one", got)
	}
	if got := logs.messages(slog.LevelWarn, "raft TLS certificate"); len(got) != 0 {
		t.Fatalf("leaf warnings with 300 days left = %q, want none", got)
	}
}

// At startup the node logs the dates it runs with, and an error at once
// when the certificate has expired or is not valid yet: such a node
// cannot open new Raft connections, and it starts anyway.
func TestRaftTLSCertificateDatesAreLoggedAtStart(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cases := []struct {
		name      string
		from, to  time.Time
		wantError string
	}{
		{"valid", now.Add(-time.Hour), now.Add(90 * day), ""},
		{"expired", now.Add(-48 * time.Hour), now.Add(-time.Hour), "has expired"},
		{"not yet valid", now.Add(time.Hour), now.Add(90 * day), "not valid yet"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logs := &tlsLog{}
			s, err := New(Config{
				NodeID: "tls-start", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
				TLS: tlsWithDates(t, c.from, c.to, now.Add(365*day)), Log: slog.New(logs),
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			deadline := time.Now().Add(5 * time.Second)
			for len(logs.messages(slog.LevelInfo, "raft TLS certificate in use")) == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := logs.messages(slog.LevelInfo, "raft TLS certificate in use"); len(got) != 1 {
				t.Fatalf("start lines = %q, want one", got)
			}
			errs := logs.messages(slog.LevelError, "raft TLS certificate")
			switch {
			case c.wantError == "" && len(errs) != 0:
				t.Fatalf("errors for a valid certificate: %q", errs)
			case c.wantError != "" && (len(errs) != 1 || !strings.Contains(errs[0], c.wantError)):
				t.Fatalf("errors = %q, want one saying %q", errs, c.wantError)
			}
		})
	}
}

// /readyz lists an expired Raft certificate, or a CA bundle that has
// run out entirely, without turning the node unready: one certificate
// usually serves every node, so it expires everywhere at once, and
// failing readiness would empty the Services.
func TestReadinessDegradedListsAnExpiredRaftCertificate(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cases := []struct {
		name string
		cfg  *TLSConfig
		want []string
	}{
		{"valid", tlsWithDates(t, now.Add(-time.Hour), now.Add(day), now.Add(day)), nil},
		{"leaf expired", tlsWithDates(t, now.Add(-48*time.Hour), now.Add(-time.Hour), now.Add(day)), []string{"raft_tls_certificate_expired"}},
		{"one CA of two expired", tlsWithDates(t, now.Add(-time.Hour), now.Add(day), now.Add(day), now.Add(-time.Minute)), nil},
		{"every CA expired", tlsWithDates(t, now.Add(-time.Hour), now.Add(day), now.Add(-time.Minute), now.Add(-2*time.Minute)), []string{"raft_tls_ca_expired"}},
		{"both", tlsWithDates(t, now.Add(-48*time.Hour), now.Add(-time.Hour), now.Add(-time.Minute)), []string{"raft_tls_certificate_expired", "raft_tls_ca_expired"}},
	}
	for _, c := range cases {
		if got := c.cfg.degraded(now); !slices.Equal(got, c.want) {
			t.Errorf("%s: degraded = %q, want %q", c.name, got, c.want)
		}
	}

	expired := tlsWithDates(t, now.Add(-48*time.Hour), now.Add(-time.Hour), now.Add(day))
	s, err := New(Config{NodeID: "tls-expired", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0", TLS: expired})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if got := s.ReadinessDegraded(); !slices.Equal(got, []string{"raft_tls_certificate_expired"}) {
		t.Fatalf("ReadinessDegraded with an expired leaf = %q", got)
	}
	plain, err := New(Config{NodeID: "plain", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if got := plain.ReadinessDegraded(); got != nil {
		t.Fatalf("ReadinessDegraded without TLS = %q, want nil", got)
	}
}
