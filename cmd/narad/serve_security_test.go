package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
)

// The authenticator's queue gauge is live in the binary: building the
// authenticator registers it on the process registry.
func TestBuildAuthenticatorRegistersTheVerifyQueueGauge(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = true
	reg := prometheus.NewRegistry()
	store := openLeaderStore(t, t.TempDir())

	if auth := buildAuthenticator(cfg, store, reg, slog.New(slog.NewTextHandler(io.Discard, nil))); auth == nil {
		t.Fatal("buildAuthenticator returned nil with security enabled")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "narad_auth_verify_queued" {
			return
		}
	}
	t.Fatal("narad_auth_verify_queued is not registered")
}

// The CA file is parsed into the bundle the metastore reads the expiry
// of, every certificate in it, in order, next to the pool that decides
// trust; the node certificate's dates come with the key pair.
func TestClusterTLSConfigParsesTheCABundle(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Truncate(time.Second)
	oldCA, oldKey := pemCA(t, now.Add(30*24*time.Hour))
	newCA, _ := pemCA(t, now.Add(3650*24*time.Hour))
	leafNotAfter := now.Add(90 * 24 * time.Hour)
	leafPEM, keyPEM := pemLeaf(t, oldCA, oldKey, leafNotAfter)

	write := func(name string, parts ...[]byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, bytes.Join(parts, nil), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	sec := config.SecurityConfig{
		ClusterTLSCertFile: write("tls.crt", leafPEM),
		ClusterTLSKeyFile:  write("tls.key", keyPEM),
		ClusterTLSCAFile: write("ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: oldCA.Raw}),
			[]byte("# the CA being rotated in\n"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: newCA.Raw})),
	}
	cfg, err := clusterTLSConfig(sec)
	if err != nil {
		t.Fatalf("clusterTLSConfig: %v", err)
	}
	if len(cfg.CACertificates) != 2 || !cfg.CACertificates[0].Equal(oldCA) || !cfg.CACertificates[1].Equal(newCA) {
		t.Fatalf("CA bundle = %d certificates, want the old and the new CA in file order", len(cfg.CACertificates))
	}
	if got := cfg.LeafNotAfter(); !got.Equal(leafNotAfter) {
		t.Fatalf("LeafNotAfter = %v, want %v", got, leafNotAfter)
	}
	if got := cfg.CANotAfter(); !got.Equal(oldCA.NotAfter) {
		t.Fatalf("CANotAfter = %v, want the earliest CA expiry %v", got, oldCA.NotAfter)
	}
}

// pemCA returns a self-signed CA certificate valid until notAfter.
func pemCA(t *testing.T, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter,
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return ca, key
}

// pemLeaf returns a PEM node certificate signed by ca, valid until
// notAfter, and its PEM private key.
func pemLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "narad-node"},
		DNSNames:  []string{metastore.ClusterCertDNSName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
