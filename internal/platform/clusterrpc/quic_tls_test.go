package clusterrpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"strings"
	"testing"
	"time"
)

func selfSignedLeaf(t *testing.T, notBefore, notAfter time.Time) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: notBefore, NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return leaf
}

// The client replaces default chain verification (no CA exists for the
// ephemeral certificates) with checks that are meaningful for the cluster
// transport: TLS 1.3, the pinned ALPN, and one valid self-signed leaf.
func TestVerifyClusterPeer(t *testing.T) {
	now := time.Now()
	good := selfSignedLeaf(t, now.Add(-time.Minute), now.Add(time.Hour))
	expired := selfSignedLeaf(t, now.Add(-2*time.Hour), now.Add(-time.Hour))
	other := selfSignedLeaf(t, now.Add(-time.Minute), now.Add(time.Hour))

	cases := []struct {
		name string
		cs   tls.ConnectionState
		want string // substring of the error, "" for success
	}{
		{"valid", tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: quicALPN, PeerCertificates: []*x509.Certificate{good}}, ""},
		{"tls12", tls.ConnectionState{Version: tls.VersionTLS12, NegotiatedProtocol: quicALPN, PeerCertificates: []*x509.Certificate{good}}, "want 1.3"},
		{"wrong alpn", tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: "h3", PeerCertificates: []*x509.Certificate{good}}, "ALPN"},
		{"no cert", tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: quicALPN}, "presented 0 certificates"},
		{"chain", tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: quicALPN, PeerCertificates: []*x509.Certificate{good, other}}, "presented 2 certificates"},
		{"expired", tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: quicALPN, PeerCertificates: []*x509.Certificate{expired}}, "not valid at"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyClusterPeer(tc.cs)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("verifyClusterPeer() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verifyClusterPeer() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

// The client config keeps the custom verifier wired in and pins TLS 1.3,
// so a future edit cannot silently drop back to unverified connections.
func TestQUICClientTLSConfigUsesClusterVerifier(t *testing.T) {
	cfg := quicClientTLSConfig()
	if cfg.VerifyConnection == nil {
		t.Fatal("client TLS config has no VerifyConnection; certificate checking would be fully disabled")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = 0x%04x, want TLS 1.3", cfg.MinVersion)
	}
	srv, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quicServerTLSConfig: %v", err)
	}
	if srv.MinVersion != tls.VersionTLS13 {
		t.Fatalf("server MinVersion = 0x%04x, want TLS 1.3", srv.MinVersion)
	}
}
