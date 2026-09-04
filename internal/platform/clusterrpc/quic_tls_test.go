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
// transport: exactly one valid self-signed leaf, plus TLS 1.3 and the
// pinned ALPN on the connection.
func TestVerifyClusterPeerCertificate(t *testing.T) {
	now := time.Now()
	good := selfSignedLeaf(t, now.Add(-time.Minute), now.Add(time.Hour))
	expired := selfSignedLeaf(t, now.Add(-2*time.Hour), now.Add(-time.Hour))
	other := selfSignedLeaf(t, now.Add(-time.Minute), now.Add(time.Hour))
	tampered := append([]byte(nil), good.Raw...)
	tampered[len(tampered)-1] ^= 0x01 // corrupt the signature bytes

	cases := []struct {
		name string
		raw  [][]byte
		want string // substring of the error, "" for success
	}{
		{"valid", [][]byte{good.Raw}, ""},
		{"no cert", nil, "presented 0 certificates"},
		{"chain", [][]byte{good.Raw, other.Raw}, "presented 2 certificates"},
		{"expired", [][]byte{expired.Raw}, "not valid at"},
		{"bad signature", [][]byte{tampered}, ""}, // filled below: parse or signature error, either is a rejection
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyClusterPeerCertificate(tc.raw, nil)
			if tc.name == "bad signature" {
				if err == nil {
					t.Fatal("tampered certificate accepted")
				}
				return
			}
			if tc.want == "" {
				if err != nil {
					t.Fatalf("verifyClusterPeerCertificate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verifyClusterPeerCertificate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestVerifyClusterConnection(t *testing.T) {
	if err := verifyClusterConnection(tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: quicALPN}); err != nil {
		t.Fatalf("valid connection rejected: %v", err)
	}
	if err := verifyClusterConnection(tls.ConnectionState{Version: tls.VersionTLS12, NegotiatedProtocol: quicALPN}); err == nil || !strings.Contains(err.Error(), "want 1.3") {
		t.Fatalf("TLS 1.2 error = %v", err)
	}
	if err := verifyClusterConnection(tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: "h3"}); err == nil || !strings.Contains(err.Error(), "ALPN") {
		t.Fatalf("wrong ALPN error = %v", err)
	}
}

// The client config keeps the custom verifier wired in and pins TLS 1.3,
// so a future edit cannot silently drop back to unverified connections.
func TestQUICClientTLSConfigUsesClusterVerifier(t *testing.T) {
	cfg := quicClientTLSConfig()
	if cfg.VerifyPeerCertificate == nil || cfg.VerifyConnection == nil {
		t.Fatal("client TLS config lost its custom verifiers; certificate checking would be fully disabled")
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
