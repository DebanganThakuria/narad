package clusterrpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"time"
)

// quicALPN identifies Narad's cluster-RPC QUIC protocol during the TLS
// handshake. All nodes in a cluster must agree on it.
const quicALPN = "narad-cluster-quic-v1"

// quicClientSessionCacheSize bounds the TLS session tickets a client keeps
// for resumption: one per peer is plenty, so 64 covers any cluster size
// this transport is designed for.
const quicClientSessionCacheSize = 64

// quicServerTLSConfig generates an ephemeral self-signed certificate at
// startup. QUIC requires TLS, but cluster traffic stays inside the
// deployment's trust boundary, so peers pin the ALPN protocol instead of
// verifying certificates (see quicClientTLSConfig). The key is ECDSA
// P-256: it generates in milliseconds rather than the hundreds of
// milliseconds RSA-2048 needs, and signs handshakes far faster.
func quicServerTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  key,
		}},
		NextProtos: []string{quicALPN},
		MinVersion: tls.VersionTLS13,
	}, nil
}

// quicClientSelfSignedTLS is the shared client configuration. quic-go clones it
// per dial, so sharing is safe; the point of sharing is the session
// cache, which lets a redial to a peer resume its TLS session instead of
// running a full handshake.
//
// Peer AUTHENTICATION on this transport is not certificate-based: every
// node presents an ephemeral self-signed certificate (see
// quicServerTLSConfig) and proves membership with the cluster-secret
// HMAC exchanged on every stream (auth.go). TLS provides confidentiality
// and integrity. So the default chain verification against system roots
// is bypassed (it could never succeed) and replaced by
// verifyClusterPeerCertificate and verifyClusterConnection, which enforce
// what is checkable here: exactly one currently valid self-signed leaf,
// TLS 1.3, and the pinned ALPN.
var quicClientSelfSignedTLS = newSelfSignedClusterClientTLS()

// newSelfSignedClusterClientTLS builds the client configuration for a
// transport whose peers present ephemeral self-signed certificates.
// InsecureSkipVerify only bypasses system-root chain building, which
// cannot succeed for a self-signed leaf; certificate verification is NOT
// disabled: VerifyPeerCertificate replaces it with the self-signed leaf
// checks, and VerifyConnection pins TLS 1.3 and the ALPN.
func newSelfSignedClusterClientTLS() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify:    true, // replaced by VerifyPeerCertificate, see above
		VerifyPeerCertificate: verifyClusterPeerCertificate,
		VerifyConnection:      verifyClusterConnection,
		NextProtos:            []string{quicALPN},
		MinVersion:            tls.VersionTLS13,
		ClientSessionCache:    tls.NewLRUClientSessionCache(quicClientSessionCacheSize),
	}
}

// verifyClusterPeerCertificate is the client-side certificate check for
// the cluster RPC transport. It replaces the default chain verification
// (there is no CA: peers use ephemeral self-signed certificates) with
// the checks that are meaningful here: exactly one currently valid
// self-signed leaf whose signature verifies under its own public key,
// which proves the peer holds the private key for the certificate it
// presented. Membership itself is proven by the per-stream cluster-secret
// HMAC. verifiedChains is always nil on this path.
func verifyClusterPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) != 1 {
		return fmt.Errorf("cluster rpc: peer presented %d certificates, want exactly one ephemeral self-signed leaf", len(rawCerts))
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("cluster rpc: parse peer certificate: %w", err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return fmt.Errorf("cluster rpc: peer certificate not valid at %s (valid %s to %s)", now.Format(time.RFC3339), leaf.NotBefore.Format(time.RFC3339), leaf.NotAfter.Format(time.RFC3339))
	}
	// Self-signed: the certificate's signature must verify under its own
	// public key (CheckSignatureFrom would additionally demand a CA
	// certificate, which an ephemeral leaf is not).
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature); err != nil {
		return fmt.Errorf("cluster rpc: peer certificate is not self-signed: %w", err)
	}
	return nil
}

// verifyClusterConnection pins the protocol parameters the certificate
// check cannot see: TLS 1.3 and the cluster ALPN.
func verifyClusterConnection(cs tls.ConnectionState) error {
	if cs.Version < tls.VersionTLS13 {
		return fmt.Errorf("cluster rpc: peer negotiated TLS 0x%04x, want 1.3", cs.Version)
	}
	if cs.NegotiatedProtocol != quicALPN {
		return fmt.Errorf("cluster rpc: peer negotiated ALPN %q, want %q", cs.NegotiatedProtocol, quicALPN)
	}
	return nil
}

func quicClientTLSConfig() *tls.Config {
	return quicClientSelfSignedTLS
}
