package clusterrpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
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
	}, nil
}

// quicClientTLS is the shared client configuration. quic-go clones it
// per dial, so sharing is safe; the point of sharing is the session
// cache, which lets a redial to a peer resume its TLS session instead of
// running a full handshake. Certificates are not verified (see
// quicServerTLSConfig); the ALPN pin is the protocol guard.
var quicClientTLS = &tls.Config{
	InsecureSkipVerify: true,
	NextProtos:         []string{quicALPN},
	ClientSessionCache: tls.NewLRUClientSessionCache(quicClientSessionCacheSize),
}

func quicClientTLSConfig() *tls.Config {
	return quicClientTLS
}
