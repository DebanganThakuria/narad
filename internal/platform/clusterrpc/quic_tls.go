package clusterrpc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"time"
)

// quicALPN identifies Narad's cluster-RPC QUIC protocol during the TLS
// handshake. All nodes in a cluster must agree on it.
const quicALPN = "narad-cluster-quic-v1"

// quicServerTLSConfig generates an ephemeral self-signed certificate at
// startup. QUIC requires TLS, but cluster traffic stays inside the
// deployment's trust boundary, so peers pin the ALPN protocol instead of
// verifying certificates (see quicClientTLSConfig).
func quicServerTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
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

func quicClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{quicALPN},
	}
}
