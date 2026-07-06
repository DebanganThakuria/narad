package metastore

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// TLSConfig enables mutual TLS on the Raft metadata transport. Both peers
// present a certificate and verify the other chains to CAs, which
// authenticates cluster membership and encrypts all consensus traffic —
// and that traffic carries the replicated metadata, including user
// password hashes and grants. Without it the transport is plaintext with
// no peer authentication, protected only by network isolation.
//
// Peer identity is CA-based, not hostname-based: raft dials pods by their
// ever-changing DNS names and IPs, and every voter is an equal peer, so
// "holds a certificate signed by the cluster CA" is the right identity to
// check. Hostname verification is therefore disabled and replaced with an
// explicit chain-to-CA check.
type TLSConfig struct {
	// Certificate is this node's cluster certificate and private key.
	Certificate tls.Certificate
	// CAs is the pool of trusted cluster CAs peer certs must chain to.
	CAs *x509.CertPool
}

func (t *TLSConfig) serverConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.Certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    t.CAs,
		MinVersion:   tls.VersionTLS13,
	}
}

func (t *TLSConfig) clientConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{t.Certificate},
		// Skip the default hostname check and verify chain-to-CA instead
		// (see the type doc). This is deliberate, not a downgrade: the peer
		// still must present a cert signed by the cluster CA.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: chainVerifier(t.CAs),
		MinVersion:            tls.VersionTLS13,
	}
}

// chainVerifier returns a VerifyPeerCertificate that checks the peer's
// leaf certificate chains to roots, ignoring hostname.
func chainVerifier(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("metastore tls: peer presented no certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			if c, err := x509.ParseCertificate(raw); err == nil {
				intermediates.AddCert(c)
			}
		}
		_, err = leaf.Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		})
		return err
	}
}

// tlsStreamLayer is a raft.StreamLayer that dials and accepts over mTLS.
type tlsStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	dialConf  *tls.Config
}

// newTLSStreamLayer listens on bindAddr with the server (mTLS) config and
// dials peers with the client config. advertise is the address raft
// advertises to peers.
func newTLSStreamLayer(bindAddr string, advertise net.Addr, cfg *TLSConfig) (*tlsStreamLayer, error) {
	listener, err := tls.Listen("tcp", bindAddr, cfg.serverConfig())
	if err != nil {
		return nil, err
	}
	return &tlsStreamLayer{listener: listener, advertise: advertise, dialConf: cfg.clientConfig()}, nil
}

func (s *tlsStreamLayer) Accept() (net.Conn, error) { return s.listener.Accept() }
func (s *tlsStreamLayer) Close() error              { return s.listener.Close() }

func (s *tlsStreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.listener.Addr()
}

func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), s.dialConf)
}
