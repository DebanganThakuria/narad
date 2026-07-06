package metastore

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// ClusterCertDNSName is the DNS SAN every node's cluster certificate must
// carry. The mTLS client verifies that the peer's certificate both chains
// to the cluster CA and presents this name — which authenticates "a holder
// of a cluster-CA cert" without depending on the pod's real hostname or IP
// (raft dials peers by those, and they rotate). Every node presents a cert
// with this SAN and dials with ServerName set to it, so all voters verify
// each other by cluster membership rather than per-host identity.
const ClusterCertDNSName = "narad-cluster.local"

// TLSConfig enables mutual TLS on the Raft metadata transport. Both peers
// present a certificate and verify the other chains to CAs, which
// authenticates cluster membership and encrypts all consensus traffic —
// and that traffic carries the replicated metadata, including user
// password hashes and grants. Without it the transport is plaintext with
// no peer authentication, protected only by network isolation.
//
// Server-cert identity is verified by the cluster CA plus the fixed
// ClusterCertDNSName SAN (see that constant), not the peer's real
// hostname — every voter is an equal peer, so cluster membership is the
// identity that matters.
type TLSConfig struct {
	// Certificate is this node's cluster certificate and private key. Its
	// SANs must include ClusterCertDNSName.
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
		RootCAs:      t.CAs,
		// Verify the peer against the fixed cluster SAN, not the dial
		// address — decouples cert identity from the pod's rotating DNS/IP.
		ServerName: ClusterCertDNSName,
		MinVersion: tls.VersionTLS13,
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
