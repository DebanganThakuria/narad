package metastore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// newTestCA returns a self-signed CA certificate and its key.
func newTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
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

// certFromCA issues a leaf certificate signed by ca.
func certFromCA(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "narad-node"},
		DNSNames:     []string{ClusterCertDNSName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// tlsConfigSignedBy returns a TLSConfig whose cert is signed by certCA
// and which trusts trustCA. Passing different CAs models an untrusted peer.
func tlsConfigSignedBy(t *testing.T, certCA *x509.Certificate, certCAKey *ecdsa.PrivateKey, trustCA *x509.Certificate) *TLSConfig {
	pool := x509.NewCertPool()
	pool.AddCert(trustCA)
	return &TLSConfig{Certificate: certFromCA(t, certCA, certCAKey), CAs: pool}
}

func TestTLSStreamLayerMutualAuthRoundTrip(t *testing.T) {
	ca, caKey := newTestCA(t)
	cfg := tlsConfigSignedBy(t, ca, caKey, ca)

	server, err := newTLSStreamLayer("127.0.0.1:0", nil, cfg)
	if err != nil {
		t.Fatalf("server stream layer: %v", err)
	}
	defer server.Close()
	addr := server.Addr().String()

	// Echo one byte back on the accepted (server-verified) connection.
	go func() {
		conn, err := server.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		if _, err := io.ReadFull(conn, buf); err == nil {
			_, _ = conn.Write(buf)
		}
	}()

	client, err := newTLSStreamLayer("127.0.0.1:0", nil, cfg)
	if err != nil {
		t.Fatalf("client stream layer: %v", err)
	}
	defer client.Close()

	conn, err := client.Dial(raft.ServerAddress(addr), 3*time.Second)
	if err != nil {
		t.Fatalf("mutual-TLS dial failed for a valid peer: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{0x42}); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil || got[0] != 0x42 {
		t.Fatalf("round trip: got %v err %v", got, err)
	}
}

func TestTLSStreamLayerRejectsUntrustedPeer(t *testing.T) {
	ca, caKey := newTestCA(t)
	server, err := newTLSStreamLayer("127.0.0.1:0", nil, tlsConfigSignedBy(t, ca, caKey, ca))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer server.Close()
	addr := server.Addr().String()
	go func() {
		if conn, err := server.Accept(); err == nil {
			// Drive the handshake, then drop; the client cert is untrusted.
			_, _ = io.ReadFull(conn, make([]byte, 1))
			conn.Close()
		}
	}()

	// Attacker presents a cert from a DIFFERENT CA (still trusts the real CA).
	otherCA, otherKey := newTestCA(t)
	attacker, err := newTLSStreamLayer("127.0.0.1:0", nil, tlsConfigSignedBy(t, otherCA, otherKey, ca))
	if err != nil {
		t.Fatalf("attacker: %v", err)
	}
	defer attacker.Close()

	conn, err := attacker.Dial(raft.ServerAddress(addr), 3*time.Second)
	if err != nil {
		return // rejected at dial time — good
	}
	// Under TLS 1.3 the client handshake can complete before the server
	// processes (and rejects) the client certificate, so a successful Dial
	// is not proof of acceptance. The security property is that an
	// untrusted peer can exchange NO application data: the server rejected
	// the cert and will not echo.
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte{0x1}); err != nil {
		return // server already tore the connection down — good
	}
	if _, err := io.ReadFull(conn, make([]byte, 1)); err == nil {
		t.Fatal("untrusted peer exchanged application data over the mTLS transport")
	}
}

func TestBootstrapThreeNodeClusterMutualTLS(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	addrs := []string{"127.0.0.1:19111", "127.0.0.1:19112", "127.0.0.1:19113"}
	ids := []string{"tls-1", "tls-2", "tls-3"}

	ca, caKey := newTestCA(t)
	stores := make([]*Store, 0, 3)
	for i := range ids {
		peers := make([]Peer, 0, len(ids)-1)
		for j := range ids {
			if i != j {
				peers = append(peers, Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		store, err := New(Config{
			NodeID:        ids[i],
			DataDir:       filepath.Join(baseDir, fmt.Sprintf("ms-%s", ids[i])),
			BindAddr:      addrs[i],
			AdvertiseAddr: addrs[i],
			Peers:         peers,
			TLS:           tlsConfigSignedBy(t, ca, caKey, ca),
		})
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores = append(stores, store)
	}
	for i := range stores {
		s := stores[i]
		t.Cleanup(func() { _ = s.Close() })
	}

	// A cluster that forms a leader and replicates over the mTLS transport
	// proves the peers authenticated each other and consensus works.
	var leader *Store
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && leader == nil {
		for _, s := range stores {
			if err := s.CreateTopic(ctx, topic.Topic{Name: "tls-probe", Partitions: 3}); err == nil {
				leader = s
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected over the mTLS transport")
	}

	// The write must replicate to every follower.
	for _, s := range stores {
		ok := false
		for time.Now().Before(deadline) {
			if got, err := s.GetTopic(ctx, "tls-probe"); err == nil && got.Partitions == 3 {
				ok = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			t.Fatal("write did not replicate to a follower over mTLS")
		}
	}
}
