//go:build cluster

package cluster

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// ---- certificates -----------------------------------------------------------------

// certAuthority is a throwaway cluster CA.
type certAuthority struct {
	name string
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	pem  []byte
}

func newCA(t *testing.T, name string) *certAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &certAuthority{name: name, key: key, cert: cert, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// nodeCert is one issued node certificate: the PEM files a node is
// started with, and the serial the test uses to recognise it on the wire.
type nodeCert struct {
	certPEM, keyPEM []byte
	serial          string
	tls             tls.Certificate
}

// issue signs a node certificate carrying the cluster SAN the mTLS
// transport verifies (metastore.ClusterCertDNSName), for both server
// and client use, since every voter dials and accepts.
func (ca *certAuthority) issue(t *testing.T, cn string) nodeCert {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{metastore.ClusterCertDNSName},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	nc := nodeCert{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		serial:  tmpl.SerialNumber.String(),
	}
	nc.tls, err = tls.X509KeyPair(nc.certPEM, nc.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return nc
}

func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func poolOf(cas ...*certAuthority) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, ca := range cas {
		pool.AddCert(ca.cert)
	}
	return pool
}

func bundleOf(cas ...*certAuthority) []byte {
	var out []byte
	for _, ca := range cas {
		out = append(out, ca.pem...)
	}
	return out
}

// tlsPaths are the fixed file names each node reads its Raft TLS
// material from; the contents change, the paths never do, which is how a
// rotation works on a real deployment (a re-mounted secret).
func (c *cluster) tlsDir(i int) string {
	return filepath.Join(c.dir, c.nodes[i].id+"-tls")
}

// installTLS writes node i's certificate, key and trust bundle, each
// through a temp file and rename so a node never reads a half-written
// file, and points the node's env at them.
func (c *cluster) installTLS(i int, cert nodeCert, trust []byte) {
	c.t.Helper()
	dir := c.tlsDir(i)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Fatal(err)
	}
	for name, body := range map[string][]byte{"cert.pem": cert.certPEM, "key.pem": cert.keyPEM, "ca.pem": trust} {
		tmp := filepath.Join(dir, name+".tmp")
		if err := os.WriteFile(tmp, body, 0o600); err != nil {
			c.t.Fatal(err)
		}
		if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
			c.t.Fatal(err)
		}
	}
	n := c.nodes[i]
	n.env["NARAD_CLUSTER_TLS_CERT_FILE"] = filepath.Join(dir, "cert.pem")
	n.env["NARAD_CLUSTER_TLS_KEY_FILE"] = filepath.Join(dir, "key.pem")
	n.env["NARAD_CLUSTER_TLS_CA_FILE"] = filepath.Join(dir, "ca.pem")
	// TLS files win over the plaintext opt-in the harness sets globally;
	// unset it anyway so the log line says which transport this is.
	n.env["NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT"] = "false"
}

// raftHandshake connects to node i's Raft port the way a peer does:
// mutual TLS, presenting client and trusting roots, verifying the
// cluster SAN. It returns the certificate the node served. A rejection
// of OUR certificate arrives as a TLS alert on the first read (TLS 1.3
// finishes the client side of the handshake before the server verifies
// the client certificate), so the read is part of the probe: a node
// that accepted us has nothing to say and the read times out, which is
// success.
func (c *cluster) raftHandshake(i int, client tls.Certificate, roots *x509.CertPool) (*x509.Certificate, error) {
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprint(c.nodes[i].raftPort))
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, &tls.Config{
		Certificates: []tls.Certificate{client},
		RootCAs:      roots,
		ServerName:   metastore.ClusterCertDNSName,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	served := conn.ConnectionState().PeerCertificates[0]
	_ = conn.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			return served, nil
		}
		return served, err
	}
	return served, nil
}

// assertServes checks node i serves a certificate with the given serial
// to a peer presenting client. Retried briefly: the listener is up
// before the node is ready.
func (c *cluster) assertServes(i int, client tls.Certificate, roots *x509.CertPool, wantSerial string) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		served, err := c.raftHandshake(i, client, roots)
		if err == nil {
			if got := served.SerialNumber.String(); got != wantSerial {
				c.t.Fatalf("%s serves certificate serial %s, want %s", c.nodes[i].id, got, wantSerial)
			}
			return
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("%s: mTLS handshake as a trusted peer failed: %v", c.nodes[i].id, lastErr)
}

// assertRejects checks node i refuses a peer presenting client (a
// certificate from a CA the node no longer trusts).
func (c *cluster) assertRejects(i int, client tls.Certificate, roots *x509.CertPool) {
	c.t.Helper()
	_, err := c.raftHandshake(i, client, roots)
	if err == nil {
		c.t.Fatalf("%s accepted a peer certificate it should not trust", c.nodes[i].id)
	}
	c.t.Logf("%s rejected the untrusted peer certificate: %v", c.nodes[i].id, err)
}

// ---- rolling restart ---------------------------------------------------------------

// rollResult is what one restart cost the cluster.
type rollResult struct {
	wasLeader bool
	stopTook  time.Duration
	readyTook time.Duration
	// leaderGap is how long after the node exited the survivors were
	// without a leader (0 when the node was a follower, or when the
	// graceful shutdown's leadership transfer completed before exit).
	leaderGap time.Duration
	// elections counts the distinct Raft terms the survivors campaigned
	// in since the stop: a graceful leader restart hands leadership over
	// with TimeoutNow (the target campaigns once) and a follower restart
	// needs none; two survivors campaigning in the same term after a
	// heartbeat timeout is still one election.
	elections int
	// transferred reports whether the survivors took over by leadership
	// transfer (TimeoutNow) rather than by noticing the heartbeat stop.
	transferred bool
}

// roll restarts node i gracefully (SIGTERM, wait, start, wait ready) and
// measures what the survivors went through. It fails the test when the
// survivors needed more than one election or were leaderless for longer
// than one election timeout budget.
func (c *cluster) roll(i int) rollResult {
	c.t.Helper()
	leader, _ := c.leader()
	res := rollResult{wasLeader: leader == i}
	offsets := map[int]int64{}
	for j := range c.nodes {
		if j != i {
			offsets[j] = c.logOffset(j)
		}
	}
	res.stopTook = c.stop(i)
	if err := c.exitError(i); err != nil {
		c.t.Fatalf("%s exited with an error on SIGTERM: %v", c.nodes[i].id, err)
	}
	gapStart := time.Now()
	for {
		if l, ok := c.leader(); ok && l != i {
			break
		}
		if time.Since(gapStart) > 10*time.Second {
			c.t.Fatalf("survivors elected no leader within 10s of %s stopping", c.nodes[i].id)
		}
		time.Sleep(20 * time.Millisecond)
	}
	res.leaderGap = time.Since(gapStart)
	terms := map[string]bool{}
	heartbeatTimeouts := 0
	for j, off := range offsets {
		for _, term := range candidateTerms(c.logSince(j, off)) {
			terms[term] = true
		}
		heartbeatTimeouts += c.countLog(j, off, "heartbeat timeout reached")
	}
	res.elections = len(terms)
	res.transferred = res.wasLeader && res.elections > 0 && heartbeatTimeouts == 0
	c.start(i)
	res.readyTook = c.waitReady(i, 90*time.Second)
	c.t.Logf("rolled %s (leader=%v, transferred=%v): stop %s, leaderless %s, %d election(s) on survivors, ready %s",
		c.nodes[i].id, res.wasLeader, res.transferred, res.stopTook.Round(time.Millisecond), res.leaderGap.Round(time.Millisecond), res.elections, res.readyTook.Round(time.Millisecond))
	// One election: Raft's election timeout is 1s, randomised up to 2x.
	if res.leaderGap > 3*time.Second {
		c.t.Errorf("survivors were leaderless for %s after %s stopped; budget is one election (3s)", res.leaderGap, c.nodes[i].id)
	}
	if res.elections > 1 {
		c.t.Errorf("survivors held %d elections after %s stopped; budget is one", res.elections, c.nodes[i].id)
	}
	return res
}

// rollAll restarts every node one at a time, followers first so the
// leader's restart (a leadership transfer) is exercised last, and
// checks the views converge after each.
func (c *cluster) rollAll() {
	c.t.Helper()
	leader := c.waitLeader(20 * time.Second)
	order := []int{}
	for i := range c.nodes {
		if i != leader {
			order = append(order, i)
		}
	}
	order = append(order, leader)
	for _, i := range order {
		c.roll(i)
		c.waitConverged(c.waitLeader(20*time.Second), 60*time.Second)
	}
}

// ---- scenarios ------------------------------------------------------------------

// tlsCluster starts a 3-node cluster with Raft mTLS from ca and returns
// it with the certificate each node was issued.
func tlsCluster(t *testing.T, ca *certAuthority, trust []byte) (*cluster, [3]nodeCert) {
	t.Helper()
	c := newCluster(t, clusterOptions{})
	var certs [3]nodeCert
	for i := range c.nodes {
		certs[i] = ca.issue(t, c.nodes[i].id)
		c.installTLS(i, certs[i], trust)
	}
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(30 * time.Second)
	for i := range c.nodes {
		if c.countLog(i, 0, "raft metadata transport secured with mutual TLS") == 0 {
			t.Fatalf("%s did not start with Raft TLS", c.nodes[i].id)
		}
	}
	return c, certs
}

// TestTLSCertRenewalRollingRestart renews every node's certificate from
// the same CA by rolling restart under load. It first records the
// current behaviour: a certificate is read once at startup, so a renewed
// file on disk changes nothing until the node restarts.
func TestTLSCertRenewalRollingRestart(t *testing.T) {
	ca := newCA(t, "narad-test-ca")
	c, certs := tlsCluster(t, ca, ca.pem)
	probe := ca.issue(t, "probe")
	for i := range c.nodes {
		c.assertServes(i, probe.tls, poolOf(ca), certs[i].serial)
	}

	d := c.startDriver(driverOptions{topics: 3, partitions: 6, messages: 12000, rate: 150, timeout: 6 * time.Minute})
	c.waitTopics(d, 6, 60*time.Second)

	// Current behaviour: renewing the files under a running node does
	// not change what it serves.
	renewed := [3]nodeCert{}
	for i := range c.nodes {
		renewed[i] = ca.issue(t, c.nodes[i].id)
		c.installTLS(i, renewed[i], ca.pem)
	}
	time.Sleep(2 * time.Second)
	for i := range c.nodes {
		c.assertServes(i, probe.tls, poolOf(ca), certs[i].serial)
	}
	t.Log("renewed certificates written under all three running nodes; every node still serves the certificate it loaded at startup (no live reload)")

	// A restart picks the renewed certificate up.
	c.rollAll()
	for i := range c.nodes {
		c.assertServes(i, probe.tls, poolOf(ca), renewed[i].serial)
	}
	if d.finished() {
		t.Log("note: the driver finished before the last restart; the load did not span the whole roll")
	}
	for i := range c.nodes {
		c.assertServesOwnPartitions(i)
	}
	d.wait(t)
	c.waitConverged(c.waitLeader(20*time.Second), 60*time.Second)
}

// TestTLSCARotation replaces the cluster CA under load in the three
// rolls a real rotation needs: trust both CAs, move every node to a
// certificate from the new CA, drop the old CA. After each roll a peer
// with a certificate from the old CA and one from the new CA try to
// connect, and the answer must match what the nodes were told to trust.
func TestTLSCARotation(t *testing.T) {
	oldCA := newCA(t, "narad-ca-old")
	newCA := newCA(t, "narad-ca-new")
	c, certs := tlsCluster(t, oldCA, oldCA.pem)
	oldPeer := oldCA.issue(t, "peer-old")
	newPeer := newCA.issue(t, "peer-new")
	both := poolOf(oldCA, newCA)

	d := c.startDriver(driverOptions{topics: 3, partitions: 6, messages: 24000, rate: 150, timeout: 8 * time.Minute})
	c.waitTopics(d, 6, 60*time.Second)

	for i := range c.nodes {
		c.assertRejects(i, newPeer.tls, both)
	}

	t.Log("phase 1: trust old+new CA, roll")
	for i := range c.nodes {
		c.installTLS(i, certs[i], bundleOf(oldCA, newCA))
	}
	c.rollAll()
	for i := range c.nodes {
		c.assertServes(i, oldPeer.tls, both, certs[i].serial)
		c.assertServes(i, newPeer.tls, both, certs[i].serial)
	}

	t.Log("phase 2: certificates from the new CA, roll")
	fresh := [3]nodeCert{}
	for i := range c.nodes {
		fresh[i] = newCA.issue(t, c.nodes[i].id)
		c.installTLS(i, fresh[i], bundleOf(oldCA, newCA))
	}
	c.rollAll()
	for i := range c.nodes {
		c.assertServes(i, oldPeer.tls, both, fresh[i].serial)
		c.assertServes(i, newPeer.tls, both, fresh[i].serial)
	}

	t.Log("phase 3: drop the old CA, roll")
	for i := range c.nodes {
		c.installTLS(i, fresh[i], newCA.pem)
	}
	c.rollAll()
	for i := range c.nodes {
		c.assertServes(i, newPeer.tls, poolOf(newCA), fresh[i].serial)
		c.assertRejects(i, oldPeer.tls, both)
	}

	if d.finished() {
		t.Log("note: the driver finished before the last restart; the load did not span the whole rotation")
	}
	for i := range c.nodes {
		c.assertServesOwnPartitions(i)
	}
	d.wait(t)
	c.waitConverged(c.waitLeader(20*time.Second), 60*time.Second)
}

// TestTLSUntrustedCertFailsLoudly restarts a follower with a certificate
// from a CA the cluster does not trust. It must not join, its log and
// the survivors' logs must say why, and the other two must keep serving
// (the driver keeps passing, one leader, no election storm). Given a
// trusted certificate again it rejoins.
func TestTLSUntrustedCertFailsLoudly(t *testing.T) {
	ca := newCA(t, "narad-test-ca")
	rogueCA := newCA(t, "somebody-elses-ca")
	c, certs := tlsCluster(t, ca, ca.pem)

	d := c.startDriver(driverOptions{topics: 3, partitions: 6, messages: 12000, rate: 150, timeout: 6 * time.Minute})
	c.waitTopics(d, 6, 60*time.Second)

	leader := c.waitLeader(20 * time.Second)
	victim := c.anyRunning(leader)
	survivors := []int{}
	offsets := map[int]int64{}
	for i := range c.nodes {
		if i != victim {
			survivors = append(survivors, i)
			offsets[i] = c.logOffset(i)
		}
	}
	c.stop(victim)
	c.installTLS(victim, rogueCA.issue(t, c.nodes[victim].id), ca.pem)
	victimOff := c.logOffset(victim)
	c.start(victim)

	// The node must say why it cannot talk to its peers, and the peers
	// must say why they refuse it.
	c.waitLogAny(victim, victimOff, []string{"bad certificate", "unknown authority"}, 30*time.Second)
	peerSaid := false
	for _, s := range survivors {
		if c.countLog(s, offsets[s], "unknown authority") > 0 || c.countLog(s, offsets[s], "bad certificate") > 0 {
			peerSaid = true
		}
	}
	if !peerSaid {
		t.Errorf("no survivor logged why it refused %s", c.nodes[victim].id)
	}
	t.Logf("%s (untrusted certificate) log:\n%s", c.nodes[victim].id, grepLog(c.logSince(victim, victimOff), "tls", "x509", "certificate", 6))
	for _, s := range survivors {
		t.Logf("%s log:\n%s", c.nodes[s].id, grepLog(c.logSince(s, offsets[s]), "tls", "x509", "certificate", 4))
	}

	// It never becomes ready, and the survivors are unbothered: still
	// one leader, no elections, converged with each other.
	watchUntil := time.Now().Add(20 * time.Second)
	for time.Now().Before(watchUntil) {
		if ready, body := c.readyNow(victim); ready {
			t.Fatalf("%s reported ready with an untrusted certificate: %s", c.nodes[victim].id, body)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !c.isRunning(victim) {
		t.Fatalf("%s exited: %v", c.nodes[victim].id, c.exitError(victim))
	}
	_, body := c.readyNow(victim)
	t.Logf("%s /readyz after 20s: %s", c.nodes[victim].id, body)
	elections := 0
	for _, s := range survivors {
		elections += c.countLog(s, offsets[s], "entering candidate state")
	}
	if elections > 1 {
		t.Errorf("survivors held %d elections while %s was misconfigured; budget is one", elections, c.nodes[victim].id)
	}
	ref := c.waitLeader(20 * time.Second)
	c.waitConvergedExcept(ref, 60*time.Second, victim)
	for _, s := range survivors {
		c.assertServesOwnPartitions(s)
	}

	// Fixed: a trusted certificate again, and it rejoins.
	c.kill(victim)
	c.installTLS(victim, certs[victim], ca.pem)
	c.start(victim)
	c.waitReady(victim, 90*time.Second)
	c.waitConverged(c.waitLeader(20*time.Second), 60*time.Second)
	c.assertServesOwnPartitions(victim)
	d.wait(t)
}

// waitLogAny blocks until node i's log (from offset on) contains one of
// the substrings.
func (c *cluster) waitLogAny(i int, offset int64, substrs []string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		body := c.logSince(i, offset)
		for _, s := range substrs {
			if strings.Contains(body, s) {
				return
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s log did not contain any of %q within %s; tail:\n%s", c.nodes[i].id, substrs, timeout, tail(body, 3000))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// grepLog returns up to limit lines of body containing any of the
// (case-insensitive) needles.
func grepLog(body string, a, b, cNeedle string, limit int) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, a) || strings.Contains(l, b) || strings.Contains(l, cNeedle) {
			out = append(out, line)
			if len(out) == limit {
				break
			}
		}
	}
	return strings.Join(out, "\n")
}

var candidateTermRE = regexp.MustCompile(`entering candidate state:[^\n]*term=(\d+)`)

// candidateTerms lists the terms of every "entering candidate state"
// line in a log excerpt.
func candidateTerms(log string) []string {
	var out []string
	for _, m := range candidateTermRE.FindAllStringSubmatch(log, -1) {
		out = append(out, m[1])
	}
	return out
}
