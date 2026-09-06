package clusterrpc

import (
	"bytes"
	"crypto/tls"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// testSessionKey stands in for the TLS exported keying material on
// transports that have no TLS session (pipes).
var testSessionKey = bytes.Repeat([]byte{0x5a}, sessionAuthKeyBytes)

// testConnAuth is the server-side auth state for a pipe-served stream
// bound to testSessionKey.
func testConnAuth(secret string) *connAuth {
	return newConnAuth(secret, sessionBinding{key: testSessionKey})
}

func TestSessionProofsAreBoundToSecretRoleAndSession(t *testing.T) {
	const secret = "s3cr3t"
	other := bytes.Repeat([]byte{0xa5}, sessionAuthKeyBytes)

	if !bytes.Equal(clientProof(secret, testSessionKey), clientProof(secret, testSessionKey)) {
		t.Fatal("client proof is not deterministic")
	}
	if bytes.Equal(clientProof(secret, testSessionKey), clientProof("wrong", testSessionKey)) {
		t.Fatal("different secrets produced the same proof")
	}
	// The whole point: the same secret on a different TLS session yields a
	// different proof, so a captured proof cannot be replayed elsewhere.
	if bytes.Equal(clientProof(secret, testSessionKey), clientProof(secret, other)) {
		t.Fatal("proof does not depend on the session key (replayable across sessions)")
	}
	// And a server cannot reflect the client's proof back as its own.
	if bytes.Equal(clientProof(secret, testSessionKey), serverProof(secret, testSessionKey)) {
		t.Fatal("client and server proofs coincide (reflection possible)")
	}
	if bytes.Equal(clientProof(secret, testSessionKey), legacyAuthToken(secret)) {
		t.Fatal("session proof collides with the legacy fixed token")
	}
	if strings.Contains(string(clientProof("supersecret", testSessionKey)), "supersecret") {
		t.Fatal("proof leaked the raw secret")
	}
	if len(clientProof(secret, testSessionKey)) > maxAuthFramePayloadBytes {
		t.Fatalf("proof is %d bytes, larger than the %d-byte auth frame cap", len(clientProof(secret, testSessionKey)), maxAuthFramePayloadBytes)
	}
}

func TestVerifyAuthToken(t *testing.T) {
	const secret = "s3cr3t"
	expected := clientProof(secret, testSessionKey)

	if !verifyAuthToken(expected, authFrame(clientProof(secret, testSessionKey))) {
		t.Fatal("valid proof rejected")
	}
	if verifyAuthToken(expected, authFrame(clientProof("wrong", testSessionKey))) {
		t.Fatal("proof from wrong secret accepted")
	}
	if verifyAuthToken(expected, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, Payload: expected}) {
		t.Fatal("non-auth frame type accepted")
	}
	if verifyAuthToken(expected, clusterwire.StreamFrame{Type: clusterwire.StreamFrameAuth}) {
		t.Fatal("empty proof accepted")
	}
	if verifyAuthToken(nil, clusterwire.StreamFrame{Type: clusterwire.StreamFrameAuth}) {
		t.Fatal("empty expected proof matched an empty payload")
	}
}

func TestSessionBindingFrom(t *testing.T) {
	// A negotiated v2 session without exporter material is rejected: the
	// zero ConnectionState has no ekm, and the binding must fail closed
	// rather than bind to nothing.
	if _, err := sessionBindingFrom(tls.ConnectionState{NegotiatedProtocol: quicALPN}, false); err == nil {
		t.Fatal("binding without keying material accepted")
	}
	if _, err := sessionBindingFrom(tls.ConnectionState{}, false); err == nil {
		t.Fatal("binding without a TLS session accepted")
	}
	if _, err := sessionBindingFrom(tls.ConnectionState{NegotiatedProtocol: quicALPNLegacy}, false); err == nil {
		t.Fatal("legacy ALPN accepted with compatibility off")
	}
	b, err := sessionBindingFrom(tls.ConnectionState{NegotiatedProtocol: quicALPNLegacy}, true)
	if err != nil || !b.legacy {
		t.Fatalf("legacy ALPN with compatibility on: binding=%+v err=%v", b, err)
	}
	if _, err := sessionBindingFrom(tls.ConnectionState{NegotiatedProtocol: "h3"}, true); err == nil {
		t.Fatal("foreign ALPN accepted")
	}
}

func TestNewConnAuth(t *testing.T) {
	if newConnAuth("", sessionBinding{key: testSessionKey}) != nil {
		t.Fatal("empty secret must disable auth")
	}
	a := newConnAuth("s", sessionBinding{key: testSessionKey})
	if a.legacy || a.serverReply == nil || !bytes.Equal(a.expectedClient, clientProof("s", testSessionKey)) {
		t.Fatalf("session auth state = %+v", a)
	}
	l := newConnAuth("s", sessionBinding{legacy: true})
	if !l.legacy || l.serverReply != nil || !bytes.Equal(l.expectedClient, legacyAuthToken("s")) {
		t.Fatalf("legacy auth state = %+v", l)
	}
}

func TestAlpnListOrderPrefersCurrentProtocol(t *testing.T) {
	if got := alpnList(false); len(got) != 1 || got[0] != quicALPN {
		t.Fatalf("alpnList(false) = %v", got)
	}
	if got := alpnList(true); len(got) != 2 || got[0] != quicALPN || got[1] != quicALPNLegacy {
		t.Fatalf("alpnList(true) = %v, want current first so two upgraded nodes never pick legacy", got)
	}
}
