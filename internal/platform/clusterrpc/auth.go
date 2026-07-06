package clusterrpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// Cluster-RPC authentication closes the "anyone who can reach the port
// speaks the protocol" hole. When a shared secret is configured, a
// client proves knowledge of it on every new stream by sending an auth
// frame first; the server verifies it before serving any request frame.
//
// The proof is HMAC-SHA256(secret, authContext) — a fixed MAC rather
// than the raw secret, so the secret itself never crosses the wire. The
// QUIC transport is already TLS-encrypted (ALPN-pinned, self-signed),
// so a passive observer cannot capture the MAC and an active attacker
// would first have to break TLS; against an unauthenticated peer on the
// network, possession of a valid MAC is required. A future hardening
// step is mutual TLS with per-node certs (tracked separately).

// authContext domain-separates the cluster-auth MAC from any other use
// of the same secret.
var authContext = []byte("narad-cluster-auth-v1")

// authToken returns the proof a client sends and a server expects.
func authToken(secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(authContext)
	return mac.Sum(nil)
}

// authFrame builds the auth frame carrying the token.
func authFrame(secret string) clusterwire.StreamFrame {
	return clusterwire.StreamFrame{
		Type:    clusterwire.StreamFrameAuth,
		Payload: authToken(secret),
	}
}

// verifyAuthFrame reports whether frame is a valid auth proof for
// secret, using a constant-time comparison.
func verifyAuthFrame(secret string, frame clusterwire.StreamFrame) bool {
	if frame.Type != clusterwire.StreamFrameAuth {
		return false
	}
	return subtle.ConstantTimeCompare(frame.Payload, authToken(secret)) == 1
}
