package clusterrpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// Cluster-RPC authentication closes the "anyone who can reach the port
// speaks the protocol" hole. When a shared secret is configured, a
// client proves knowledge of it on every new stream by sending an auth
// frame first; the server verifies it before serving any request frame.
//
// The proof is HMAC-SHA256(secret, authContext), a fixed MAC rather
// than the raw secret, so the secret itself never crosses the wire. The
// QUIC transport is already TLS-encrypted (ALPN-pinned, self-signed),
// so a passive observer cannot capture the MAC and an active attacker
// would first have to break TLS; against an unauthenticated peer on the
// network, possession of a valid MAC is required. A future hardening
// step is mutual TLS with per-node certs (tracked separately).

// authContext domain-separates the cluster-auth MAC from any other use
// of the same secret.
var authContext = []byte("narad-cluster-auth-v1")

// statelessResetContext domain-separates the QUIC stateless reset key
// from the auth MAC derived from the same secret.
var statelessResetContext = []byte("narad-stateless-reset-v1")

// authToken returns the proof a client sends and a server expects.
func authToken(secret string) []byte {
	return deriveKey(secret, authContext)
}

// expectedAuthToken returns the token a server should require, or nil
// when no secret is configured (auth disabled). Computed once per
// listener so the per-stream verify is a constant-time compare, not a
// fresh HMAC.
func expectedAuthToken(secret string) []byte {
	if secret == "" {
		return nil
	}
	return authToken(secret)
}

// statelessResetKey derives the QUIC stateless reset key from the cluster
// secret. It MUST be deterministic across restarts of a node: a peer
// verifies a reset by comparing it with the token it learned from the
// pre-restart process, and that token is HMAC(key, connection ID). A
// random per-process key would make every reset unverifiable and leave
// peers holding dead connections until MaxIdleTimeout. With no secret
// configured the key is still deterministic (HMAC over the empty key),
// which is public knowledge; that only matters to an attacker already on
// the cluster network, where an unauthenticated plane is exposed anyway.
func statelessResetKey(secret string) *quic.StatelessResetKey {
	var key quic.StatelessResetKey
	copy(key[:], deriveKey(secret, statelessResetContext))
	return &key
}

func deriveKey(secret string, context []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(context)
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
	return verifyAuthToken(authToken(secret), frame)
}

// verifyAuthToken reports whether frame carries exactly the expected
// token, using a constant-time comparison.
func verifyAuthToken(expected []byte, frame clusterwire.StreamFrame) bool {
	if frame.Type != clusterwire.StreamFrameAuth {
		return false
	}
	return subtle.ConstantTimeCompare(frame.Payload, expected) == 1
}
