package clusterrpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// Cluster-RPC authentication closes the "anyone who can reach the port
// speaks the protocol" hole. When a shared secret is configured, every
// new stream starts with a mutual proof of the secret before any
// request frame is served.
//
// The proof is BOUND TO THE TLS SESSION. Both ends export 32 bytes of
// keying material from the TLS 1.3 session (RFC 8446 section 7.5, label
// ekmLabel) and MAC it with the cluster secret: HMAC-SHA256(secret,
// role || ekm). The keying material is unique per connection and known
// only to the two ends of that TLS session, so:
//
//   - a proof captured from one connection is useless on another
//     (nothing a rogue endpoint receives can be replayed to a real node);
//   - the SERVER proves the secret too, with the server role label, and
//     the client refuses to use a stream whose server cannot; a spoofed
//     peer address therefore yields the impostor nothing and the client
//     never trusts its replies;
//   - the secret itself never crosses the wire.
//
// The certificate is still ephemeral and self-signed (quic_tls.go): TLS
// provides confidentiality and a fresh session key, the shared secret
// provides authentication. Mutual TLS with per-node certificates remains
// the longer-term option.
//
// Compatibility: nodes that predate the session binding send a FIXED
// token, HMAC-SHA256(secret, authContext), and negotiate the legacy ALPN
// (quicALPNLegacy). That token is replayable and the server side of it
// is unauthenticated, so it is only honoured when the operator turns on
// SetLegacyAuthCompat for the duration of a rolling upgrade; see
// docs/operate/index.md for the upgrade order.

// ekmLabel is the TLS exporter label for the per-session auth key.
const ekmLabel = "narad-cluster-auth-v1"

// sessionAuthKeyBytes is the length of the exported keying material.
const sessionAuthKeyBytes = 32

// maxAuthFramePayloadBytes caps the payload of the auth handshake
// frames. A proof is a 32-byte MAC; anything larger is not a proof.
// Reading the pre-auth frame under this cap (rather than the 16 MiB
// general frame cap) means an unauthenticated peer cannot make the
// server allocate more than 64 bytes per stream before it proves
// anything.
const maxAuthFramePayloadBytes = 64

var (
	// authContext domain-separates the LEGACY fixed-token MAC from any
	// other use of the same secret.
	authContext = []byte("narad-cluster-auth-v1")

	// clientProofContext and serverProofContext domain-separate the two
	// directions of the session-bound proof so a server cannot answer a
	// client with the client's own proof (reflection).
	clientProofContext = []byte("narad-cluster-auth-v1/client")
	serverProofContext = []byte("narad-cluster-auth-v1/server")

	// statelessResetContext domain-separates the QUIC stateless reset key
	// from the auth MACs derived from the same secret.
	statelessResetContext = []byte("narad-stateless-reset-v1")
)

// legacyAuthCompat is the process-wide switch that lets this node talk
// to peers still running the fixed-token protocol. Off by default.
var legacyAuthCompat atomic.Bool

// SetLegacyAuthCompat enables (or disables) the one-release
// compatibility path for peers that predate session-bound cluster auth.
// While enabled, listeners also offer the legacy ALPN and accept the
// fixed token from peers that negotiate it (logging each such
// connection), and clients fall back to the fixed token when a peer
// only speaks the legacy ALPN. Call it before constructing any listener
// or client; it is read at construction time.
func SetLegacyAuthCompat(enabled bool) {
	legacyAuthCompat.Store(enabled)
}

// LegacyAuthCompat reports the current compatibility setting.
func LegacyAuthCompat() bool {
	return legacyAuthCompat.Load()
}

// sessionBinding is what the stream auth needs to know about the TLS
// session a stream rides on: whether the peer negotiated the legacy
// fixed-token protocol, and otherwise the exported keying material the
// proofs are bound to.
type sessionBinding struct {
	legacy bool
	key    []byte
}

var errNoTLSSession = errors.New("cluster rpc: connection has no TLS session to bind the auth proof to")

// sessionBindingFrom derives the binding from a negotiated TLS
// connection state. The legacy ALPN is only accepted while
// allowLegacy is set.
func sessionBindingFrom(cs tls.ConnectionState, allowLegacy bool) (sessionBinding, error) {
	switch cs.NegotiatedProtocol {
	case quicALPN:
		key, err := exportKeyingMaterial(cs)
		if err != nil {
			return sessionBinding{}, fmt.Errorf("cluster rpc: export keying material: %w", err)
		}
		if len(key) != sessionAuthKeyBytes {
			return sessionBinding{}, fmt.Errorf("cluster rpc: exported %d bytes of keying material, want %d", len(key), sessionAuthKeyBytes)
		}
		return sessionBinding{key: key}, nil
	case quicALPNLegacy:
		if !allowLegacy {
			return sessionBinding{}, fmt.Errorf("cluster rpc: peer negotiated legacy ALPN %q but legacy auth compatibility is off", cs.NegotiatedProtocol)
		}
		return sessionBinding{legacy: true}, nil
	case "":
		return sessionBinding{}, errNoTLSSession
	default:
		return sessionBinding{}, fmt.Errorf("cluster rpc: unexpected ALPN %q", cs.NegotiatedProtocol)
	}
}

// exportKeyingMaterial wraps the TLS exporter so a connection state
// with no exporter (never the case for a completed TLS 1.3 handshake,
// but crypto/tls panics rather than erroring on it) fails closed.
func exportKeyingMaterial(cs tls.ConnectionState) (key []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			key, err = nil, fmt.Errorf("no exporter on this connection state: %v", r)
		}
	}()
	return cs.ExportKeyingMaterial(ekmLabel, nil, sessionAuthKeyBytes)
}

// sessionProof is the session-bound proof for one direction:
// HMAC-SHA256(secret, role || key).
func sessionProof(secret string, role, key []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(role)
	mac.Write(key)
	return mac.Sum(nil)
}

// clientProof and serverProof are the two directions of the handshake.
func clientProof(secret string, key []byte) []byte {
	return sessionProof(secret, clientProofContext, key)
}
func serverProof(secret string, key []byte) []byte {
	return sessionProof(secret, serverProofContext, key)
}

// legacyAuthToken is the fixed proof exchanged with peers on the legacy
// ALPN: HMAC-SHA256(secret, authContext), identical on every stream.
func legacyAuthToken(secret string) []byte {
	return deriveKey(secret, authContext)
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

// authFrame wraps a proof in an auth frame.
func authFrame(proof []byte) clusterwire.StreamFrame {
	return clusterwire.StreamFrame{
		Type:    clusterwire.StreamFrameAuth,
		Payload: proof,
	}
}

// verifyAuthToken reports whether frame carries exactly the expected
// proof, using a constant-time comparison.
func verifyAuthToken(expected []byte, frame clusterwire.StreamFrame) bool {
	if frame.Type != clusterwire.StreamFrameAuth || len(expected) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare(frame.Payload, expected) == 1
}

// connAuth is the per-connection server-side auth state: the proofs
// the server expects and sends on every stream of one TLS session,
// derived once per connection rather than once per stream. nil means
// auth is disabled (no secret configured).
type connAuth struct {
	legacy         bool
	expectedClient []byte
	serverReply    []byte // nil in legacy mode: old clients do not read a reply
}

// newConnAuth builds the server's per-connection auth state, or nil
// when no secret is configured.
func newConnAuth(secret string, binding sessionBinding) *connAuth {
	if secret == "" {
		return nil
	}
	if binding.legacy {
		return &connAuth{legacy: true, expectedClient: legacyAuthToken(secret)}
	}
	return &connAuth{
		expectedClient: clientProof(secret, binding.key),
		serverReply:    serverProof(secret, binding.key),
	}
}
