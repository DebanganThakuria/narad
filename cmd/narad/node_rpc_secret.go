package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// nodeRPCSecretBytes is the entropy of a generated node RPC secret.
const nodeRPCSecretBytes = 32

// secureNodeRPC makes sure the node RPC plane (QUIC on the API port over
// UDP) is authenticated whenever the API is. That plane serves the whole
// control plane with authorization bypassed by design (forwarded
// requests carry no identity; the ingress node already decided), so an
// empty secret there hands every operation, user creation included, to
// anything that can send the port a datagram.
//
// Validation already requires a secret for a secured multi-node cluster.
// A secured node with no peers and no secret gets a random one for the
// life of the process, which no other process knows, so its node RPC
// answers only itself. The secret lives in memory only and is never
// logged or written. A node started with security disabled keeps the
// open plane it opted into, and says so: closing it would cut the node
// RPC of a security-off cluster grown by joining nodes to a node started
// alone (that node has no peers configured), and security off already
// leaves the HTTP API open.
//
// It must run before the first peer client or cluster RPC listener is
// built: both read cfg.Security.ClusterSecret.
func secureNodeRPC(cfg *config.Config, log *slog.Logger) error {
	switch {
	case strings.TrimSpace(cfg.Security.ClusterSecret) != "":
		return nil
	case !cfg.Security.Enabled:
		log.Warn("node RPC plane is unauthenticated: security is disabled and no cluster secret is set, so anything that can send UDP to the API port can create users and topics and produce, consume and ack without credentials",
			"component", "audit", "addr", cfg.HTTP.Addr)
		return nil
	case len(cfg.Cluster.Peers) > 0:
		// Refused by validation. Never generate a secret peers could
		// not share.
		return nil
	}
	secret, err := randomNodeRPCSecret()
	if err != nil {
		return fmt.Errorf("generate node rpc secret: %w", err)
	}
	cfg.Security.ClusterSecret = secret
	cfg.Security.ClusterSecretGenerated = true
	log.Info("single node with no cluster secret: generated a per-process secret, so node RPC is closed to other processes; set NARAD_CLUSTER_SECRET on every node before adding peers, and note that a node whose raft first started on a loopback cluster.addr can never take peers",
		"component", "audit")
	return nil
}

// randomNodeRPCSecret returns nodeRPCSecretBytes of crypto/rand, hex
// encoded. A read error is returned, never papered over with a weaker
// source.
func randomNodeRPCSecret() (string, error) {
	b := make([]byte, nodeRPCSecretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
