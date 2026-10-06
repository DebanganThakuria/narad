package main

// Remote replication at startup: the node's posture, the startup rules
// of a node whose metastore holds remotes, and the outbound plane (the
// address guard, the credential cache and the remote service).

import (
	"fmt"
	"log/slog"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// remotesStack is the node's outbound plane.
type remotesStack struct {
	guard   *remote.Guard
	cache   *remote.Cache
	service *remote.Service
}

// nodePosture is this node's own security posture, as it reports it to
// the upgrade gate. Raft counts as TLS on a single node, which has no
// Raft peers to talk plaintext to.
func nodePosture(cfg *config.Config) remote.Posture {
	return remote.Posture{
		SecurityEnabled:   cfg.Security.Enabled,
		LegacyClusterAuth: cfg.Security.AllowLegacyClusterAuth,
		RaftTLS:           cfg.Security.ClusterTLSConfigured() || len(cfg.Cluster.Peers) == 0,
		APIHopEncrypted:   cfg.Remotes.APIHopEncrypted,
	}
}

// remotesSecret is the cluster secret remote passwords are sealed
// under. A secret secureNodeRPC generated for this process is not one:
// it is gone after a restart, so every password sealed under it would
// become unreadable with no previous secret to open it. The remotes
// plane sees no secret instead, so a node holding remotes refuses to
// start and every seal answers 412 secret_missing, naming the fix (set
// NARAD_CLUSTER_SECRET).
func remotesSecret(cfg *config.Config) string {
	if cfg.Security.ClusterSecretGenerated {
		return ""
	}
	return cfg.Security.ClusterSecret
}

// buildRemotes applies the startup rules and builds the outbound plane.
// A node whose metastore holds any remote refuses to start with
// security off or without a cluster secret; plaintext Raft (Q23) and a
// weak secret are logged and do not stop it (a weak secret only makes
// every seal answer 412). A remotes.allowed_hosts entry that does not
// canonicalize is a startup error.
func buildRemotes(cfg *config.Config, ms *metastore.Store, nodeID string, m *metrics.Metrics, log *slog.Logger) (*remotesStack, error) {
	remote.SetClientVersion(versionString())
	posture := nodePosture(cfg)
	records, err := ms.ListRemotes()
	if err != nil {
		return nil, fmt.Errorf("remotes: read registry: %w", err)
	}
	holds := len(records) > 0
	secret := remotesSecret(cfg)
	rep, err := remote.StartupCheck(remote.StartupInputs{
		HoldsRemotes:  holds,
		Posture:       posture,
		ClusterSecret: secret,
		SecretCheck:   remotecred.CheckSecretStrength,
	})
	if err != nil {
		return nil, fmt.Errorf("remotes: %w", err)
	}
	var rm *metrics.RemoteMetrics
	if m != nil {
		rm = m.Remote
	}
	guard, err := remote.NewGuard(remote.GuardConfig{
		AllowedHosts:   cfg.Remotes.AllowedHosts,
		AllowedPorts:   cfg.Remotes.AllowedPorts,
		AllowAddresses: cfg.Remotes.AllowAddresses,
		Metrics:        rm,
	})
	if err != nil {
		return nil, err
	}
	plaintextWarned := false
	warnPlaintextRaft := func() {
		if posture.RaftTLS || plaintextWarned {
			return
		}
		plaintextWarned = true
		log.Warn("this node holds remotes and its Raft transport runs without TLS: a Raft-port attacker can delete remotes and stall links (not read or redirect a password); set the security.cluster_tls_* files",
			"component", "audit")
		if rm != nil {
			rm.PlaintextRaft.Set(1)
		}
	}
	if rep.PlaintextRaft {
		warnPlaintextRaft()
	}
	if rep.WeakSecret {
		log.Error("this node holds remotes and its cluster secret fails the strength rule: every seal (create, password change, re-encrypt) answers 412 until it is replaced; "+
			"stored passwords stay readable only if they were sealed under this secret", "component", "audit")
	}
	if rm != nil {
		if guard.AllowlistConfigured() {
			rm.AllowlistConfigured.Set(1)
		} else {
			rm.AllowlistConfigured.Set(0)
		}
	}
	if holds && !guard.AllowlistConfigured() {
		log.Warn("this node holds remotes and remotes.allowed_hosts is empty: remotes may point at any host the address guard allows; set it in production",
			"component", "audit")
	}
	if len(cfg.Remotes.AllowAddresses) > 0 {
		log.Warn("remotes.allow_addresses lets remotes reach addresses the address guard refuses by default",
			"component", "audit", "allow_addresses", cfg.Remotes.AllowAddresses)
	}
	if cfg.Remotes.APIHopEncrypted {
		log.Info("remotes.api_hop_encrypted attests that the ingress-to-pod hop is encrypted; remote writes are accepted on this node",
			"component", "audit")
	}
	secrets := remote.Secrets{Current: secret, Previous: cfg.Security.ClusterSecretPrevious}
	cache := remote.NewCache(remote.CacheConfig{
		Registry: ms,
		Secrets:  secrets,
		Guard:    guard,
		Posture:  posture,
		Metrics:  rm,
		Log:      log,
		OnRemotesAppear: func() {
			warnPlaintextRaft()
		},
	})
	service := remote.NewService(remote.ServiceConfig{
		Store:   ms,
		Cache:   cache,
		Guard:   guard,
		Secrets: secrets,
		Posture: posture,
		NodeID:  nodeID,
		Metrics: rm,
		Log:     log,
	})
	return &remotesStack{guard: guard, cache: cache, service: service}, nil
}
