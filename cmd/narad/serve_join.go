package main

// Cluster scale-out: a node whose ID is not in cluster.initial_members
// must never bootstrap a Raft cluster from its peer list — it would
// create a phantom cluster the real one knows nothing about, sit
// leaderless forever, and (past the readiness timeout) serve an empty
// metastore behind the load balancer. Such a node starts join-only and
// runs this loop instead: ask each configured peer to admit it until
// the leader answers, then let normal Raft replication take over.

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const clusterJoinRetryInterval = 2 * time.Second

// joinOnlyNode reports whether this node must join an existing cluster
// rather than bootstrap one. An empty initial-members list preserves the
// original behavior: every node may bootstrap (single-node and static
// deployments where all peers start together).
func joinOnlyNode(nodeID string, initialMembers []string) bool {
	if len(initialMembers) == 0 {
		return false
	}
	for _, m := range initialMembers {
		if m == nodeID {
			return false
		}
	}
	return true
}

// clusterJoiner is the slice of PeerClient the join loop needs.
type clusterJoiner interface {
	JoinCluster(ctx context.Context, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error)
}

// runClusterJoin asks the configured peers for admission until this
// node's Raft learns a leader (proof the leader has admitted and
// contacted it), or ctx is cancelled. Safe to run on a node that is
// already a member — the loop exits on the first leader sighting without
// sending anything if Raft already knows one.
func runClusterJoin(ctx context.Context, store *metastore.Store, peer clusterJoiner, cfg *config.Config, nodeID string, log *slog.Logger) {
	req := nodewire.JoinClusterRequest{
		ID:          nodeID,
		ClusterAddr: advertisedClusterAddr(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers),
	}
	ticker := time.NewTicker(clusterJoinRetryInterval)
	defer ticker.Stop()
	attempts := 0
	for {
		if store.LeaderID() != "" {
			log.Info("cluster join: admitted", "node", nodeID, "leader", store.LeaderID(), "attempts", attempts)
			return
		}
		for _, p := range cfg.Cluster.Peers {
			if p.ID == nodeID {
				continue
			}
			addr := peerMemberAddr(p.Addr, cfg.HTTP.Addr)
			if addr == "" {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			res, err := peer.JoinCluster(rpcCtx, addr, req)
			cancel()
			switch {
			case err != nil:
				log.Debug("cluster join attempt failed", "peer", p.ID, "err", err)
				continue
			case res.Status == http.StatusOK:
				log.Info("cluster join: leader accepted", "node", nodeID, "via", p.ID)
			case res.Status == http.StatusMisdirectedRequest:
				continue // not the leader; try the next peer
			default:
				log.Warn("cluster join rejected", "peer", p.ID, "status", res.Status)
			}
			break
		}
		attempts++
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// waitForClusterLeader blocks until this node's Raft knows a leader —
// for a join-only node, proof that admission completed — or ctx is
// cancelled.
func waitForClusterLeader(ctx context.Context, store *metastore.Store) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for store.LeaderID() == "" {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// peerMemberAddr derives a peer's node-RPC (HTTP/QUIC) address from its
// advertised cluster address: same host, this deployment's HTTP port.
// Every node in a deployment shares the HTTP port (the peer list itself
// is shared verbatim), so the swap is sound.
func peerMemberAddr(peerClusterAddr, httpAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(peerClusterAddr))
	if err != nil || host == "" {
		return ""
	}
	_, port, err := net.SplitHostPort(strings.TrimSpace(httpAddr))
	if err != nil {
		port = strings.TrimPrefix(strings.TrimSpace(httpAddr), ":")
	}
	if port == "" {
		return ""
	}
	return net.JoinHostPort(host, port)
}
