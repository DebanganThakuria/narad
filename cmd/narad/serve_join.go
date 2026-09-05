package main

// Cluster admission. A node whose ID is not in cluster.initial_members
// must never bootstrap a Raft cluster from its peer list — it would
// create a phantom cluster the real one knows nothing about, sit
// leaderless forever, and serve an empty metastore behind the load
// balancer. Such a node starts join-only and runs the join loop: ask
// each configured peer to admit it until the leader answers, then let
// normal Raft replication take over.
//
// Two more situations end in the same loop, because "I am an initial
// member" is not proof of membership:
//
//   - An initial member that was decommissioned and Raft-removed, then
//     restarted with its old volume, has state and is not join-only, so
//     it used to neither bootstrap nor join: leaderless forever. Now any
//     node with no leader after a bounded wait runs the join loop; the
//     leader decides (it refuses the old incarnation of a removed ID).
//   - An initial member restored with an EMPTY volume while the cluster
//     exists used to bootstrap a rival configuration from its peer list.
//     Now it asks the peers first: if any of them answers for a
//     configured cluster it starts join-only instead.

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const clusterJoinRetryInterval = 2 * time.Second

// leaderlessJoinDelay is how long a node with prior Raft state may go
// without a leader before it runs the join loop. It must comfortably
// exceed an election (hashicorp/raft defaults: ~1-2s) and the stagger of
// a rolling restart, since a healthy node that merely waits on an
// election must not issue joins. A variable so tests can shorten it.
var leaderlessJoinDelay = 15 * time.Second

// Existing-cluster probe: how many rounds an initial member with no
// state asks its peers before concluding there is nothing to join and
// bootstrapping. Peers that are down or not yet listening answer
// nothing, so a fresh install pays at most a few seconds here.
const (
	existingClusterProbeRounds   = 3
	existingClusterProbeInterval = time.Second
	existingClusterProbeTimeout  = 2 * time.Second
)

// joinOnlyNode reports whether this node must join an existing cluster
// rather than bootstrap one. An empty initial-members list preserves the
// original behavior: every node may bootstrap (single-node and static
// deployments where all peers start together).
func joinOnlyNode(nodeID string, initialMembers []string) bool {
	if len(initialMembers) == 0 {
		return false
	}
	return !slices.Contains(initialMembers, nodeID)
}

// clusterJoiner is the slice of PeerClient the join loop needs.
type clusterJoiner interface {
	JoinCluster(ctx context.Context, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error)
}

// leaderWatcher is the slice of the metastore the join watchdog needs;
// *metastore.Store implements it.
type leaderWatcher interface {
	LeaderID() string
}

// runClusterJoinWhenLeaderless runs the join loop whenever this node
// needs admission, for the life of the process. A join-only node runs it
// immediately. Every node (join-only or not) runs it again whenever it
// has had no leader for leaderlessJoinDelay: that is how a node that was
// removed from the voter set, or whose peers were all replaced, gets a
// second chance instead of sitting leaderless while /readyz stays down.
// The loop is idle while a leader is in view and sends nothing on a
// healthy restart, and AddVoter is idempotent, so a spurious join during
// a slow election is harmless. fresh declares that this node started
// from an empty data directory (see JoinClusterRequest.Fresh); it is
// cleared after the first admission.
func runClusterJoinWhenLeaderless(ctx context.Context, store leaderWatcher, peer clusterJoiner, cfg *config.Config, nodeID string, joinOnly, fresh bool, log *slog.Logger) {
	if len(cfg.Cluster.Peers) == 0 {
		return // single-node: nothing to join
	}
	needJoin := joinOnly
	for ctx.Err() == nil {
		if !needJoin {
			if !waitLeaderlessFor(ctx, store, leaderlessJoinDelay) {
				return
			}
			log.Warn("no raft leader for a while; running the cluster join loop", "node", nodeID, "leaderless_for", leaderlessJoinDelay)
		}
		runClusterJoin(ctx, store, peer, cfg, nodeID, fresh, log)
		needJoin = false
		fresh = false
	}
}

// waitLeaderlessFor blocks until this node has been continuously without
// a leader for d (returning true), or ctx is cancelled (false).
func waitLeaderlessFor(ctx context.Context, store leaderWatcher, d time.Duration) bool {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	leaderlessSince := time.Time{}
	for {
		if store.LeaderID() != "" {
			leaderlessSince = time.Time{}
		} else if leaderlessSince.IsZero() {
			leaderlessSince = time.Now()
		} else if time.Since(leaderlessSince) >= d {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// runClusterJoin asks the configured peers for admission until this
// node's Raft learns a leader (proof the leader has admitted and
// contacted it), or ctx is cancelled. Safe to run on a node that is
// already a member — the loop exits on the first leader sighting without
// sending anything if Raft already knows one.
func runClusterJoin(ctx context.Context, store leaderWatcher, peer clusterJoiner, cfg *config.Config, nodeID string, fresh bool, log *slog.Logger) {
	req := nodewire.JoinClusterRequest{
		ID:          nodeID,
		ClusterAddr: clusterAdvertiseAddr(cfg, nodeID),
		Fresh:       fresh,
	}
	ticker := time.NewTicker(clusterJoinRetryInterval)
	defer ticker.Stop()
	attempts := 0
	lastRejection := 0
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
			case res.Status == http.StatusMisdirectedRequest, res.Status == http.StatusPreconditionFailed:
				continue // not the leader, or no configuration yet; try the next peer
			case res.Status == http.StatusConflict:
				// The leader refuses the old incarnation of a removed ID.
				// Say so once, loudly, then keep retrying quietly: the
				// operator either scales this pod away or wipes its volume.
				if lastRejection != res.Status {
					log.Warn("cluster join refused: this node was decommissioned and removed; it will not rejoin with its old data directory. Scale it away, or delete its volume to rejoin as a new node",
						"node", nodeID, "peer", p.ID, "body", strings.TrimSpace(string(res.Body)))
				}
			default:
				log.Warn("cluster join rejected", "peer", p.ID, "status", res.Status)
			}
			lastRejection = res.Status
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

// existingClusterAnswers reports whether any configured peer answers the
// join request as a member of a CONFIGURED cluster: accepted (200), not
// the leader (421), or refused as a removed ID (409). A peer with no Raft
// configuration (412), one that is down, or one not yet listening is not
// evidence. An initial member with no local state asks this before
// bootstrapping: "yes" means the cluster already exists and this node
// must join it, whatever its old role was. The request carries
// Fresh=true, which is what makes a readmission of a removed ID legal.
func existingClusterAnswers(ctx context.Context, peer clusterJoiner, cfg *config.Config, nodeID string, log *slog.Logger) bool {
	req := nodewire.JoinClusterRequest{
		ID:          nodeID,
		ClusterAddr: clusterAdvertiseAddr(cfg, nodeID),
		Fresh:       true,
	}
	for round := 0; round < existingClusterProbeRounds; round++ {
		for _, p := range cfg.Cluster.Peers {
			if p.ID == nodeID {
				continue
			}
			addr := peerMemberAddr(p.Addr, cfg.HTTP.Addr)
			if addr == "" {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, existingClusterProbeTimeout)
			res, err := peer.JoinCluster(rpcCtx, addr, req)
			cancel()
			if err != nil {
				log.Debug("existing-cluster probe: peer did not answer", "peer", p.ID, "err", err)
				continue
			}
			switch res.Status {
			case http.StatusOK, http.StatusMisdirectedRequest, http.StatusConflict:
				log.Info("existing-cluster probe: a configured cluster answered", "peer", p.ID, "status", res.Status)
				return true
			default:
				log.Debug("existing-cluster probe: peer has no cluster to offer", "peer", p.ID, "status", res.Status)
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(existingClusterProbeInterval):
		}
	}
	return false
}

// clusterAdvertiseAddr is the Raft address this node tells peers to
// dial: cluster.advertise_addr when set, else the host borrowed from
// its own entry in the shared peer list (advertisedClusterAddr).
func clusterAdvertiseAddr(cfg *config.Config, nodeID string) string {
	if addr := strings.TrimSpace(cfg.Cluster.AdvertiseAddr); addr != "" {
		return addr
	}
	return advertisedClusterAddr(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers)
}

// bootstrapPeers is the voter set a brand-new cluster is seeded with:
// the configured peers restricted to cluster.initial_members when that
// list is set, minus this node (it is the local voter, not a remote
// peer). The full peer list still serves join-only nodes as the set of
// addresses to walk when looking for the leader; only the BOOTSTRAP
// configuration is limited, so a fresh seven-replica install with three
// initial members needs two of three votes, not four of seven.
func bootstrapPeers(nodeID, clusterAddr string, peers []config.ClusterPeer, initialMembers []string) []metastore.Peer {
	if len(initialMembers) == 0 {
		return configPeersToMetastore(nodeID, clusterAddr, peers)
	}
	var out []metastore.Peer
	for _, peer := range peers {
		if peer.ID == nodeID && netaddr.ClusterAddrMatchesPeer(clusterAddr, peer.Addr) {
			continue
		}
		if !slices.Contains(initialMembers, peer.ID) {
			continue
		}
		out = append(out, metastore.Peer{ID: peer.ID, Addr: peer.Addr})
	}
	return out
}

// waitForClusterLeader blocks until this node's Raft knows a leader —
// for a join-only node, proof that admission completed — or ctx is
// cancelled.
func waitForClusterLeader(ctx context.Context, store leaderWatcher) {
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
