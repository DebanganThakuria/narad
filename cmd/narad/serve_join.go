package main

// Cluster admission. A node whose ID is not in cluster.initial_members
// must never bootstrap a Raft cluster from its peer list — it would
// create a phantom cluster the real one knows nothing about, sit
// leaderless forever, and serve an empty metastore behind the load
// balancer. Such a node starts join-only and runs the join loop: ask
// each configured peer to admit it until the leader answers (following a
// non-leader's 421 to the leader it names), then let normal Raft
// replication take over. The leader admits it as a Raft non-voter; once
// its replica has caught up it asks again, and the leader promotes it to
// voter (awaitVoter).
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
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/cluster"
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
// healthy restart, and a join from a node already in the configuration
// changes nothing but its address, so a spurious join during a slow
// election is harmless. After every admission, and once at start, a node
// that is a staged non-voter asks for promotion until it is a voter
// (awaitVoter): that also promotes a node that joined while it ran 3.0.x
// once it runs this release. fresh declares that this node started from
// an empty data directory (see JoinClusterRequest.Fresh); it is cleared
// after the first admission.
func runClusterJoinWhenLeaderless(ctx context.Context, store leaderWatcher, peer clusterJoiner, cfg *config.Config, nodeID string, joinOnly, fresh bool, log *slog.Logger) {
	if len(cfg.Cluster.Peers) == 0 {
		return // single-node: nothing to join
	}
	needJoin := joinOnly
	if !needJoin {
		needJoin = awaitVoter(ctx, store, peer, cfg, nodeID, log)
	}
	for ctx.Err() == nil {
		if !needJoin {
			if !waitLeaderlessFor(ctx, store, leaderlessJoinDelay) {
				return
			}
			log.Warn("no raft leader for a while; running the cluster join loop", "node", nodeID, "leaderless_for", leaderlessJoinDelay)
		}
		runClusterJoin(ctx, store, peer, cfg, nodeID, fresh, log)
		fresh = false
		if ctx.Err() != nil {
			return
		}
		needJoin = awaitVoter(ctx, store, peer, cfg, nodeID, log)
	}
}

// stagedVoter is the slice of the metastore a staged non-voter needs to
// ask for promotion; *metastore.Store implements it. A store without it
// (a test fake) never asks.
type stagedVoter interface {
	LocalVoter() (bool, error)
	AppliedCaughtUp() bool
}

// awaitVoter runs while this node is in the Raft configuration but not
// a voter: the leader admits a joiner as a non-voter (metastore
// AdmitJoiner). With a leader in view and its replica caught up, it
// sends its join again every clusterJoinRetryInterval, which the leader
// answers by promoting it or with the reason it defers, logged here at
// info whenever it changes. It returns false once this node is a voter
// (at once when it already is) or ctx ends, and true when the node has
// seen no leader for leaderlessJoinDelay: the join loop must run again.
func awaitVoter(ctx context.Context, store leaderWatcher, peer clusterJoiner, cfg *config.Config, nodeID string, log *slog.Logger) bool {
	sv, ok := store.(stagedVoter)
	if !ok {
		return false
	}
	req := nodewire.JoinClusterRequest{ID: nodeID, ClusterAddr: clusterAdvertiseAddr(cfg, nodeID), EntryTypes: metastore.MaxEntryType}
	walker := &joinWalker{peer: peer, cfg: cfg, nodeID: nodeID, log: log}
	ticker := time.NewTicker(clusterJoinRetryInterval)
	defer ticker.Stop()
	var leaderlessSince time.Time
	asked, lastReason := false, ""
	for {
		voter, err := sv.LocalVoter()
		switch {
		case err == nil && voter:
			if asked {
				log.Info("cluster join: promoted to raft voter", "node", nodeID)
			}
			return false
		case store.LeaderID() == "":
			if leaderlessSince.IsZero() {
				leaderlessSince = time.Now()
			} else if time.Since(leaderlessSince) >= leaderlessJoinDelay {
				log.Warn("raft non-voter has had no leader for a while; running the cluster join loop", "node", nodeID, "leaderless_for", leaderlessJoinDelay)
				return true
			}
		default:
			leaderlessSince = time.Time{}
			if !sv.AppliedCaughtUp() {
				break
			}
			res, addr, answered := walker.walk(ctx, req, stagedLeaderAddr(store))
			if !answered {
				break
			}
			asked = true
			outcome := parseJoinOutcome(res.Body)
			switch {
			case res.Status != http.StatusOK:
				log.Debug("cluster join: promotion request not accepted", "node", nodeID, "via", addr, "status", res.Status)
			case outcome.Status == metastore.JoinDeferred && outcome.Reason != lastReason:
				log.Info("cluster join: caught up as a raft non-voter; the leader defers promotion", "node", nodeID, "reason", outcome.Reason)
				lastReason = outcome.Reason
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// stagedLeaderAddr is the leader's node-RPC address from the local
// replica when the store can resolve it, else "".
func stagedLeaderAddr(store leaderWatcher) string {
	if st, ok := store.(*metastore.Store); ok {
		return leaderMemberAddr(st)
	}
	return ""
}

// joinOutcome is the body of a 200 join answer: staged, deferred,
// promoted or voter, with the deferral reason ("joined" from a 3.0.x
// leader).
type joinOutcome struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

func parseJoinOutcome(body []byte) joinOutcome {
	var out joinOutcome
	_ = json.Unmarshal(body, &out)
	return out
}

// joinRejection names a join answer for log-once bookkeeping: "" for a
// 200, the body's code when it has one (cluster.JoinCodeOlderRelease),
// else the status.
func joinRejection(res nodewire.Response) string {
	if res.Status == http.StatusOK {
		return ""
	}
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(res.Body, &body) == nil && body.Code != "" {
		return body.Code
	}
	return strconv.Itoa(res.Status)
}

// sendJoin sends one join request. A 3.0.x node refuses a request
// carrying EntryTypes with a trailing-field refusal before it looks at
// anything else; the request is sent again at once without the field,
// which that node cannot check anyway. Every join sender goes through
// here: an initial member probing for an existing cluster that read a
// 3.0.x peer's 400 as "no cluster" would bootstrap a rival one.
func sendJoin(ctx context.Context, peer clusterJoiner, addr string, req nodewire.JoinClusterRequest) (nodewire.Response, error) {
	res, err := peer.JoinCluster(ctx, addr, req)
	if err == nil && req.EntryTypes != 0 && cluster.IsTrailingFieldRefusal(res) {
		req.EntryTypes = 0
		res, err = peer.JoinCluster(ctx, addr, req)
	}
	return res, err
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

// runClusterJoin asks for admission until this node's Raft learns a
// leader (proof the leader has admitted and contacted it), or ctx is
// cancelled. Each attempt walks the configured peers and the leader
// addresses earlier attempts learned, following a non-leader's 421 to
// the leader it names (joinWalker): the configured list is pinned to the
// first pods while leadership moves freely, so the leader is often not
// in it. Safe to run on a node that is already a member: the loop exits
// on the first leader sighting without sending anything if Raft already
// knows one.
//
// The request carries the newest Raft entry type this release applies.
// A leader whose every member applies more refuses it (409, code
// older_release): logged at error once, then retried quietly until the
// operator upgrades this node.
func runClusterJoin(ctx context.Context, store leaderWatcher, peer clusterJoiner, cfg *config.Config, nodeID string, fresh bool, log *slog.Logger) {
	req := nodewire.JoinClusterRequest{
		ID:          nodeID,
		ClusterAddr: clusterAdvertiseAddr(cfg, nodeID),
		Fresh:       fresh,
		EntryTypes:  metastore.MaxEntryType,
	}
	walker := &joinWalker{peer: peer, cfg: cfg, nodeID: nodeID, log: log}
	ticker := time.NewTicker(clusterJoinRetryInterval)
	defer ticker.Stop()
	attempts := 0
	lastRejection := ""
	for {
		if store.LeaderID() != "" {
			log.Info("cluster join: admitted", "node", nodeID, "leader", store.LeaderID(), "attempts", attempts)
			return
		}
		if res, addr, ok := walker.walk(ctx, req, ""); ok {
			rejection := joinRejection(res)
			switch {
			case res.Status == http.StatusOK:
				log.Info("cluster join: leader accepted", "node", nodeID, "via", addr, "outcome", parseJoinOutcome(res.Body).Status)
			case rejection == cluster.JoinCodeOlderRelease:
				// Every member applies Raft entry types this release does
				// not: the cluster may already use them. Say so once, at
				// error, then keep retrying quietly.
				if lastRejection != rejection {
					log.Error("cluster join refused: this node runs an older release than every member of the cluster, which may already use Raft entries it cannot apply. Upgrade it to the cluster's release",
						"node", nodeID, "via", addr, "entry_types", req.EntryTypes, "body", strings.TrimSpace(string(res.Body)))
				}
			case res.Status == http.StatusConflict:
				// The leader refuses the old incarnation of a removed ID.
				// Say so once, loudly, then keep retrying quietly: the
				// operator either scales this pod away or wipes its volume.
				if lastRejection != rejection {
					log.Warn("cluster join refused: this node was decommissioned and removed; it will not rejoin with its old data directory. Scale it away, or delete its volume to rejoin as a new node",
						"node", nodeID, "via", addr, "body", strings.TrimSpace(string(res.Body)))
				}
			default:
				log.Warn("cluster join rejected", "via", addr, "status", res.Status)
			}
			lastRejection = rejection
		}
		attempts++
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Bounds on 421 hints: how many leader addresses a joiner remembers, and
// how many hints one walk follows, so a chain of stale hints cannot
// stretch an attempt or grow the target list without bound.
const (
	joinLearnedLeaders = 8
	joinHintFollows    = 4
)

// joinWalker walks the join targets for one node, in order: an optional
// first address, the configured peers, then the leader addresses learned
// from earlier 421 hints (most recent first). A 421 naming the leader's
// member address puts it next in the walk. Learned addresses outlive the
// walk, so a later attempt still reaches a leader no configured peer can
// name any more (every pinned pod down).
type joinWalker struct {
	peer    clusterJoiner
	cfg     *config.Config
	nodeID  string
	log     *slog.Logger
	learned []string
}

// walk sends req along the targets until one answers with anything but
// a transport error, 421 (not the leader) or 412 (no configuration), and
// returns that answer and its address; ok is false if none did.
func (w *joinWalker) walk(ctx context.Context, req nodewire.JoinClusterRequest, first string) (res nodewire.Response, addr string, ok bool) {
	var queue []string
	add := func(a string) {
		if a != "" && !slices.Contains(queue, a) {
			queue = append(queue, a)
		}
	}
	add(first)
	for _, p := range w.cfg.Cluster.Peers {
		if p.ID != w.nodeID {
			add(peerMemberAddr(p.Addr, w.cfg.HTTP.Addr))
		}
	}
	for _, a := range w.learned {
		add(a)
	}
	follows := 0
	for i := 0; i < len(queue); i++ {
		target := queue[i]
		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		res, err := sendJoin(rpcCtx, w.peer, target, req)
		cancel()
		switch {
		case err != nil:
			w.log.Debug("cluster join attempt failed", "addr", target, "err", err)
			continue
		case res.Status == http.StatusMisdirectedRequest:
			// Not the leader: ask the leader it names next, unless this
			// walk already asked it or has followed its share of hints.
			hint := joinHint(res.Body)
			if hint == "" || follows >= joinHintFollows || slices.Contains(queue[:i+1], hint) {
				continue
			}
			follows++
			w.learn(hint)
			queue = slices.DeleteFunc(queue, func(a string) bool { return a == hint })
			queue = slices.Insert(queue, i+1, hint)
			continue
		case res.Status == http.StatusPreconditionFailed:
			continue // no configuration yet; try the next target
		}
		return res, target, true
	}
	return nodewire.Response{}, "", false
}

// learn remembers a leader address, most recent first, bounded.
func (w *joinWalker) learn(addr string) {
	w.learned = slices.DeleteFunc(w.learned, func(a string) bool { return a == addr })
	w.learned = slices.Insert(w.learned, 0, addr)
	if len(w.learned) > joinLearnedLeaders {
		w.learned = w.learned[:joinLearnedLeaders]
	}
}

// joinHint extracts the leader's member address from a 421 join answer,
// or "" when the body names none or names something that is not a
// plausible host:port. The hint comes from a cluster peer over the
// node-RPC plane and following it only sends this node's own join
// request there; the checks keep a garbled body from producing a
// nonsense dial.
func joinHint(body []byte) string {
	var hint struct {
		LeaderAddr string `json:"leader_addr"`
	}
	if json.Unmarshal(body, &hint) != nil {
		return ""
	}
	addr := strings.TrimSpace(hint.LeaderAddr)
	if addr == "" || len(addr) > 261 { // a 253-byte DNS name, ':' and a port
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return ""
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return ""
	}
	return addr
}

// existingClusterAnswers reports whether any configured peer answers the
// join request as a member of a CONFIGURED cluster: accepted (200), not
// the leader (421), or refused as a removed ID or as older than every
// member (409). A peer with no Raft configuration (412), one that is
// down, or one not yet listening is not evidence. An initial member with
// no local state asks this before bootstrapping: "yes" means the cluster
// already exists and this node must join it, whatever its old role was.
// The request carries Fresh=true, which is what makes a readmission of a
// removed ID legal, and is resent without its entry types to a 3.0.x
// peer that refuses them (sendJoin).
func existingClusterAnswers(ctx context.Context, peer clusterJoiner, cfg *config.Config, nodeID string, log *slog.Logger) bool {
	req := nodewire.JoinClusterRequest{
		ID:          nodeID,
		ClusterAddr: clusterAdvertiseAddr(cfg, nodeID),
		Fresh:       true,
		EntryTypes:  metastore.MaxEntryType,
	}
	for range existingClusterProbeRounds {
		for _, p := range cfg.Cluster.Peers {
			if p.ID == nodeID {
				continue
			}
			addr := peerMemberAddr(p.Addr, cfg.HTTP.Addr)
			if addr == "" {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, existingClusterProbeTimeout)
			res, err := sendJoin(rpcCtx, peer, addr, req)
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
