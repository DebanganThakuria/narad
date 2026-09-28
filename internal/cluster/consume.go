package cluster

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// consumeProbeTimeout bounds one non-blocking remote consume probe (wait
// 0, local_only). The probe is a single local scan on the owner, so a
// peer that takes longer than this is stalled or unreachable and is
// skipped for the round; without an explicit deadline the transport's
// 5s fallback applied per peer, serializing seconds of dead time into a
// request whose own budget may be much shorter. Pinned long-poll
// forwards carry the client's wait and are not subject to this.
const consumeProbeTimeout = 500 * time.Millisecond

type consumeProbeResult struct {
	res nodewire.Response
	err error
}

// localConsumePartition picks the next locally-owned partition of the topic
// for a queue-style pull, rotating a per-topic cursor so pulls spread across
// this node's partitions.
func (rt *Router) localConsumePartition(topicName string) (int, bool) {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return 0, false
	}
	if len(routes.localEntries) == 0 {
		return 0, false
	}

	cursor := rt.nextConsumeCursor(topicName+":local", len(routes.localEntries))
	return routes.localEntries[cursor].partition, true
}

// remoteConsumeCandidates returns the addresses of the topic's live remote
// owners, one per owner (an owner with several partitions is probed once),
// rotated by a per-topic cursor so probe order spreads across owners.
func (rt *Router) remoteConsumeCandidates(topicName string) []string {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return nil
	}
	owners := rt.remoteOwnerAddrs(routes, nil)
	if len(owners) == 0 {
		return nil
	}
	start := rt.nextConsumeCursor(topicName+":remote", len(owners))
	rotated := make([]string, 0, len(owners))
	for i := range owners {
		rotated = append(rotated, owners[(start+i)%len(owners)])
	}
	return rotated
}

// hasRemoteOwner reports whether the topic has a live remote owner,
// without building the owner list or touching the probe cursor.
func (rt *Router) hasRemoteOwner(topicName string) bool {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return false
	}
	for _, entry := range routes.remoteEntries {
		if rt.consumeOwnerAddr(entry) != "" {
			return true
		}
	}
	return false
}

// remoteOwnerAddrsForTopic returns the topic's live remote owner
// addresses in partition order, without touching the probe cursor.
func (rt *Router) remoteOwnerAddrsForTopic(topicName string) []string {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return nil
	}
	return rt.remoteOwnerAddrs(routes, nil)
}

// remoteOwnerAddrs appends the unique live remote owner addresses of the
// route table to dst (in partition order) and returns it.
func (rt *Router) remoteOwnerAddrs(routes cachedRouteTable, dst []string) []string {
	dst = dst[:0]
	for _, entry := range routes.remoteEntries {
		addr := rt.consumeOwnerAddr(entry)
		if addr == "" {
			continue
		}
		seen := slices.Contains(dst, addr)
		if !seen {
			dst = append(dst, addr)
		}
	}
	return dst
}

// remoteCandidateCache keeps the re-probe loop's owner list across rounds
// so it is rebuilt only when the route table's versions change.
type remoteCandidateCache struct {
	valid                 bool
	assignmentVersion     uint64
	routingMembersVersion uint64
	addrs                 []string
}

// reprobeRemote runs one round of the long-poll re-probe: it refreshes the
// cached owner list only when the route table changed, rotates the start
// owner, and probes each owner once. Returns (forwarded, hadCandidates)
// with the same meaning as RouteConsumeRemote.
func (rt *Router) reprobeRemote(ctx context.Context, w http.ResponseWriter, topicName string, cache *remoteCandidateCache) (bool, bool) {
	forwarded, hadCandidates, _ := rt.reprobeRemoteUpTo(ctx, w, topicName, cache, 0)
	return forwarded, hadCandidates
}

// reprobeRemoteUpTo is reprobeRemote for a consume that takes up to max
// records (see probeBatchCandidates); batch reports that what it wrote
// is a batch body.
func (rt *Router) reprobeRemoteUpTo(ctx context.Context, w http.ResponseWriter, topicName string, cache *remoteCandidateCache, max int) (forwarded, hadCandidates, batch bool) {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return false, false, false
	}
	if !cache.valid || cache.assignmentVersion != routes.assignmentVersion || cache.routingMembersVersion != routes.routingMembersVersion {
		cache.addrs = rt.remoteOwnerAddrs(routes, cache.addrs)
		cache.assignmentVersion = routes.assignmentVersion
		cache.routingMembersVersion = routes.routingMembersVersion
		cache.valid = true
	}
	if len(cache.addrs) == 0 {
		return false, false, false
	}
	start := rt.nextConsumeCursor(topicName+":remote", len(cache.addrs))
	if max > 1 {
		forwarded, batch = rt.probeBatchCandidates(ctx, w, topicName, cache.addrs, start, max)
	} else {
		forwarded = rt.probeCandidates(ctx, w, topicName, cache.addrs, start)
	}
	return forwarded, true, batch
}

// probeCandidates probes each candidate once, starting at index start and
// wrapping around, and writes the first delivered message to w. It
// reports whether a message was written. It stops early once ctx is
// done.
func (rt *Router) probeCandidates(ctx context.Context, w http.ResponseWriter, topicName string, candidates []string, start int) bool {
	for i := range candidates {
		if ctx.Err() != nil {
			// The client is gone: probing the rest would only reserve a
			// record for nobody and release it again.
			return false
		}
		addr := candidates[(start+i)%len(candidates)]
		result := rt.callConsumeProbe(ctx, topicName, addr)
		if result.err != nil {
			continue
		}
		if result.res.Status == http.StatusNoContent {
			continue
		}
		writePeerResponse(w, result.res)
		return true
	}
	return false
}

// probeBatchCandidates is probeCandidates for a batch consume: each
// owner is asked for up to max records (see consumeFrom), and batch
// reports that what was written is the owner's {"messages":[...]} body.
// It is false for a single message, from an owner too old to take the
// field. It is kept apart from probeCandidates so a single-record probe,
// the one every consume on an owning node makes when its own partitions
// are empty, pays nothing for it.
func (rt *Router) probeBatchCandidates(ctx context.Context, w http.ResponseWriter, topicName string, candidates []string, start, max int) (forwarded, batch bool) {
	for i := range candidates {
		if ctx.Err() != nil {
			// The client is gone: probing the rest would only reserve
			// records for nobody and release them again.
			return false, false
		}
		addr := candidates[(start+i)%len(candidates)]
		res, isBatch, err := rt.consumeFrom(ctx, addr, nodewire.ConsumeRequest{Topic: topicName, LocalOnly: true}, max, consumeProbeTimeout)
		if err != nil || res.Status != http.StatusOK {
			// As for a single probe: an owner that failed, timed out or
			// just lost the partition is skipped for this round.
			continue
		}
		writePeerResponse(w, res)
		return true, isBatch
	}
	return false, false
}

// callConsumeProbe asks one remote owner for a single non-blocking,
// local-only scan of its partitions. Non-OK statuses (including 421 from an
// owner that just lost the partition) are normalized to an empty 204 so the
// caller simply moves on to the next candidate. A probe that exceeds
// consumeProbeTimeout fails with DeadlineExceeded, which the caller treats
// the same way: skip this owner for the round.
func (rt *Router) callConsumeProbe(ctx context.Context, topicName, addr string) consumeProbeResult {
	req := nodewire.ConsumeRequest{
		Topic:     topicName,
		LocalOnly: true,
	}
	res, err := rt.peer.ConsumeWithin(ctx, addr, consumeProbeTimeout, req)
	if err != nil {
		return consumeProbeResult{err: fmt.Errorf("consume probe %s: %w", addr, err)}
	}
	if res.Status != http.StatusOK {
		return consumeProbeResult{res: nodewire.Response{Status: http.StatusNoContent}}
	}
	return consumeProbeResult{res: res}
}
