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
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return false, false
	}
	if !cache.valid || cache.assignmentVersion != routes.assignmentVersion || cache.routingMembersVersion != routes.routingMembersVersion {
		cache.addrs = rt.remoteOwnerAddrs(routes, cache.addrs)
		cache.assignmentVersion = routes.assignmentVersion
		cache.routingMembersVersion = routes.routingMembersVersion
		cache.valid = true
	}
	if len(cache.addrs) == 0 {
		return false, false
	}
	start := rt.nextConsumeCursor(topicName+":remote", len(cache.addrs))
	return rt.probeCandidates(ctx, w, topicName, cache.addrs, start), true
}

// probeCandidates probes each candidate once, starting at index start and
// wrapping around, and writes the first delivered message to w. It
// reports whether a message was written.
func (rt *Router) probeCandidates(ctx context.Context, w http.ResponseWriter, topicName string, candidates []string, start int) bool {
	for i := range candidates {
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
	probeCtx, cancel := context.WithTimeout(ctx, consumeProbeTimeout)
	defer cancel()
	res, err := rt.peer.Consume(probeCtx, addr, req)
	if err != nil {
		return consumeProbeResult{err: fmt.Errorf("consume probe %s: %w", addr, err)}
	}
	if res.Status != http.StatusOK {
		return consumeProbeResult{res: nodewire.Response{Status: http.StatusNoContent}}
	}
	return consumeProbeResult{res: res}
}
