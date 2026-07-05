package cluster

import (
	"context"
	"fmt"
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

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

	remote := routes.remoteEntries
	cursor := rt.nextConsumeCursor(topicName+":remote", len(remote))

	seenOwners := make(map[string]struct{}, len(remote))
	candidates := make([]string, 0, len(remote))
	for i := range remote {
		entry := remote[(cursor+i)%len(remote)]
		addr := rt.consumeOwnerAddr(entry)
		if addr == "" {
			continue
		}
		if _, ok := seenOwners[addr]; ok {
			continue
		}
		seenOwners[addr] = struct{}{}
		candidates = append(candidates, addr)
	}
	return candidates
}

// callConsumeProbe asks one remote owner for a single non-blocking,
// local-only scan of its partitions. Non-OK statuses (including 421 from an
// owner that just lost the partition) are normalized to an empty 204 so the
// caller simply moves on to the next candidate.
func (rt *Router) callConsumeProbe(ctx context.Context, topicName, addr string) consumeProbeResult {
	req := nodewire.ConsumeRequest{
		Topic:     topicName,
		LocalOnly: true,
	}
	res, err := rt.peer.Consume(ctx, addr, req)
	if err != nil {
		return consumeProbeResult{err: fmt.Errorf("consume probe %s: %w", addr, err)}
	}
	if res.Status != http.StatusOK {
		return consumeProbeResult{res: nodewire.Response{Status: http.StatusNoContent}}
	}
	return consumeProbeResult{res: res}
}
