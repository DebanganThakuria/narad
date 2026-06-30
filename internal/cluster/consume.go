package cluster

import (
	"context"
	"fmt"
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type consumeNodeCandidate struct {
	addr string
}

type consumeProbeResult struct {
	res   nodewire.Response
	err   error
	fatal bool
}

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

func (rt *Router) remoteConsumeCandidates(topicName string) []consumeNodeCandidate {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return nil
	}

	remote := routes.remoteEntries
	cursor := rt.nextConsumeCursor(topicName+":remote", len(remote))

	seenOwners := make(map[string]struct{}, len(remote))
	candidates := make([]consumeNodeCandidate, 0, len(remote))
	for i := range remote {
		entry := remote[(cursor+i)%len(remote)]
		addr := rt.consumeOwnerAddrForRoute(entry)
		if addr == "" {
			continue
		}
		if _, ok := seenOwners[addr]; ok {
			continue
		}
		seenOwners[addr] = struct{}{}
		candidates = append(candidates, consumeNodeCandidate{addr: addr})
	}
	return candidates
}

func (rt *Router) callConsumeProbe(ctx context.Context, topicName string, candidate consumeNodeCandidate) consumeProbeResult {
	req := nodewire.ConsumeRequest{
		Topic:     topicName,
		LocalOnly: true,
	}
	res, err := rt.peer.Consume(ctx, candidate.addr, req)
	if err != nil {
		return consumeProbeResult{err: fmt.Errorf("consume probe %s: %w", candidate.addr, err)}
	}
	if res.Status != http.StatusOK {
		return consumeProbeResult{res: nodewire.Response{Status: http.StatusNoContent}}
	}
	return consumeProbeResult{res: res}
}
