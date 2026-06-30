// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Router forwards HTTP requests to the pod that owns the target partition.
// All read methods hit the local bbolt replica (fast, ms-stale).
type Router struct {
	store      *metastore.Store
	selfID     string
	partitions partition.Manager
	peer       peerClient

	routeMu       sync.RWMutex
	routes        map[string]cachedRouteTable
	consumeMu     sync.Mutex
	consumeCursor map[string]uint64
}

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager, _ metrics.SnapshotProvider) *Router {
	return &Router{
		store:         store,
		selfID:        selfID,
		partitions:    mgr,
		peer:          NewPeerClient(defaultPeerRPCTimeout),
		routes:        make(map[string]cachedRouteTable),
		consumeCursor: make(map[string]uint64),
	}
}

// RouteProduce forwards a produce request to the first alive partition owner
// starting from the key-hashed partition and walking forward circularly.
// body is the already-read request body bytes. Returns true if forwarded.
func (rt *Router) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	routes, ok := rt.routesForTopic(topicName)
	if !ok || len(routes.entries) == 0 || routes.partitions == 0 {
		return false
	}
	cursor := rt.partitions.Pick(topicName, key, routes.partitions)
	for i := 0; i < routes.partitions; i++ {
		p := (cursor + i) % routes.partitions
		entry, exists := routes.byPartition[p]
		if !exists {
			continue
		}
		addr, local := rt.produceOwnerAddrForRoute(entry)
		if local {
			return false
		}
		if addr == "" {
			continue
		}
		res, err := rt.peer.Produce(ctx, addr, nodewire.ProduceRequest{
			Topic:     topicName,
			Key:       key,
			Partition: p,
			Payload:   body,
		})
		if err != nil || res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
			continue
		}
		writePeerResponse(w, res)
		return true
	}
	return false
}

// RouteConsume forwards a consume request to the owner of a partition.
// pinnedPartition is set when the caller already chose a partition (replay
// or pinned consume); nil queue-style pulls prefer the local node first.
// Returns true if forwarded. For queue-style pulls, localPartition is set when
// the request should be handled locally against all partitions owned by this node.
func (rt *Router) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) (bool, *int) {
	if pinnedPartition != nil {
		addr := rt.ownerAddr(topicName, *pinnedPartition)
		if addr == "" {
			return false, nil
		}
		req, err := consumeRPCRequestFromHTTP(r, topicName, pinnedPartition, false)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return true, nil
		}
		res, err := rt.peer.Consume(ctx, addr, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return true, nil
		}
		writePeerResponse(w, res)
		return true, nil
	}

	if localPartition, ok := rt.localConsumePartition(topicName); ok {
		return false, &localPartition
	}

	forwarded, hadCandidates := rt.RouteConsumeRemote(ctx, w, r, topicName)
	if forwarded {
		return true, nil
	}
	if hadCandidates {
		w.WriteHeader(http.StatusNoContent)
		return true, nil
	}
	return false, nil
}

// RouteConsumeRemote probes remote nodes for queue-style consume. Each remote
// node scans its own local partitions exactly once. The probes are
// non-blocking; when all remote owners are empty, the
// caller decides whether to return 204 or keep polling a local partition.
func (rt *Router) RouteConsumeRemote(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) (bool, bool) {
	candidates := rt.remoteConsumeCandidates(topicName)
	if len(candidates) == 0 {
		return false, false
	}

	for _, candidate := range candidates {
		result := rt.callConsumeProbe(ctx, topicName, candidate)
		if result.err != nil {
			if result.fatal {
				http.Error(w, result.err.Error(), http.StatusBadRequest)
				return true, true
			}
			continue
		}
		if result.res.Status == http.StatusNoContent {
			continue
		}
		writePeerResponse(w, result.res)
		return true, true
	}
	return false, true
}

// RouteAck forwards an ack request to the owner of the handle partition.
// Returns true if forwarded.
func (rt *Router) RouteAck(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	addr := rt.ownerAddr(topicName, handle.Partition)
	if addr == "" {
		return false
	}
	res, err := rt.peer.Ack(ctx, addr, nodewire.AckRequest{
		Topic:     topicName,
		Partition: handle.Partition,
		Offset:    handle.Offset,
		Nonce:     handle.Nonce,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// RouteCreateTopic forwards a topic create request to the cluster leader.
func (rt *Router) RouteCreateTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, body []byte) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.CreateTopic(ctx, memberAddr, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// RouteAlterTopic forwards a topic alter request to the cluster leader.
func (rt *Router) RouteAlterTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, body []byte) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.AlterTopic(ctx, memberAddr, topicName, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// RouteDeleteTopic forwards a topic delete request to the cluster leader.
func (rt *Router) RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.DeleteTopic(ctx, memberAddr, topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}

func (rt *Router) BroadcastDeleteTopic(ctx context.Context, topicName string) error {
	members, err := rt.store.ListMembers()
	if err != nil {
		return err
	}
	// Attempt every live member even if one fails: a single unreachable
	// peer must not stop the others from purging. Any member we miss is
	// reclaimed by its startup orphan sweep.
	var joined error
	for _, member := range members {
		if member.Status == metastore.MemberDead || strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) {
			continue
		}
		res, err := rt.peer.PurgeTopic(ctx, member.Addr, topicName)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("purge %s on %s: %w", topicName, member.ID, err))
			continue
		}
		if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
			joined = errors.Join(joined, fmt.Errorf("purge %s returned status %d for %s", topicName, res.Status, member.ID))
		}
	}
	return joined
}
