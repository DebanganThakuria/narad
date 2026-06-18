// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

// Router forwards HTTP requests to the pod that owns the target partition.
// All read methods hit the local bbolt replica (fast, ms-stale).
type Router struct {
	store      *metastore.Store
	selfID     string
	partitions partition.Manager
	snapshots  metrics.SnapshotProvider
}

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager, snapshots metrics.SnapshotProvider) *Router {
	return &Router{store: store, selfID: selfID, partitions: mgr, snapshots: snapshots}
}

// RouteProduce forwards a produce request to the first alive partition owner
// starting from the key-hashed partition and walking forward circularly.
// body is the already-read request body bytes. Returns true if forwarded.
func (rt *Router) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	t, err := rt.store.GetTopic(ctx, topicName)
	if err != nil || t.Partitions == 0 {
		return false
	}
	start := rt.partitions.Pick(topicName, key, t.Partitions)
	for i := 0; i < t.Partitions; i++ {
		p := (start + i) % t.Partitions
		addr, local := rt.produceOwnerAddr(topicName, p)
		if local {
			return false
		}
		if addr == "" {
			continue
		}
		fwd := r.Clone(ctx)
		q := fwd.URL.Query()
		q.Set("partition", strconv.Itoa(p))
		fwd.URL.RawQuery = q.Encode()
		probe := rt.forwardProbe(fwd, addr, body)
		if probe.code < http.StatusOK || probe.code >= http.StatusMultipleChoices {
			continue
		}
		copyHeader(w.Header(), probe.header)
		w.WriteHeader(probe.code)
		if len(probe.body) > 0 {
			_, _ = w.Write(probe.body)
		}
		return true
	}
	return false
}

// RouteConsume forwards a consume request to the owner of a partition.
// pinnedPartition is set when the caller already chose a partition (replay
// or pinned consume); nil causes the router to walk candidate partitions once
// with non-blocking probes.
// Returns true if forwarded. For queue-style pulls, localPartition is set when
// the request should be handled locally against a specific owned partition.
func (rt *Router) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) (bool, *int) {
	if pinnedPartition != nil {
		addr := rt.ownerAddr(topicName, *pinnedPartition)
		if addr == "" {
			return false, nil
		}
		fwd := r.Clone(ctx)
		q := fwd.URL.Query()
		q.Set("partition", strconv.Itoa(*pinnedPartition))
		fwd.URL.RawQuery = q.Encode()
		rt.forward(w, fwd, addr, nil)
		return true, nil
	}

	candidates := rt.consumePartitionCandidates(ctx, topicName)
	var localPartition *int
	for _, candidate := range candidates {
		if candidate.local {
			if localPartition == nil {
				localPartition = new(candidate.partition)
			}
			continue
		}
		_, forwarded := rt.forwardConsumeProbe(ctx, w, r, candidate.partition, candidate.addr)
		if forwarded {
			return true, nil
		}
	}
	if localPartition != nil {
		return false, localPartition
	}
	if len(candidates) > 0 {
		w.WriteHeader(http.StatusNoContent)
		return true, nil
	}
	return false, nil
}

// RouteAck forwards an ack request to the owner of the given partition.
// body is the already-read request body bytes.
// Returns true if forwarded.
func (rt *Router) RouteAck(_ context.Context, w http.ResponseWriter, r *http.Request, topicName string, partition int, body []byte) bool {
	addr := rt.ownerAddr(topicName, partition)
	if addr == "" {
		return false
	}
	rt.forward(w, r, addr, body)
	return true
}

// RouteCreateTopic forwards a topic create request to the cluster leader.
func (rt *Router) RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool {
	return rt.routeToLeader(ctx, w, r, body)
}

// RouteAlterTopic forwards a topic alter request to the cluster leader.
func (rt *Router) RouteAlterTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, _ string, body []byte) bool {
	return rt.routeToLeader(ctx, w, r, body)
}

// RouteDeleteTopic forwards a topic delete request to the cluster leader.
func (rt *Router) RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, _ string) bool {
	return rt.routeToLeader(ctx, w, r, nil)
}

func (rt *Router) BroadcastDeleteTopic(ctx context.Context, topicName string) error {
	members, err := rt.store.ListMembers()
	if err != nil {
		return err
	}
	for _, member := range members {
		if member.Status == metastore.MemberDead || strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "/internal/v1/topics/"+topicName, nil)
		if err != nil {
			return err
		}
		probe := rt.forwardProbe(req, member.Addr, nil)
		if probe.code < http.StatusOK || probe.code >= http.StatusMultipleChoices {
			return fmt.Errorf("delete purge returned status %d for %s", probe.code, member.ID)
		}
	}
	return nil
}

func (rt *Router) routeToLeader(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool {
	memberAddr := rt.memberAddrByClusterAddr(rt.store.LeaderAddr())
	if memberAddr == "" {
		return false
	}
	fwd := r.Clone(ctx)
	rt.forward(w, fwd, memberAddr, body)
	return true
}
