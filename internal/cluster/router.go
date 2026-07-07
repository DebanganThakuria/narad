// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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

	// consumeReprobeInterval paces the remote re-probe loop for queue-style
	// long-poll consumes on nodes that own no partitions of the topic.
	// Defaults to remoteConsumeReprobeInterval; tests shrink it.
	consumeReprobeInterval time.Duration

	// maxConsumeWait caps the client-supplied ?wait= budget on every
	// long-poll consume path this router touches (pinned forwards and the
	// remote re-probe loop). Defaults to defaultMaxConsumeWait; serve.go
	// overrides it with the configured http.max_consume_wait via
	// SetMaxConsumeWait so the router honors the same ceiling as the HTTP
	// handlers.
	maxConsumeWait time.Duration
}

// defaultMaxConsumeWait is the ceiling applied to a long-poll consume wait
// when no configured value has been wired in via SetMaxConsumeWait. It
// mirrors handlers.DefaultMaxConsumeWait (the HTTP layer's fallback ceiling,
// not imported to keep this package below the transport layer) so routers
// built without explicit wiring — tests, mostly — stay bounded.
const defaultMaxConsumeWait = 30 * time.Second

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager, clusterSecret string) *Router {
	return &Router{
		store:                  store,
		selfID:                 selfID,
		partitions:             mgr,
		peer:                   NewPeerClient(defaultPeerRPCTimeout, clusterSecret),
		routes:                 make(map[string]cachedRouteTable),
		consumeCursor:          make(map[string]uint64),
		consumeReprobeInterval: remoteConsumeReprobeInterval,
		maxConsumeWait:         defaultMaxConsumeWait,
	}
}

// SetMaxConsumeWait wires the configured long-poll consume wait ceiling
// (http.max_consume_wait). Values <= 0 keep the defaultMaxConsumeWait
// fallback, matching how the HTTP handlers treat an unset config value.
func (rt *Router) SetMaxConsumeWait(d time.Duration) {
	if d > 0 {
		rt.maxConsumeWait = d
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
		addr, local := rt.produceOwnerAddr(entry)
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
		req, err := consumeRPCRequestFromHTTP(r, topicName, pinnedPartition, false, rt.maxConsumeWait)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return true, nil
		}
		// A long-poll forward legitimately waits up to the requested
		// duration on the remote owner; give it an explicit deadline of
		// wait + grace so the transport's short no-deadline fallback
		// timeout does not cut the poll short.
		consumeCtx, cancel := longWaitRPCContext(ctx, time.Duration(req.WaitNanos))
		defer cancel()
		res, err := rt.peer.Consume(consumeCtx, addr, req)
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
		// The handler contract says wait > 0 long-polls up to the wait
		// for a message. Honor that budget even though this node owns no
		// partitions of the topic: keep re-probing the remote owners
		// until a message materializes. Answering 204 immediately would
		// make long-poll behavior depend on which node a load balancer
		// picked and degrade clients into busy-polling.
		if rt.longPollConsumeRemote(ctx, w, r, topicName) {
			return true, nil
		}
		w.WriteHeader(http.StatusNoContent)
		return true, nil
	}
	return false, nil
}

// longWaitRPCGrace is added on top of a known server-side wait when
// deriving an explicit RPC deadline, covering transfer and scan overhead.
const longWaitRPCGrace = 2 * time.Second

// longWaitRPCContext bounds an RPC whose server side legitimately blocks
// for up to wait (e.g. a forwarded long-poll consume) to wait + grace when
// the inbound context carries no deadline of its own. Without an explicit
// deadline the peer transport applies its short default reply timeout,
// which would cut such calls short.
func longWaitRPCContext(ctx context.Context, wait time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || wait <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, wait+longWaitRPCGrace)
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

	for _, addr := range candidates {
		result := rt.callConsumeProbe(ctx, topicName, addr)
		if result.err != nil {
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

// remoteConsumeReprobeInterval paces longPollConsumeRemote. Each round costs
// one RPC per remote owner, so the interval trades delivery latency against
// probe QPS: a few hundred ms keeps the worst-case added latency small while
// capping the extra load at len(owners)/interval RPCs per waiting client.
const remoteConsumeReprobeInterval = 250 * time.Millisecond

// longPollConsumeRemote honors a queue-style long-poll on a node that owns
// no partitions of the topic: it re-probes every remote owner on a fixed
// interval until a message materializes (response written, returns true),
// the wait budget expires, or the request context is done (returns false;
// the caller answers 204). Re-probing all owners each round is preferred
// over parking the whole wait on a single owner because a message can
// materialize on any owner. Each probe is non-blocking and individually
// bounded by the peer transport's default reply timeout, so unlike a pinned
// long-poll forward the loop needs no longWaitRPCContext-stretched deadline.
func (rt *Router) longPollConsumeRemote(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string) bool {
	// The HTTP handler already rejected malformed wait values, so a parse
	// failure here conservatively degrades to no wait.
	wait, err := consumeWaitFromHTTP(r, rt.maxConsumeWait)
	if err != nil || wait <= 0 {
		return false
	}
	deadline := time.Now().Add(wait)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			return false
		}
		interval := rt.consumeReprobeInterval
		if interval <= 0 {
			interval = remoteConsumeReprobeInterval
		}
		if remaining < interval {
			interval = remaining
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		forwarded, hadCandidates := rt.RouteConsumeRemote(ctx, w, r, topicName)
		if forwarded {
			return true
		}
		if !hadCandidates {
			// Ownership changed under us (e.g. a rebalance removed every
			// remote owner); nothing is left to poll against.
			return false
		}
	}
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

// RouteExtendAck forwards a visibility-window extension to the owner of
// the handle partition. Returns true if forwarded.
func (rt *Router) RouteExtendAck(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	addr := rt.ownerAddr(topicName, handle.Partition)
	if addr == "" {
		return false
	}
	res, err := rt.peer.ExtendAck(ctx, addr, nodewire.AckRequest{
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

// RouteNack forwards an immediate reservation release to the owner of
// the handle partition. Returns true if forwarded.
func (rt *Router) RouteNack(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	addr := rt.ownerAddr(topicName, handle.Partition)
	if addr == "" {
		return false
	}
	res, err := rt.peer.Nack(ctx, addr, nodewire.AckRequest{
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
