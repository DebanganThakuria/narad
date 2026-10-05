// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"context"
	"log/slog"
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
	// legacyClaim remembers owners (by address) that refused the Claim
	// flag on the node consume RPC: a release before the flag existed
	// answers 400 to any trailing byte. Claims to them go out as plain
	// local-only probes, which they accept and which still reserve the
	// record; they simply keep the deadline behaviour for the hold. Each
	// entry carries an expiry (a time.Time, legacyClaimTTL ahead): once
	// it passes the flag is tried again, so an owner upgraded mid-roll is
	// back on the fast path within a TTL at the cost of one refused claim
	// per owner per TTL while it is still old.
	legacyClaim sync.Map
	// legacyBatchConsume is legacyClaim for the Max field: owners that
	// refused a batch consume (a release before the field existed) and
	// are asked for one record at a time until the entry's expiry.
	legacyBatchConsume sync.Map

	// consumeReprobeInterval is the first interval of the remote re-probe
	// loop for queue-style long-poll consumes on nodes that own no
	// partitions of the topic; each empty round doubles it up to
	// consumeReprobeMaxInterval. Defaults to remoteConsumeReprobeInterval
	// and remoteConsumeReprobeMaxInterval; tests shrink them.
	consumeReprobeInterval    time.Duration
	consumeReprobeMaxInterval time.Duration

	// maxConsumeWait caps the client-supplied ?wait= budget on every
	// long-poll consume path this router touches (pinned forwards and the
	// remote re-probe loop). Defaults to defaultMaxConsumeWait; serve.go
	// overrides it with the configured http.max_consume_wait via
	// SetMaxConsumeWait so the router honors the same ceiling as the HTTP
	// handlers.
	maxConsumeWait time.Duration

	// tokens is the requester half of the token protocol: this node's
	// parked consumers and the interest it has registered with the owners
	// of the topics they wait on. Disabled until SetSelfAddr supplies a
	// return address for owners to call back on.
	tokens *tokenRequester

	// acks lets forwarded acks to one owner share an RPC when they
	// overlap (see ack_coalescer.go), and remembers the owners too old to
	// take a batch.
	acks ackCoalescer

	// logger receives what the router decides on its own and nobody else
	// reports, such as members still owing a topic purge. slog.Default
	// until serve.go wires the process logger with SetLogger.
	logger *slog.Logger
}

// defaultMaxConsumeWait is the ceiling applied to a long-poll consume wait
// when no configured value has been wired in via SetMaxConsumeWait. It
// mirrors handlers.DefaultMaxConsumeWait (the HTTP layer's fallback ceiling,
// not imported to keep this package below the transport layer) so routers
// built without explicit wiring (tests, mostly) stay bounded.
const defaultMaxConsumeWait = 30 * time.Second

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
// The router builds its own PeerClient so tests and single-node setups
// work unwired; serve.go replaces it with the process-wide client via
// SetPeerClient so the node holds one connection pool per peer.
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager, clusterSecret string) *Router {
	rt := &Router{
		store:                     store,
		selfID:                    selfID,
		partitions:                mgr,
		peer:                      NewPeerClient(defaultPeerRPCTimeout, clusterSecret),
		routes:                    make(map[string]cachedRouteTable),
		consumeCursor:             make(map[string]uint64),
		consumeReprobeInterval:    remoteConsumeReprobeInterval,
		consumeReprobeMaxInterval: remoteConsumeReprobeMaxInterval,
		maxConsumeWait:            defaultMaxConsumeWait,
		logger:                    slog.Default(),
	}
	rt.tokens = newTokenRequester(rt, "")
	return rt
}

// SetSelfAddr supplies the address peers should call back on and turns
// the token protocol on for this node. Without it a token could never be
// spent, so registering one would only strand it; the router falls back
// to the polling path instead. Call before serving.
func (rt *Router) SetSelfAddr(addr string) {
	rt.tokens = newTokenRequester(rt, addr)
}

// LocalDemand exposes the requester half to the RPC server, which turns
// an inbound notification into a woken consumer.
func (rt *Router) LocalDemand() *tokenRequester { return rt.tokens }

// RunTokenKeeper keeps this node's tokens registered with the owners of
// every topic that has consumers parked here, until ctx ends. Call after
// SetSelfAddr; without a return address there is nothing to keep.
func (rt *Router) RunTokenKeeper(ctx context.Context) { rt.tokens.Run(ctx) }

// SetPeerClient makes the router forward through pc instead of the client
// NewRouter built. A nil pc keeps the current client. Call before serving.
func (rt *Router) SetPeerClient(pc *PeerClient) {
	if pc != nil {
		rt.peer = pc
	}
}

// SetLogger makes the router log through l. A nil l keeps the current
// logger. Call before serving.
func (rt *Router) SetLogger(l *slog.Logger) {
	if l != nil {
		rt.logger = l
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
		addr, unavailable := rt.ownerRoute(topicName, *pinnedPartition)
		if unavailable {
			writeOwnerDown(w)
			return true, nil
		}
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
		// partitions of the topic. Answering 204 immediately would make
		// long-poll behavior depend on which node a load balancer picked
		// and degrade clients into busy-polling. The HTTP handler already
		// rejected malformed wait values, so a parse failure here
		// conservatively degrades to no wait.
		wait, err := consumeWaitFromHTTP(r, rt.maxConsumeWait)
		if err != nil {
			wait = 0
		}
		rt.consumeRemoteWait(ctx, w, topicName, wait, 0)
		return true, nil
	}
	if handled, _ := rt.awaitConsumeRoute(ctx, w, r, topicName, 0); handled {
		return true, nil
	}
	return false, nil
}

// RouteConsumeBatch is RouteConsume for a batch consume (?max=N): each
// forward asks the owner for up to max records (nodewire
// ConsumeRequest.Max) where RouteConsume's ask for one, so a node that
// owns none of the topic's partitions (or not the pinned one) serves a
// batch in one round trip. The remote owners are probed in turn and the
// first that has records answers the whole request; the request is not
// held to fill max from several owners, as a local batch is not. The
// wait phase is RouteConsume's, with max carried into its claims and
// re-probes, so a consumer that parked is served a batch too.
//
// batch reports that a 200 it wrote is already {"messages":[...]}. It is
// false for a single message the caller wraps: from an owner that does
// not take Max yet (asked again for one record, see consumeFrom and
// claimUpTo). forwarded and localPartition mean what they mean for
// RouteConsume.
func (rt *Router) RouteConsumeBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int, max int) (forwarded, batch bool, localPartition *int) {
	if pinnedPartition != nil {
		addr, unavailable := rt.ownerRoute(topicName, *pinnedPartition)
		if unavailable {
			writeOwnerDown(w)
			return true, false, nil
		}
		if addr == "" {
			return false, false, nil
		}
		req, err := consumeRPCRequestFromHTTP(r, topicName, pinnedPartition, false, rt.maxConsumeWait)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return true, false, nil
		}
		consumeCtx, cancel := longWaitRPCContext(ctx, time.Duration(req.WaitNanos))
		defer cancel()
		res, isBatch, err := rt.consumeFrom(consumeCtx, addr, req, max, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return true, false, nil
		}
		writePeerResponse(w, res)
		return true, isBatch, nil
	}

	if localPartition, ok := rt.localConsumePartition(topicName); ok {
		return false, false, &localPartition
	}

	forwarded, hadCandidates, batch := rt.routeConsumeRemote(ctx, w, r, topicName, max)
	if forwarded {
		return true, batch, nil
	}
	if hadCandidates {
		// The wait phase, as RouteConsume's (see there).
		wait, err := consumeWaitFromHTTP(r, rt.maxConsumeWait)
		if err != nil {
			wait = 0
		}
		return true, rt.consumeRemoteWait(ctx, w, topicName, wait, max), nil
	}
	if handled, batch := rt.awaitConsumeRoute(ctx, w, r, topicName, max); handled {
		return true, batch, nil
	}
	return false, false, nil
}

// consumeFrom sends req to the owner at addr asking for up to max
// records, and reports whether the reply answers a batch. timeout bounds
// the call as ConsumeWithin does (0: the transport's own bound, or ctx's
// deadline). An owner on a release before Max refuses the field with
// 400 (trailing payload): it is asked again for one record within what
// is left of the budget and remembered for legacyClaimTTL, so a rolling
// upgrade costs one refused request per owner per TTL, not one per
// consume.
func (rt *Router) consumeFrom(ctx context.Context, addr string, req nodewire.ConsumeRequest, max int, timeout time.Duration) (nodewire.Response, bool, error) {
	legacy := rt.legacyBatchOwner(addr)
	if !legacy {
		req.Max = max
	}
	deadline := time.Now().Add(timeout)
	res, err := rt.peer.ConsumeWithin(ctx, addr, timeout, req)
	if err == nil && req.Max > 1 && isTrailingFieldRefusal(res) {
		rt.legacyBatchConsume.Store(addr, time.Now().Add(legacyClaimTTL))
		req.Max = 0
		left := time.Duration(0)
		if timeout > 0 {
			if left = time.Until(deadline); left <= 0 {
				return nodewire.Response{}, false, context.DeadlineExceeded
			}
		}
		res, err = rt.peer.ConsumeWithin(ctx, addr, left, req)
	}
	return res, req.Max > 1, err
}

// legacyBatchOwner reports whether consumes to addr must ask for one
// record, and forgets an entry whose TTL has passed so Max is tried
// again.
func (rt *Router) legacyBatchOwner(addr string) bool {
	v, ok := rt.legacyBatchConsume.Load(addr)
	if !ok {
		return false
	}
	if time.Now().Before(v.(time.Time)) {
		return true
	}
	rt.legacyBatchConsume.Delete(addr)
	return false
}

// consumeRemoteWait serves the wait phase of a queue-style long-poll on
// a node that owns none of the topic's partitions, after the opening
// probe found nothing, and writes the response.
//
// It parks the consumer on tokens exactly as an owning node does, with
// nothing local to race: an owner that gets a record notifies, and the
// consumer claims from it, two round trips after the record lands and
// no network at all while nothing does. It falls back to re-probing
// every owner on a backoff (longPollConsumeRemote) when this node has
// no return address for notifications, or when an owner recently
// refused a registration (a node on the previous release during a
// rolling upgrade), since a token left there is discarded and a record
// on it would wait out the whole budget.
//
// A max above 1 is a batch consume: every claim and probe asks for up
// to max records, and batch reports that what was written is the
// owner's {"messages":[...]} body.
func (rt *Router) consumeRemoteWait(ctx context.Context, w http.ResponseWriter, topicName string, wait time.Duration, max int) (batch bool) {
	if wait > 0 && rt.tokens.enabled() && !rt.tokens.pollingOwner(topicName) {
		return rt.waitOnTokens(ctx, w, topicName, wait, gatewayWait{reprobe: gatewayReprobeInterval}, max)
	}
	if forwarded, batch := rt.longPollConsumeRemote(ctx, w, topicName, wait, max); forwarded {
		return batch
	}
	w.WriteHeader(http.StatusNoContent)
	return false
}

// routeWaitPoll is how often awaitConsumeRoute checks the route table's
// versions. Each check is two atomic loads, and the window it covers
// (a topic created moments ago, the first seconds of a cold cluster) is
// about a second long.
const routeWaitPoll = 50 * time.Millisecond

// awaitConsumeRoute holds a queue-style long-poll for a topic that exists
// but has no partition this node can reach: none assigned yet (a topic
// created moments ago, the first seconds of a cold cluster) or every
// owner down. Answering 204 at once turned every looping consumer into
// a busy poll for as long as that lasted. It waits for the budget, or
// until the route table changes and gives the consumer somewhere to go:
// remote owners are then tried with what is left of the budget, and a
// partition that became this node's is answered 204 so the client's
// next poll takes the local path with its full wait.
//
// It reports whether it wrote the response. False leaves the request to
// the caller as before: no wait was asked for, or the topic is unknown
// (the caller answers 404) or has no partitions. max and batch are
// consumeRemoteWait's.
func (rt *Router) awaitConsumeRoute(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, max int) (handled, batch bool) {
	wait, err := consumeWaitFromHTTP(r, rt.maxConsumeWait)
	if err != nil || wait <= 0 {
		return false, false
	}
	if t, err := rt.store.GetTopic(ctx, topicName); err != nil || t.Partitions <= 0 {
		return false, false
	}
	deadline := time.Now().Add(wait)
	assignmentVersion, membersVersion := rt.store.AssignmentVersion(topicName), rt.store.RoutingMembersVersion()
	budget := time.NewTimer(wait)
	defer budget.Stop()
	poll := time.NewTicker(routeWaitPoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusNoContent)
			return true, false
		case <-budget.C:
			w.WriteHeader(http.StatusNoContent)
			return true, false
		case <-poll.C:
		}
		av, mv := rt.store.AssignmentVersion(topicName), rt.store.RoutingMembersVersion()
		if av == assignmentVersion && mv == membersVersion {
			continue
		}
		assignmentVersion, membersVersion = av, mv
		routes, ok := rt.routesForTopic(topicName)
		switch {
		case !ok:
			continue
		case len(routes.localEntries) > 0:
			w.WriteHeader(http.StatusNoContent)
			return true, false
		case !rt.hasRemoteOwner(topicName):
			continue
		}
		if forwarded, _, batch := rt.routeConsumeRemote(ctx, w, r, topicName, max); forwarded {
			return true, batch
		}
		return true, rt.consumeRemoteWait(ctx, w, topicName, time.Until(deadline), max)
	}
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
	return rt.probeCandidates(ctx, w, topicName, candidates, 0), true
}

// routeConsumeRemote is RouteConsumeRemote for a consume that takes up to
// max records (see probeBatchCandidates).
func (rt *Router) routeConsumeRemote(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, max int) (forwarded, hadCandidates, batch bool) {
	if max <= 1 {
		forwarded, hadCandidates = rt.RouteConsumeRemote(ctx, w, r, topicName)
		return forwarded, hadCandidates, false
	}
	candidates := rt.remoteConsumeCandidates(topicName)
	if len(candidates) == 0 {
		return false, false, false
	}
	forwarded, batch = rt.probeBatchCandidates(ctx, w, topicName, candidates, 0, max)
	return forwarded, true, batch
}

// RouteConsumeRemoteBatch is RouteConsumeRemote for a batch consume
// (?max=N) on a node that owns some of the topic's partitions, all of
// them empty: each remote owner is asked for up to max records instead
// of one, and the first that has any answers the whole request. batch
// reports that a 200 it wrote is already {"messages":[...]}. It is
// false for a single message the caller wraps: from an owner that does
// not take Max yet (asked again for one record, see consumeFrom), or
// for a max of 1. RouteConsumeRemote stays as it is, so a plain consume
// on an owning node pays nothing for this.
func (rt *Router) RouteConsumeRemoteBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, max int) (forwarded, batch bool) {
	forwarded, _, batch = rt.routeConsumeRemote(ctx, w, r, topicName, max)
	return forwarded, batch
}

// Re-probe pacing for longPollConsumeRemote. Each round costs one RPC per
// remote owner, so the interval trades delivery latency against probe
// QPS. The loop starts at remoteConsumeReprobeInterval, so a message
// that lands right after the initial probes is picked up quickly, and
// doubles after every empty round up to remoteConsumeReprobeMaxInterval,
// so an idle topic with many waiting clients settles at one round per
// second per client instead of four.
const (
	remoteConsumeReprobeInterval    = 100 * time.Millisecond
	remoteConsumeReprobeMaxInterval = time.Second
)

// longPollConsumeRemote honors a queue-style long-poll on a node that owns
// no partitions of the topic when the token protocol cannot (see
// consumeRemoteWait): it re-probes every remote owner, backing off
// between rounds, until a message materializes (response written, returns
// true), the wait budget expires, or the request context is done (returns
// false; the caller answers 204). Re-probing all owners each round is
// preferred over parking the whole wait on a single owner because a
// message can materialize on any owner. Each probe is non-blocking and
// individually bounded by consumeProbeTimeout, so unlike a pinned
// long-poll forward the loop needs no longWaitRPCContext-stretched
// deadline. The owner list is rebuilt only when the route table changes.
// max and batch are consumeRemoteWait's.
func (rt *Router) longPollConsumeRemote(ctx context.Context, w http.ResponseWriter, topicName string, wait time.Duration, max int) (forwarded, batch bool) {
	if wait <= 0 {
		return false, false
	}
	interval := rt.consumeReprobeInterval
	if interval <= 0 {
		interval = remoteConsumeReprobeInterval
	}
	maxInterval := rt.consumeReprobeMaxInterval
	if maxInterval < interval {
		maxInterval = interval
	}

	deadline := time.Now().Add(wait)
	var cache remoteCandidateCache
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
			return false, false
		}
		timer := time.NewTimer(min(interval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, false
		case <-timer.C:
		}
		forwarded, hadCandidates, batch := rt.reprobeRemoteUpTo(ctx, w, topicName, &cache, max)
		if forwarded {
			return true, batch
		}
		if !hadCandidates {
			// Ownership changed under us (e.g. a rebalance removed every
			// remote owner); nothing is left to poll against.
			return false, false
		}
		interval = min(interval*2, maxInterval)
	}
}

// ackForwardTimeout bounds a forwarded ack, extend, or nack. These are
// single-record bookkeeping calls on the owner, so 2s is generous; the
// transport's 5s no-deadline fallback let a stalled owner hold the
// client's HTTP request that long. Timing out is safe: acks are
// idempotent by nonce (the owner commits only if the handle's nonce
// still matches the active reservation), so a client retry after a
// timeout cannot double-commit a record. The bound is handed to the
// transport as the call's budget rather than derived as a
// context.WithTimeout per ack: the transport already runs a timer for
// the reply wait, and the derived context cost four allocations and a
// lock on the request's context for every forwarded ack.
const ackForwardTimeout = 2 * time.Second

// RouteAck forwards an ack request to the owner of the handle partition.
// Returns true if forwarded.
func (rt *Router) RouteAck(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	return rt.routeAckShaped(ctx, w, topicName, handle, nodewire.AckModeAck)
}

// RouteExtendAck forwards a visibility-window extension to the owner of
// the handle partition. Returns true if forwarded.
func (rt *Router) RouteExtendAck(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	return rt.routeAckShaped(ctx, w, topicName, handle, nodewire.AckModeExtend)
}

// RouteNack forwards an immediate reservation release to the owner of
// the handle partition. Returns true if forwarded.
func (rt *Router) RouteNack(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, handle consumer.Handle) bool {
	return rt.routeAckShaped(ctx, w, topicName, handle, nodewire.AckModeNack)
}

// routeAckShaped forwards an ack, extend or nack to the owner of the
// handle partition, through the owner's ack coalescer (see forwardAck).
func (rt *Router) routeAckShaped(ctx context.Context, w http.ResponseWriter, topicName string, handle consumer.Handle, mode nodewire.AckMode) bool {
	addr, unavailable := rt.ownerRoute(topicName, handle.Partition)
	if unavailable {
		writeOwnerDown(w)
		return true
	}
	if addr == "" {
		return false
	}
	res, err := rt.forwardAck(ctx, addr, nodewire.AckBatchItem{
		Topic:     topicName,
		Partition: handle.Partition,
		Offset:    handle.Offset,
		Nonce:     handle.Nonce,
		Mode:      mode,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}
