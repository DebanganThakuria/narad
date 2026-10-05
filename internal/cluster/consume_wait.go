package cluster

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Cross-node consume: the token protocol.
//
// The shape this replaced parked a forwarded long-poll on ONE rotated
// remote owner. A record produced meanwhile to a partition owned by any
// other node stayed invisible for the client's whole wait, and the poll
// held a reservation on the owner it happened to pick, which then had to
// be given back whenever the local side won the race.
//
// Now this node leaves a token with EVERY remote owner, concurrently.
// A token reserves nothing, so an owner that never hears back strands no
// record and there is nothing to give back. When an owner gets a record
// it spends one token to call here; a parked consumer is woken with that
// owner's address and claims with an ordinary non-blocking consume.
//
// The claim is the only call that reserves, and it is aimed at exactly
// one node. Losing the race for it costs one round trip: the record
// stays available, the consumer stays parked, and it re-registers.

// LocalConsumeWaiter is the handler's side of the race (see
// topic.LocalConsumeWaiter): Wait is a broker ConsumeWait, Release a
// broker Nack.
type LocalConsumeWaiter = topic.LocalConsumeWaiter

// RouteConsumeWait serves the wait phase of a queue-style consume:
// the local wait raced against the token protocol.
//
// Instead of parking a forwarded long-poll on one rotated owner, this
// leaves a token with EVERY remote owner, concurrently, and parks. A
// token reserves nothing on those nodes, so an owner that never hears
// back strands no record. When one of them gets a record it calls back,
// this consumer is woken with that owner's address, and it claims with
// an ordinary non-blocking consume.
//
// It writes the response and returns true when it handled the request;
// false when there is nothing to wait on remotely, in which case the
// caller runs the local wait alone.
func (rt *Router) RouteConsumeWait(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, wait time.Duration, local LocalConsumeWaiter) bool {
	if wait <= 0 || local == nil {
		return false
	}
	if !rt.tokens.enabled() || !rt.hasRemoteOwner(topicName) {
		return false
	}
	rt.waitOnTokens(ctx, w, topicName, wait, local, 0)
	return true
}

// RouteConsumeWaitBatch is RouteConsumeWait for a batch consume
// (?max=N): the claim an owner's notification triggers asks for up to
// max records, so a consumer parked on a node that owns some of the
// topic's partitions is served the owner's batch rather than one
// record. handled means what RouteConsumeWait's return does. batch
// reports that a 200 it wrote is already {"messages":[...]}; it is false
// for a single message the caller wraps or tops up: one the local
// waiter delivered, or one from an owner that does not take Max yet
// (asked again for one record, see claimUpTo).
func (rt *Router) RouteConsumeWaitBatch(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, wait time.Duration, local LocalConsumeWaiter, max int) (handled, batch bool) {
	if wait <= 0 || local == nil {
		return false, false
	}
	if !rt.tokens.enabled() || !rt.hasRemoteOwner(topicName) {
		return false, false
	}
	return true, rt.waitOnTokens(ctx, w, topicName, wait, local, max)
}

// waitOnTokens parks the consumer on this node's tokens, races that
// against local's wait, and writes the response. A max above 1 is a
// batch consume: the claim and the re-probe ask the owner for up to max
// records, and batch reports that what was written is the owner's
// {"messages":[...]} body rather than one message.
func (rt *Router) waitOnTokens(ctx context.Context, w http.ResponseWriter, topicName string, wait time.Duration, local LocalConsumeWaiter, max int) (batch bool) {
	if wait > rt.maxConsumeWait && rt.maxConsumeWait > 0 {
		wait = rt.maxConsumeWait
	}
	deadline := time.Now().Add(wait)

	parked, unpark := rt.tokens.park(topicName, deadline)
	defer unpark()
	// Every owner is told at once: one round trip total, not one each.
	rt.tokens.register(ctx, topicName, wait)

	// leave takes the consumer off the queue before it answers, so no
	// notification lands on it from here on, and passes on one that
	// already did: the owner holds that record for a claim, and a
	// consumer that leaves without claiming would waste the hold.
	leave := func() {
		unpark()
		if from := parked.take(); from != "" {
			rt.tokens.WakeOneWaiter(topicName, from)
		}
	}

	// One goroutine, not two. The local wait and the cross-node
	// notification are folded into a single select inside the broker, so
	// a parked consumer costs one goroutine rather than one for the
	// request plus one racing it. With thousands parked on a gateway
	// node that difference is most of the per-waiter cost.
	var owners remoteCandidateCache
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			leave()
			w.WriteHeader(http.StatusNoContent)
			return false
		}
		msg, found, wokeExternal, err := local.Wait(ctx, remaining, parked.ch)

		switch {
		case found && err == nil:
			// The local partitions had it. The tokens left with the
			// owners are shared by every consumer parked here for the
			// topic and stay for the others, or lapse (see register).
			leave()
			writeConsumeMessage(w, msg)
			return false

		case wokeExternal:
			from := parked.take()
			if res, isBatch, ok := rt.claimUpTo(ctx, from, topicName, max); ok {
				// The owner spent this node's token on us. If others are
				// still parked here, leave it a fresh one so its next
				// record reaches them too.
				if remaining, others := rt.tokens.othersParked(topicName, parked); others {
					rt.tokens.registerAt(ctx, topicName, from, remaining)
				}
				writePeerResponse(w, res)
				return isBatch
			}
			// Someone beat us to it. Being woken took this consumer off
			// the queue, so put it back before re-registering, or the
			// next offer would find nobody listening.
			rt.tokens.repark(topicName, parked)
			rt.tokens.register(ctx, topicName, time.Until(deadline))

		case errors.Is(err, errReprobeOwners):
			// A node that owns none of the topic's partitions scans the
			// owners now and then while parked (see gatewayWait).
			forwarded, hadOwners, isBatch := rt.reprobeRemoteUpTo(ctx, w, topicName, &owners, max)
			if forwarded {
				leave()
				return isBatch
			}
			if !hadOwners {
				// Ownership moved under us; the next poll routes afresh.
				leave()
				w.WriteHeader(http.StatusNoContent)
				return false
			}

		case err != nil && !errors.Is(err, context.Canceled):
			leave()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false

		default:
			// Budget spent, or the client left.
			leave()
			w.WriteHeader(http.StatusNoContent)
			return false
		}
	}
}

// gatewayReprobeInterval is how often a consumer parked on a node that
// owns none of the topic's partitions scans the owners anyway. The
// token is its only wake, so a notification lost on the way, or a token
// an owner silently dropped (a restart, a lagging assignment view),
// would otherwise hold a record back until the keeper's refresh, up to
// registrationRefresh later. The polling this replaced scanned every
// owner at least once a second for the whole wait.
const gatewayReprobeInterval = 2 * time.Second

// errReprobeOwners is gatewayWait's signal that a re-probe is due.
var errReprobeOwners = errors.New("cluster: re-probe the remote owners")

// gatewayWait is the local side of the race on a node that owns none of
// the topic's partitions. There is nothing local to find, so its wait
// ends on the owner's notification, the client leaving, or the budget,
// and every reprobe it hands back errReprobeOwners so the router scans
// the owners once.
type gatewayWait struct{ reprobe time.Duration }

func (g gatewayWait) Wait(ctx context.Context, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	var due error
	if g.reprobe > 0 && wait > g.reprobe {
		wait, due = g.reprobe, errReprobeOwners
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-external:
		return topic.Message{}, false, true, nil
	case <-ctx.Done():
		return topic.Message{}, false, false, nil
	case <-timer.C:
		return topic.Message{}, false, false, due
	}
}

// Release is never called: a gateway wait delivers nothing.
func (gatewayWait) Release(context.Context, topic.Message) error { return nil }

// claimFrom takes the record an owner said it had. A 204 means somebody
// else claimed it first, which costs one round trip and nothing else:
// the consumer stays parked and the record stays available.
func (rt *Router) claimFrom(ctx context.Context, addr, topicName string) (nodewire.Response, bool) {
	res, _, ok := rt.claimUpTo(ctx, addr, topicName, 0)
	return res, ok
}

// claimUpTo is claimFrom for a consume that takes up to max records: a
// max above 1 asks the owner for a batch, as the probes of a batch
// consume do, and batch reports that the reply is one.
func (rt *Router) claimUpTo(ctx context.Context, addr, topicName string, max int) (res nodewire.Response, batch, ok bool) {
	// One budget covers the claim and a legacy owner's retries.
	deadline := time.Now().Add(consumeProbeTimeout)
	req := nodewire.ConsumeRequest{Topic: topicName, LocalOnly: true, Claim: !rt.legacyOwner(addr)}
	if max > 1 && !rt.legacyBatchOwner(addr) {
		req.Max = max
	}
	res, err := rt.peer.ConsumeWithin(ctx, addr, consumeProbeTimeout, req)
	for err == nil && (req.Claim || req.Max > 1) && IsTrailingFieldRefusal(res) {
		// An owner on an earlier release rejects a trailing field it does
		// not know outright, and the reply does not say which. Drop the
		// newest first: Max, then the Claim byte, which leaves the plain
		// probe every release understands (it reserves the record just
		// the same, the hold merely runs to its deadline). Each refusal
		// is remembered for a while, so the roll costs one refused claim
		// per owner per TTL, not one per record.
		if req.Max > 1 {
			rt.legacyBatchConsume.Store(addr, time.Now().Add(legacyClaimTTL))
			req.Max = 0
		} else {
			rt.legacyClaim.Store(addr, time.Now().Add(legacyClaimTTL))
			req.Claim = false
		}
		left := time.Until(deadline)
		if left <= 0 {
			err = context.DeadlineExceeded
			break
		}
		res, err = rt.peer.ConsumeWithin(ctx, addr, left, req)
	}
	if err != nil || res.Status != http.StatusOK {
		return nodewire.Response{}, false, false
	}
	return res, req.Max > 1, true
}

// legacyClaimTTL is how long a claim keeps going out unflagged to an
// owner that refused the flag. Long enough that a roll costs one refused
// claim per owner per TTL, short enough that an owner upgraded mid-roll
// is back on the fast path in minutes.
const legacyClaimTTL = 2 * time.Minute

// legacyOwner reports whether claims to addr must go out unflagged, and
// forgets an entry whose TTL has passed so the flag is tried again.
func (rt *Router) legacyOwner(addr string) bool {
	v, ok := rt.legacyClaim.Load(addr)
	if !ok {
		return false
	}
	if time.Now().Before(v.(time.Time)) {
		return true
	}
	rt.legacyClaim.Delete(addr)
	return false
}

// writeConsumeMessage encodes a locally delivered message the way the
// HTTP handler does.
func writeConsumeMessage(w http.ResponseWriter, msg topic.Message) {
	body := msg.AppendJSON(make([]byte, 0, len(msg.Payload)+128))
	body = append(body, '\n')
	setContentHeaders(w.Header(), nodewire.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
