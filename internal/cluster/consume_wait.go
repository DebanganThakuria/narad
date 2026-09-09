package cluster

import (
	"context"
	"errors"
	"log/slog"
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
	if !rt.tokens.enabled() || len(rt.remoteConsumeCandidates(topicName)) == 0 {
		return false
	}
	if wait > rt.maxConsumeWait && rt.maxConsumeWait > 0 {
		wait = rt.maxConsumeWait
	}
	deadline := time.Now().Add(wait)

	parked, unpark := rt.tokens.park(topicName)
	defer unpark()
	// Every owner is told at once: one round trip total, not one each.
	rt.tokens.register(ctx, topicName, wait)

	// One goroutine, not two. The local wait and the cross-node
	// notification are folded into a single select inside the broker, so
	// a parked consumer costs one goroutine rather than one for the
	// request plus one racing it. With thousands parked on a gateway
	// node that difference is most of the per-waiter cost.
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			w.WriteHeader(http.StatusNoContent)
			return true
		}
		msg, found, wokeExternal, err := local.Wait(ctx, remaining, parked.ch)

		switch {
		case found && err == nil:
			// The local partitions had it. Retire the tokens left
			// elsewhere so those owners do not spend a notification on a
			// consumer that has been served.
			rt.tokens.drop(ctx, topicName, "")
			writeConsumeMessage(w, msg)
			return true

		case wokeExternal:
			from := parked.take()
			if res, ok := rt.claimFrom(ctx, from, topicName); ok {
				rt.tokens.drop(ctx, topicName, from)
				writePeerResponse(w, res)
				return true
			}
			// Someone beat us to it. Being woken took this consumer off
			// the queue, so put it back before re-registering, or the
			// next offer would find nobody listening.
			rt.tokens.repark(topicName, parked)
			rt.tokens.register(ctx, topicName, time.Until(deadline))

		case err != nil && !errors.Is(err, context.Canceled):
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return true

		default:
			// Budget spent, or the client left.
			w.WriteHeader(http.StatusNoContent)
			return true
		}
	}
}

// claimFrom takes the record an owner said it had. A 204 means somebody
// else claimed it first, which costs one round trip and nothing else:
// the consumer stays parked and the record stays available.
func (rt *Router) claimFrom(ctx context.Context, addr, topicName string) (nodewire.Response, bool) {
	claimCtx, cancel := context.WithTimeout(ctx, consumeProbeTimeout)
	defer cancel()
	res, err := rt.peer.Consume(claimCtx, addr, nodewire.ConsumeRequest{Topic: topicName, LocalOnly: true})
	if err != nil || res.Status != http.StatusOK {
		return nodewire.Response{}, false
	}
	return res, true
}

// writeConsumeMessage encodes a locally delivered message the way the
// HTTP handler does.
func writeConsumeMessage(w http.ResponseWriter, msg topic.Message) {
	body := msg.AppendJSON(make([]byte, 0, len(msg.Payload)+128))
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (rt *Router) logConsumeWait(what, topicName string, err error) {
	slog.Default().Warn(what, "topic", topicName, "err", err)
}
