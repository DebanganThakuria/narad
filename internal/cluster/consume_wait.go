package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Cross-node long-poll on an owner node.
//
// A queue-style consume on a node that owns some of the topic's
// partitions probes locally, probes every remote owner once, and then
// parks on the local wake-up channel for the client's wait. A message
// produced meanwhile to a partition owned by ANOTHER node was only seen
// when that wait expired and the client re-polled. RouteConsumeWait
// races the local wait against one forwarded long-poll
// (ConsumeRequest{LocalOnly: true, WaitNanos: remaining}) to a rotated
// remote owner, takes whichever delivers first, and gives the loser's
// delivery back:
//
//   - the local wait wins: the remote RPC's context is cancelled, which
//     sends the cluster RPC cancel frame; a remote owner that already
//     reserved a message for that request nacks it itself
//     (HandleStreamCancel), and any reply that still arrives with a
//     message is nacked explicitly here;
//   - the remote reply wins: the local wait's context is cancelled; if
//     the local path had reserved a message in the meantime it is
//     released through the waiter's Release.
//
// Goroutines are bounded: one extra goroutine per waiting client, and
// at most maxRemoteLongPolls of them per process; beyond that the
// request falls back to the local-only wait.

// LocalConsumeWaiter is the handler's side of the race (see
// topic.LocalConsumeWaiter): Wait is a broker ConsumeWait, Release a
// broker Nack.
type LocalConsumeWaiter = topic.LocalConsumeWaiter

// maxRemoteLongPolls bounds the goroutines parked on forwarded
// long-polls across all waiting clients.
const maxRemoteLongPolls = 4096

// remoteLongPollGrace is added to the forwarded wait for the RPC
// deadline, covering transfer and scan overhead.
const remoteLongPollGrace = longWaitRPCGrace

type remoteWaitResult struct {
	addr string
	res  nodewire.Response
	err  error
}

// remoteLongPollSlots is the process-wide goroutine bound for forwarded
// long-polls. Lazily sized to runtime.GOMAXPROCS-independent
// maxRemoteLongPolls.
var (
	remoteLongPollSlotsOnce sync.Once
	remoteLongPollSlots     chan struct{}
)

func remoteLongPollSlot() (release func(), ok bool) {
	remoteLongPollSlotsOnce.Do(func() {
		n := maxRemoteLongPolls
		if n < runtime.GOMAXPROCS(0) {
			n = runtime.GOMAXPROCS(0)
		}
		remoteLongPollSlots = make(chan struct{}, n)
	})
	select {
	case remoteLongPollSlots <- struct{}{}:
		return func() { <-remoteLongPollSlots }, true
	default:
		return nil, false
	}
}

// RouteConsumeWait serves the wait phase of a queue-style consume on a
// node that owns partitions of the topic: the local wait raced against a
// forwarded long-poll to one rotated remote owner. It writes the
// response and returns true when it handled the request; false when
// there is no remote owner to ask or no goroutine slot is free, in
// which case the caller runs the local wait alone.
func (rt *Router) RouteConsumeWait(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, wait time.Duration, local LocalConsumeWaiter) bool {
	if wait <= 0 || local == nil {
		return false
	}
	candidates := rt.remoteConsumeCandidates(topicName)
	if len(candidates) == 0 {
		return false
	}
	release, ok := remoteLongPollSlot()
	if !ok {
		return false
	}
	defer release()
	if wait > rt.maxConsumeWait && rt.maxConsumeWait > 0 {
		wait = rt.maxConsumeWait
	}

	localCtx, cancelLocal := context.WithCancel(ctx)
	defer cancelLocal()
	remoteCtx, cancelRemote := context.WithCancel(ctx)
	defer cancelRemote()

	results := make(chan remoteWaitResult, 1)
	deadline := time.Now().Add(wait)
	go rt.remoteLongPoll(remoteCtx, cancelLocal, topicName, candidates, deadline, results)

	msg, found, err := local.Wait(localCtx, wait)
	// The local side wins only if it delivered BEFORE anyone cancelled
	// it: a message it reserved after the remote reply won (localCtx
	// already cancelled) or after the client left is the loser's and
	// must be given back.
	localWon := found && err == nil && localCtx.Err() == nil
	if localWon {
		// Stop the remote poll; a reply that beat the cancel is given back.
		cancelRemote()
	}
	remote := <-results

	if localWon {
		rt.releaseRemoteDelivery(ctx, topicName, remote)
		writeConsumeMessage(w, msg)
		return true
	}
	if found && err == nil {
		if rerr := local.Release(ctx, msg); rerr != nil && !errors.Is(rerr, consumer.ErrHandleStale) {
			rt.logConsumeWait("release local delivery that lost the wait race", topicName, rerr)
		}
	}
	if remote.err == nil && remote.res.Status == http.StatusOK {
		writePeerResponse(w, remote.res)
		return true
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return true
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

// remoteLongPoll forwards a local-only long-poll to the candidates in
// turn (each with the remaining budget) until one delivers, the budget
// is spent, or ctx ends. A delivery cancels the local wait through
// cancelLocal; the result is always sent exactly once.
func (rt *Router) remoteLongPoll(ctx context.Context, cancelLocal context.CancelFunc, topicName string, candidates []string, deadline time.Time, results chan<- remoteWaitResult) {
	var last remoteWaitResult
	for i := 0; ctx.Err() == nil; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		addr := candidates[i%len(candidates)]
		req := nodewire.ConsumeRequest{Topic: topicName, LocalOnly: true, WaitNanos: int64(remaining)}
		rpcCtx, cancel := context.WithTimeout(ctx, remaining+remoteLongPollGrace)
		res, err := rt.peer.Consume(rpcCtx, addr, req)
		cancel()
		last = remoteWaitResult{addr: addr, res: res, err: err}
		if err == nil && res.Status == http.StatusOK {
			cancelLocal()
			break
		}
		if err != nil && ctx.Err() != nil {
			break // cancelled by the local win
		}
		if i+1 >= len(candidates) && time.Until(deadline) > remoteConsumeReprobeInterval {
			// Every owner answered empty early (a shorter wait ceiling on
			// their side): pace the next round like the re-probe loop.
			if !sleepCtx(ctx, remoteConsumeReprobeInterval) {
				break
			}
		}
	}
	results <- last
}

// releaseRemoteDelivery nacks a message a remote owner delivered for a
// wait the local side already won. A cancelled RPC carries no message
// (and the owner's cancel handling already released anything it
// reserved), so only a completed 200 needs the explicit nack.
func (rt *Router) releaseRemoteDelivery(ctx context.Context, topicName string, remote remoteWaitResult) {
	if remote.err != nil || remote.res.Status != http.StatusOK {
		return
	}
	var msg struct {
		Partition     int    `json:"partition"`
		ReceiptHandle string `json:"receipt_handle"`
	}
	if err := json.Unmarshal(remote.res.Body, &msg); err != nil || msg.ReceiptHandle == "" {
		return
	}
	h, err := consumer.DecodeHandle(msg.ReceiptHandle)
	if err != nil {
		return
	}
	nackCtx, cancel := ackForwardContext(context.WithoutCancel(ctx))
	defer cancel()
	if _, err := rt.peer.Nack(nackCtx, remote.addr, nodewire.AckRequest{
		Topic: topicName, Partition: h.Partition, Offset: h.Offset, Nonce: h.Nonce,
	}); err != nil {
		rt.logConsumeWait("nack late remote delivery after local win", topicName, err)
	}
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
