package cluster

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// forwardSettleTimeout bounds how long a follower waits, after a
// forwarded control-plane write succeeded on the leader, for its own
// replica to apply that write before answering the client. Replication
// normally takes one round trip; the bound only matters while this
// node is partitioned from the leader or far behind, and then the
// client still gets the leader's answer, just without the local
// read-your-writes guarantee.
const forwardSettleTimeout = 3 * time.Second

// settleForwardedWrite gives the client read-your-writes on a write
// this node forwarded to the leader: a 2xx from the leader means the
// entry is committed and applied THERE, but this replica applies it a
// replication round trip later, so a GET the client issues on the same
// connection could still see the old state (a schema update followed
// by a read of the schema, a topic create followed by a produce). The
// leader reports the index it had applied when the write completed and
// this node waits until its FSM has reached it.
//
// Every failure is non-fatal: the write already happened. A leader that
// predates the probe, lost leadership, or is unreachable, and a replica
// that cannot catch up within forwardSettleTimeout, all fall back to
// answering immediately, which is exactly the pre-probe behaviour.
func (rt *Router) settleForwardedWrite(ctx context.Context, memberAddr string, res nodewire.Response) {
	if rt.store == nil || res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return
	}
	settleCtx, cancel := context.WithTimeout(ctx, forwardSettleTimeout)
	defer cancel()
	index, err := rt.peer.AppliedIndex(settleCtx, memberAddr)
	if err != nil {
		return
	}
	_ = rt.store.WaitApplied(settleCtx, index)
}

// writeForwardedWrite is writeForwardResult for a write: it settles a
// successful forward on the local replica before answering.
func (rt *Router) writeForwardedWrite(ctx context.Context, w http.ResponseWriter, memberAddr string, res nodewire.Response, err error) bool {
	if err != nil {
		writeLeaderForwardError(w, err)
		return true
	}
	rt.settleForwardedWrite(ctx, memberAddr, res)
	writePeerResponse(w, res)
	return true
}

// SyncWithLeader returns once this node's replica has applied everything
// the leader had applied when asked, so a read that follows reflects
// every write the leader had finished. The HTTP ingress calls it before
// answering 404 for a topic its replica does not have (the topic may
// have just been created through another node): a topic still missing
// after it is missing on the leader too.
//
// On the leader it is the once-per-term leader barrier. Elsewhere it is
// the applied-index probe forwarded writes already use (every release
// since 2.2.0 answers it), bounded by forwardSettleTimeout. A leader that
// is unknown, unreachable, or not caught up within the bound is an error
// wrapping errs.ErrUnavailable (503: retry), never a silent pass.
func (rt *Router) SyncWithLeader(ctx context.Context) error {
	if rt.store == nil {
		return nil
	}
	if rt.store.IsLeader() {
		return rt.store.LeaderBarrier(ctx)
	}
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return fmt.Errorf("%w: no reachable leader to confirm the topic with", errs.ErrUnavailable)
	}
	syncCtx, cancel := context.WithTimeout(ctx, forwardSettleTimeout)
	defer cancel()
	index, err := rt.peer.AppliedIndex(syncCtx, memberAddr)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: ask the leader for its applied index: %v", errs.ErrUnavailable, err)
	}
	if err := rt.store.WaitApplied(syncCtx, index); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: catch up with the leader: %v", errs.ErrUnavailable, err)
	}
	return nil
}
