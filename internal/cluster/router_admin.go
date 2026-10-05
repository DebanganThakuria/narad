package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/security"
)

// writeLeaderForwardError answers a failed control-plane forward to the
// leader with 503, not 502: the leader was momentarily unreachable
// (election, partition, rolling restart), which is retryable — not a
// bad gateway the client should treat as broken.
func writeLeaderForwardError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusServiceUnavailable)
}

// forwardActor is the caller a forwarded topic write is made for: the
// authenticated user of the request, or "" when security is off. The
// leader looks it up in its own replica and re-checks that user's
// rights (audit H1).
func forwardActor(ctx context.Context) string {
	if id, ok := security.IdentityFrom(ctx); ok {
		return id.Username
	}
	return ""
}

// createForwardTimeout bounds a follower's create forward to the cluster
// leader. The leader legitimately parks a create on its armed startup
// create gate while startup reconciliation is still running — up to the
// ~60s metastore catch-up cap (startupReconcileCaughtUpTimeout in
// cmd/narad) plus the sweep work itself — so this sits above that window.
// Without an explicit deadline the transport's short default reply timeout
// fires: the client gets a 502 while the create still executes on the
// leader, and the retry then hits a 409.
const createForwardTimeout = 75 * time.Second

// RouteCreateTopic forwards a topic create request to the cluster leader.
func (rt *Router) RouteCreateTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, body []byte) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	createCtx, cancel := longWaitRPCContext(ctx, createForwardTimeout)
	defer cancel()
	res, err := rt.peer.CreateTopic(createCtx, memberAddr, body, forwardActor(ctx))
	return rt.writeForwardedWrite(ctx, w, memberAddr, res, err)
}

// RouteAlterTopic forwards a topic alter request to the cluster leader.
func (rt *Router) RouteAlterTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string, body []byte) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	res, err := rt.peer.AlterTopic(ctx, memberAddr, topicName, body, forwardActor(ctx))
	return rt.writeForwardedWrite(ctx, w, memberAddr, res, err)
}

// deleteTopicForwardTimeout bounds a follower's delete forward to the
// leader. The leader synchronously fans the purge out to every member and
// each purge can wait several seconds for a lagging replica, so this sits
// deliberately far above the transport's default reply timeout.
const deleteTopicForwardTimeout = 30 * time.Second

// RouteDeleteTopic forwards a topic delete request to the cluster leader.
func (rt *Router) RouteDeleteTopic(ctx context.Context, w http.ResponseWriter, _ *http.Request, topicName string) bool {
	memberAddr := rt.leaderMemberAddr()
	if memberAddr == "" {
		return false
	}
	deleteCtx, cancel := longWaitRPCContext(ctx, deleteTopicForwardTimeout)
	defer cancel()
	res, err := rt.peer.DeleteTopic(deleteCtx, memberAddr, topicName, forwardActor(ctx))
	return rt.writeForwardedWrite(ctx, w, memberAddr, res, err)
}

// BroadcastDeleteTopic asks every live member (except this node) to purge the
// deleted topic incarnation's on-disk state (id is the deleted record's
// topic.Topic.ID; it lets a member that already applied a recreate of
// the same name purge the old directory without touching the new one).
// The purges run concurrently under ONE
// shared deadline, so the whole fan-out costs the slowest member, not the
// sum of every member: sequential purges on a five-node cluster with two
// slow members overran the 30s the forwarding follower waits (and the
// client's own patience), turning an already-committed delete into a 503
// whose retry then 404s. The returned error joins every member that
// failed; a nil error means all live members purged.
//
// The fan-out is detached from ctx's cancellation and deadline (audit
// M8): the delete it follows has already committed, so a client that
// disconnects or times out must not cancel the purge on every other
// member, which then kept the deleted topic's files until it restarted.
// It runs under its own bounded budget instead (purgeBroadcastBudget).
// A member whose replica has not applied the delete yet answers
// purge_deferred, and one that cannot be reached fails in transport;
// each is asked again with a backoff, at most purgeMaxAttempts times in
// all, inside the same budget. Members that still owe the purge at the
// end are logged once, at error, with the topic, the incarnation and
// their IDs: their copies stay until their startup orphan sweep.
func (rt *Router) BroadcastDeleteTopic(ctx context.Context, topicName, id string) error {
	members, err := rt.store.ListMembers()
	if err != nil {
		rt.logger.Error("topic purge fan-out could not list the members; their copies stay until their startup orphan sweep reclaims them",
			"topic", topicName, "incarnation", id, "err", err)
		return err
	}
	// One budget for the lot: a purge legitimately waits up to
	// purgeApplyWaitTimeout on the remote for its replica to reflect the
	// deletion, and only THEN starts the purge work itself, which can take
	// multiple seconds for a topic with many partition directories. Budget
	// both phases (plus the transfer grace) so the deadline doesn't expire
	// on a purge that is about to succeed.
	purgeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), purgeBroadcastBudget)
	defer cancel()

	// Attempt every live member even if one fails: a single unreachable
	// peer must not stop the others from purging. Any member we miss is
	// reclaimed by its startup orphan sweep.
	var targets []metastore.Member
	for _, member := range members {
		if member.Status == metastore.MemberDead || strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) {
			continue
		}
		targets = append(targets, member)
	}
	results := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, member := range targets {
		wg.Go(func() {
			results[i] = rt.purgeMember(purgeCtx, member, topicName, id)
		})
	}
	wg.Wait()

	var owing []string
	for i, err := range results {
		if err != nil {
			owing = append(owing, targets[i].ID)
		}
	}
	// Joined in member order so the message is stable regardless of
	// which purge finished first.
	joined := errors.Join(results...)
	if joined != nil {
		rt.logger.Error("topic purge unfinished on some members; their copies stay until their startup orphan sweep reclaims them",
			"topic", topicName, "incarnation", id, "members", owing, "err", joined)
	}
	return joined
}

// purgeBroadcastBudget bounds the whole purge fan-out of one delete: the
// remote's replica apply wait, the purge work itself and the transfer
// grace. Retries of a deferred or unreachable member happen inside it.
const purgeBroadcastBudget = purgeApplyWaitTimeout + purgeExecutionAllowance + longWaitRPCGrace

// purgeMaxAttempts caps how many times the fan-out asks one member to
// purge, and purgeRetryBackoff is the pause before the second attempt
// (doubled before each later one).
const (
	purgeMaxAttempts  = 3
	purgeRetryBackoff = 250 * time.Millisecond
)

// purgeMember asks one member to purge the deleted incarnation and
// returns why it did not, if it did not. A member that answered
// purge_deferred (its replica had not applied the delete) or could not be
// reached is asked again after a backoff while attempts and the budget
// last. Any other answer is final: a 2xx is a purge that ran (a 3.0.x
// member answers 204 also when it skipped, as it always has), and any
// other status is a refusal another attempt would not change.
func (rt *Router) purgeMember(ctx context.Context, member metastore.Member, topicName, id string) error {
	backoff := purgeRetryBackoff
	for attempt := 1; ; attempt++ {
		res, err := rt.peer.PurgeTopic(ctx, member.Addr, topicName, id)
		var failure error
		switch {
		case err != nil:
			failure = fmt.Errorf("purge %s on %s: %w", topicName, member.ID, err)
		case res.Status >= http.StatusOK && res.Status < http.StatusMultipleChoices:
			return nil
		case purgeDeferred(res):
			failure = fmt.Errorf("purge %s deferred by %s: its replica had not applied the delete", topicName, member.ID)
		default:
			return fmt.Errorf("purge %s returned status %d for %s", topicName, res.Status, member.ID)
		}
		if attempt >= purgeMaxAttempts {
			return failure
		}
		select {
		case <-ctx.Done():
			return failure
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// purgeDeferredCode is the code of a purge a member deferred because its
// replica still showed the incarnation (see handlePurgeTopic).
const purgeDeferredCode = "purge_deferred"

// purgeDeferred reports whether res is a member's purge_deferred answer.
func purgeDeferred(res nodewire.Response) bool {
	if res.Status != http.StatusServiceUnavailable {
		return false
	}
	var body struct {
		Code string `json:"code"`
	}
	return json.Unmarshal(res.Body, &body) == nil && body.Code == purgeDeferredCode
}
