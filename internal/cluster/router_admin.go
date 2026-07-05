package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

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
	res, err := rt.peer.CreateTopic(createCtx, memberAddr, body)
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
	res, err := rt.peer.DeleteTopic(deleteCtx, memberAddr, topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return true
	}
	writePeerResponse(w, res)
	return true
}

// BroadcastDeleteTopic asks every live member (except this node) to purge the
// deleted topic's on-disk state. The returned error joins every member that
// failed; a nil error means all live members purged.
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
		// A purge legitimately waits up to purgeApplyWaitTimeout on the
		// remote for its replica to reflect the deletion, and only THEN
		// starts the purge work itself — which can take multiple seconds
		// for a topic with many partition directories. Budget both phases
		// (plus longWaitRPCContext's transfer grace) so the leader's
		// deadline doesn't expire on a purge that is about to succeed,
		// turning a completed delete into a 500 whose retry then 404s.
		purgeCtx, cancel := longWaitRPCContext(ctx, purgeApplyWaitTimeout+purgeExecutionAllowance)
		res, err := rt.peer.PurgeTopic(purgeCtx, member.Addr, topicName)
		cancel()
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
