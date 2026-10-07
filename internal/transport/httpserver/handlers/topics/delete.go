package topics

import (
	"context"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Delete handles DELETE /v1/topics/{topic}. The request is forwarded to
// the leader, which deletes topic metadata locally and then asks other
// nodes to purge their local runtime and disk state for the topic.
//
// The metadata delete is the commit point. Once it stands the topic is
// gone for every client, so a failure purging files afterwards, local
// or on a remote member, is logged and answered 204 rather than 5xx:
// a 5xx would make the client retry a delete that already happened and
// get a 404, and the leftover directories are reclaimed by the owning
// node's startup orphan sweep either way. The forwarded (RPC) path has
// treated purge failures this way from the start; this keeps the
// leader-direct path consistent.
//
// The purge fan-out names the incarnation the delete removed and runs
// detached from the client's request: a client that disconnects while
// the leader purges must not cancel the purge on the other members. The
// router logs any member that still owes it.
//
// A delete of a remote child's stub, or of a parent with remote
// children, travels to the leader as a remote write: it is refused (409)
// while records are unshipped unless ?force=true abandons them, and then
// runs the same topic-manager delete (see children_remote.go).
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		force, ok := forceParam(s, w, r)
		if !ok {
			return
		}
		aw := newAuditWriter(w)
		w = aw
		var incarnation, requestID string
		defer func() {
			var extra []any
			if incarnation != "" {
				extra = append(extra, "incarnation", incarnation)
			}
			if requestID != "" {
				// The leader's topic.delete line carries the same one.
				extra = append(extra, "request_id", requestID)
			}
			if force {
				extra = append(extra, "force", true)
			}
			aw.audit(s, r, auditEventDelete, topicName, extra...)
		}()
		local, lookupErr := s.Deps.Broker.GetTopic(r.Context(), topicName)
		linked := lookupErr == nil && remoteLinked(r, s, local)
		if lookupErr == nil && local.IsRemoteChild() {
			if !authorizeStubDelete(s, w, r, local.Parent) {
				return
			}
		} else if !s.AuthorizeTopicManage(w, r, topicName) {
			return
		}
		if linked {
			// A remote-linked delete needs security on, as a stub's does
			// (authorizeStubDelete): a node with no identity never
			// abandons a link's unshipped records.
			if _, ok := handlers.Identity(r); !ok {
				s.WriteError(w, http.StatusForbidden, "remotes require security")
				return
			}
			if s.Deps.Remote.Writer == nil {
				s.WriteError(w, http.StatusNotImplemented, "remote children are not available on this node")
				return
			}
			requestID = remote.NewRequestID()
			deleteThroughLeader(s, w, r, topicName, force, requestID)
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDeleteTopic(r.Context(), w, r, topicName) {
				return
			}
		}
		// The purge fan-out names the incarnation the delete removed so a
		// member that already applied a recreate of the same name purges
		// the old directory, not the new one.
		id, err := deleteTopicReportingID(r.Context(), s, topicName)
		incarnation = id
		if err != nil {
			purgeErr, ok := errors.AsType[brokertopics.PurgeError](err)
			if !ok {
				s.WriteBrokerError(w, "delete topic", err)
				return
			}
			s.Deps.Logger.Warn("topic deleted but local purge failed; the startup orphan sweep reclaims the directory",
				"topic", topicName, "err", purgeErr.Err)
		}
		if s.Deps.Router != nil {
			// The router detaches the fan-out from the request and logs
			// the members that still owe the purge.
			_ = s.Deps.Router.BroadcastDeleteTopic(r.Context(), topicName, incarnation)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// deleteTopicReportingID deletes the topic and returns the incarnation
// ID the purge fan-out should name. A broker that reports the
// incarnation it deleted from under its name lock is asked for it, so an
// interleaved delete and recreate cannot make the fan-out name the wrong
// one; otherwise the ID is read before the delete, as before, and a
// failed read purges by name. The ID accompanies a PurgeError too: the
// metadata delete committed and the other members still have to purge.
func deleteTopicReportingID(ctx context.Context, s *handlers.Set, topicName string) (string, error) {
	if deleter, ok := s.Deps.Broker.(broker.TopicIDDeleter); ok {
		return deleter.DeleteTopicID(ctx, topicName)
	}
	var id string
	if t, err := s.Deps.Broker.GetTopic(ctx, topicName); err == nil {
		id = t.ID
	}
	return id, s.Deps.Broker.DeleteTopic(ctx, topicName)
}

// remoteLinked reports whether t is a remote child's stub or a parent
// with one, as this node's replica shows it.
func remoteLinked(r *http.Request, s *handlers.Set, t topic.Topic) bool {
	if t.IsRemoteChild() {
		return true
	}
	for _, name := range t.Children {
		if c, err := s.Deps.Broker.GetTopic(r.Context(), name); err == nil && c.IsRemoteChild() {
			return true
		}
	}
	return false
}
