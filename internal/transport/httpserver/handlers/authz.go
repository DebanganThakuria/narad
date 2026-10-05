package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// Authorization happens at the HTTP ingress node, before any routing,
// against this node's replica. A topic write forwarded to the leader
// also carries the caller, and the leader re-checks ownership under the
// topic's lock against its own records (audit H1); the ingress check
// answers early and keeps a request naming a topic this node has never
// seen from reaching the leader at all. When security is disabled there
// is no identity on the context and every check passes.

// Authorize reports whether the request may perform action on the named
// topic, writing a 403 when it may not.
func (s *Set) Authorize(w http.ResponseWriter, r *http.Request, action user.Action, topicName string) bool {
	id, ok := security.IdentityFrom(r.Context())
	if !ok || id.Allowed(action, topicName) {
		return true
	}
	s.WriteError(w, http.StatusForbidden, string(action)+" not allowed on this topic")
	return false
}

// RequireAdmin returns the calling user when they hold the admin action,
// writing a 403 otherwise. When security is disabled there is no
// identity on the request; that is treated as full access (dev mode),
// and a synthetic root user is returned so no-escalation checks pass.
func (s *Set) RequireAdmin(w http.ResponseWriter, r *http.Request) (user.User, bool) {
	caller, ok := security.IdentityFrom(r.Context())
	if !ok {
		return user.User{Root: true}, true
	}
	if caller.IsAdmin() {
		return caller, true
	}
	s.WriteError(w, http.StatusForbidden, "admin privileges required")
	return user.User{}, false
}

// LeaderSyncer is the Router capability behind the missing-topic check:
// SyncWithLeader returns once this node's replica has applied everything
// the leader had applied when asked (nil at once on the leader, after
// its once-per-term barrier), or fails wrapping errs.ErrUnavailable when
// the leader is unknown or unreachable.
type LeaderSyncer interface {
	SyncWithLeader(ctx context.Context) error
}

// canManageTopic reports whether the caller may alter, delete, or attach
// the named topic: security disabled, admin, or topic owner. A topic
// this node's replica does not have is looked for again once the node
// has caught up with the leader (the topic may have just been created
// elsewhere); still missing, it is an errs.ErrTopicNotFound error (404),
// never a pass, so a request racing a create, or read from a lagging
// replica, is not forwarded as allowed (audit H1). A failed catch-up is
// an errs.ErrUnavailable error (503). Admins are not looked up.
func (s *Set) canManageTopic(r *http.Request, topicName string) (bool, error) {
	id, ok := security.IdentityFrom(r.Context())
	if !ok || id.IsAdmin() {
		return true, nil
	}
	t, err := s.Deps.Broker.GetTopic(r.Context(), topicName)
	if isTopicMissing(err) {
		t, err = s.getTopicAfterLeaderSync(r.Context(), topicName)
	}
	if isTopicMissing(err) {
		return false, fmt.Errorf("%w: %q", errs.ErrTopicNotFound, topicName)
	}
	if err != nil {
		return false, err
	}
	return t.Owner == id.Username, nil
}

// getTopicAfterLeaderSync re-reads a topic once this node has caught up
// with the leader. Without a router (single node) the local replica is
// the only one, so the first read stands.
func (s *Set) getTopicAfterLeaderSync(ctx context.Context, topicName string) (topic.Topic, error) {
	syncer, ok := s.Deps.Router.(LeaderSyncer)
	if !ok {
		return topic.Topic{}, errs.ErrTopicNotFound
	}
	if err := syncer.SyncWithLeader(ctx); err != nil {
		return topic.Topic{}, err
	}
	return s.Deps.Broker.GetTopic(ctx, topicName)
}

func isTopicMissing(err error) bool {
	return errors.Is(err, errs.ErrTopicNotFound) || errors.Is(err, errs.ErrNotFound)
}

// AuthorizeTopicManage enforces the owner-or-admin rule for altering or
// deleting a topic, writing a 403 when the caller is neither, a 404 when
// the topic does not exist once this node has caught up with the
// leader, and a 503 when it could not catch up.
func (s *Set) AuthorizeTopicManage(w http.ResponseWriter, r *http.Request, topicName string) bool {
	ok, err := s.canManageTopic(r, topicName)
	if err != nil {
		s.WriteBrokerError(w, "authorize topic", err)
		return false
	}
	if ok {
		return true
	}
	s.WriteError(w, http.StatusForbidden, "only the topic owner or an admin may modify this topic")
	return false
}

// AuthorizeTopicManageAny enforces the owner-or-admin rule on at least
// one of the named topics: detaching a child is something either side of
// the link may do, so the parent's owner and the child's owner both
// qualify. Writes a 403 when the caller manages neither, and a 404 only
// when none of the topics exists (see canManageTopic).
func (s *Set) AuthorizeTopicManageAny(w http.ResponseWriter, r *http.Request, topicNames ...string) bool {
	missing := 0
	var missingErr error
	for _, name := range topicNames {
		ok, err := s.canManageTopic(r, name)
		if isTopicMissing(err) {
			missing++
			missingErr = err
			continue
		}
		if err != nil {
			s.WriteBrokerError(w, "authorize topic", err)
			return false
		}
		if ok {
			return true
		}
	}
	if missing == len(topicNames) && missingErr != nil {
		s.WriteBrokerError(w, "authorize topic", missingErr)
		return false
	}
	s.WriteError(w, http.StatusForbidden, "only a topic owner or an admin may modify this link")
	return false
}

// AuthorizeTopicRead enforces the read rule for topic metadata: the
// caller must hold some grant matching the topic (produce, consume, or
// create), own it, or be an admin. Topic details carry partition owners,
// sizes, and traffic, which a principal with no stake in the topic has
// no business seeing. A missing topic passes so the handler's own lookup
// produces the canonical 404 (and a 403 would confirm existence anyway).
func (s *Set) AuthorizeTopicRead(w http.ResponseWriter, r *http.Request, topicName string) bool {
	id, ok := security.IdentityFrom(r.Context())
	if !ok || id.AllowedAny(topicName) {
		return true
	}
	t, err := s.Deps.Broker.GetTopic(r.Context(), topicName)
	if errors.Is(err, errs.ErrTopicNotFound) || errors.Is(err, errs.ErrNotFound) {
		return true
	}
	if err != nil {
		s.WriteBrokerError(w, "authorize topic", err)
		return false
	}
	if t.Owner == id.Username {
		return true
	}
	s.WriteError(w, http.StatusForbidden, "no grant on this topic")
	return false
}

// CanReadTopic reports whether the caller may see the given topic in a
// listing: security disabled, admin, owner, or any matching grant. It is
// the filter behind GET /v1/topics and never writes a response.
func CanReadTopic(r *http.Request, owner, topicName string) bool {
	id, ok := security.IdentityFrom(r.Context())
	if !ok {
		return true
	}
	return id.AllowedAny(topicName) || (owner != "" && owner == id.Username)
}

// Identity returns the authenticated user and whether one is present
// (false when security is disabled or the path was exempt).
func Identity(r *http.Request) (user.User, bool) {
	return security.IdentityFrom(r.Context())
}
