package handlers

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// Authorization happens at the HTTP ingress node, before any routing:
// forwarded cluster RPCs carry no user identity because the ingress
// already decided the request is allowed. When security is disabled
// there is no identity on the context and every check passes.

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

// AuthorizeTopicManage enforces the owner-or-admin rule for altering or
// deleting a topic, writing a 403 when the caller is neither. A missing
// topic passes — the handler's own lookup produces the canonical 404.
func (s *Set) AuthorizeTopicManage(w http.ResponseWriter, r *http.Request, topicName string) bool {
	id, ok := security.IdentityFrom(r.Context())
	if !ok || id.IsAdmin() {
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
	s.WriteError(w, http.StatusForbidden, "only the topic owner or an admin may modify this topic")
	return false
}

// Identity returns the authenticated user and whether one is present
// (false when security is disabled or the path was exempt).
func Identity(r *http.Request) (user.User, bool) {
	return security.IdentityFrom(r.Context())
}
