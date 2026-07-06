package handlers

import "net/http"

// audit emits a structured security-audit log line. The "component":
// "audit" attribute lets operators route these to a dedicated sink and
// alert on them. The actor is the authenticated caller (empty when
// security is disabled).
func (s *Set) Audit(r *http.Request, event, target string) {
	actor := ""
	if id, ok := Identity(r); ok {
		actor = id.Username
	}
	s.Deps.Logger.Info("audit",
		"component", "audit",
		"event", event,
		"actor", actor,
		"target", target,
	)
}
