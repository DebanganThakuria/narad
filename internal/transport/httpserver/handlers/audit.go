package handlers

import (
	"log/slog"
	"net/http"
)

// audit emits a structured security-audit log line. The "component":
// "audit" attribute lets operators route these to a dedicated sink and
// alert on them. The actor is the authenticated caller (empty when
// security is disabled).
func (s *Set) Audit(r *http.Request, event, target string) {
	s.Deps.Logger.Info("audit",
		"component", "audit",
		"event", event,
		"actor", auditActor(r),
		"target", target,
	)
}

// Audit outcomes of a mutation (see AuditOutcome).
const (
	// AuditOK: the change was applied (a 2xx, or the part of a PATCH
	// this node applied before a later part failed).
	AuditOK = "ok"
	// AuditDenied: the caller was refused (403).
	AuditDenied = "denied"
	// AuditRejected: refused by this node or the leader (another 4xx).
	AuditRejected = "rejected"
	// AuditFailed: failed on this node or the leader (a 5xx).
	AuditFailed = "failed"
	// AuditUnknown: the request ended without a decision this node
	// knows, so the change may or may not have been applied: a forward
	// whose reply never came back, a change the leader lost its
	// leadership over while committing it, a client that went away
	// mid-change, or the part of a forwarded PATCH before the part that
	// failed.
	AuditUnknown = "unknown"
)

// AuditOutcome emits the audit line of one mutation with its outcome
// (AuditOK, AuditDenied, AuditRejected, AuditFailed or AuditUnknown) and
// the HTTP status the client got, plus extra key/value attributes. It
// has the shape of Audit's line with those added; a denied mutation is
// logged at warn, any other at info.
func (s *Set) AuditOutcome(r *http.Request, event, target, outcome string, status int, extra ...any) {
	args := make([]any, 0, 12+len(extra))
	args = append(args,
		"component", "audit",
		"event", event,
		"actor", auditActor(r),
		"target", target,
		"outcome", outcome,
		"status", status,
	)
	args = append(args, extra...)
	level := slog.LevelInfo
	if outcome == AuditDenied {
		level = slog.LevelWarn
	}
	s.Deps.Logger.Log(r.Context(), level, "audit", args...)
}

func auditActor(r *http.Request) string {
	if id, ok := Identity(r); ok {
		return id.Username
	}
	return ""
}
