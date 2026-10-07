package remote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
)

// NewRequestID returns 16 hex characters from crypto/rand. It joins the
// ingress node's remote.request audit line to the leader's line for the
// same proposal.
func NewRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // never fails (crypto/rand panics on a broken RNG)
	return hex.EncodeToString(b[:])
}

// AuditEvent is one leader-side audit line.
type AuditEvent struct {
	Event, Actor, RequestID, Target, Outcome, Class string
	Attrs                                           []slog.Attr
}

// Leader audit outcomes.
const (
	OutcomeCommitted = "committed"
	OutcomeRefused   = "refused"
)

// leaderAuditFixedAttrs is the most fields LeaderAudit writes before
// the caller's attrs: component, event, actor, request_id, target,
// outcome and class.
const leaderAuditFixedAttrs = 7

// LeaderAudit writes the authoritative audit line after a Raft apply,
// exactly one per proposal: component=audit, event, actor, request_id,
// target, outcome ("committed" or "refused"), class, then attrs. Lines
// carry the remote name, the URL host, the fingerprint and the
// credential version; never a password, a ciphertext, a key version or
// the replicator's username.
func LeaderAudit(log *slog.Logger, ev AuditEvent) {
	if log == nil {
		return
	}
	// Sized for the fixed fields only: the caller's attrs are appended
	// (one growth at most), so no size is computed from their length.
	attrs := make([]slog.Attr, 0, leaderAuditFixedAttrs)
	attrs = append(attrs,
		slog.String("component", "audit"),
		slog.String("event", ev.Event),
		slog.String("actor", ev.Actor),
		slog.String("request_id", ev.RequestID),
		slog.String("target", ev.Target),
		slog.String("outcome", ev.Outcome),
	)
	if ev.Class != "" {
		attrs = append(attrs, slog.String("class", ev.Class))
	}
	attrs = append(attrs, ev.Attrs...)
	log.LogAttrs(context.Background(), slog.LevelInfo, "audit", attrs...)
}
