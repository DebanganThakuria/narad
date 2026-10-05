package handlers

import "net/http"

// AuditWriter records the status a mutation answered, and whether the
// answer carried a decision, so its audit line can be written once the
// request finishes, on the node the client called, whether that node
// applied the mutation or forwarded it to the leader. Wrap the response
// writer once the request has passed its admin and body checks, and
// defer Audit.
//
// The outcome is read from the status the client got (for a forward,
// the leader's), except that an answer without a decision this node
// knows is AuditUnknown: a forward whose reply never came back (the
// cluster router calls MarkUndecided before its 503) and a client that
// went away mid-change (499). Either may have been applied, so neither
// is logged as rejected or failed.
type AuditWriter struct {
	http.ResponseWriter
	status    int
	undecided bool
}

// NewAuditWriter wraps w.
func NewAuditWriter(w http.ResponseWriter) *AuditWriter {
	return &AuditWriter{ResponseWriter: w}
}

// WriteHeader records the first status written.
func (w *AuditWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

// Write records an implicit 200 when no status was written first.
func (w *AuditWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the connection's writer.
func (w *AuditWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// MarkUndecided records that the answer about to be written does not
// carry the leader's decision.
func (w *AuditWriter) MarkUndecided() { w.undecided = true }

// Outcome names what the recorded answer says about the mutation.
func (w *AuditWriter) Outcome() string {
	switch {
	case w.undecided, w.status == 0, w.status == StatusClientClosedRequest:
		return AuditUnknown
	case w.status < http.StatusBadRequest:
		return AuditOK
	case w.status == http.StatusForbidden:
		return AuditDenied
	case w.status < http.StatusInternalServerError:
		return AuditRejected
	}
	return AuditFailed
}

// Audit writes the mutation's audit line (see Set.AuditOutcome).
func (w *AuditWriter) Audit(s *Set, r *http.Request, event, target string, extra ...any) {
	s.AuditOutcome(r, event, target, w.Outcome(), w.status, extra...)
}
