package topics

// Audit trail for topic mutations. Every topic create, alter, schema
// change, delete, attach and detach that passed body validation writes
// one audit line, when the request finishes, on the node the client
// called, whether this node applied it or forwarded it to the leader.
// The outcome is read from the status the client got (for a forward,
// the leader's), except that a request that ended without a decision
// this node knows is "unknown": a forward whose reply never came back
// (the router marks the writer undecided before answering 503), a 503
// for a change the leader appended and then lost its leadership over
// (errs.ErrOutcomeUnknown, here or in the leader's reply), and a 499 for
// a client that went away mid-change. Each may have been applied, so
// none is logged as rejected or failed: an audit query for the changes
// that happened must not miss one. A PATCH that failed part way is
// audited per field group (see auditAlter).

import (
	"net/http"
	"strings"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Audit event names of the topic mutations.
const (
	auditEventCreate = "topic.create"
	auditEventAlter  = "topic.alter"
	auditEventSchema = "topic.schema"
	auditEventDelete = "topic.delete"
	auditEventAttach = "topic.attach"
	auditEventDetach = "topic.detach"
)

// auditWriter records the status a mutation answered and whether the
// answer carried a decision.
type auditWriter struct {
	http.ResponseWriter
	status    int
	undecided bool
}

func newAuditWriter(w http.ResponseWriter) *auditWriter {
	return &auditWriter{ResponseWriter: w}
}

func (w *auditWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *auditWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the connection's writer.
func (w *auditWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// MarkUndecided records that the answer about to be written does not
// carry the leader's decision (the cluster router calls it when a
// forward ended without a reply).
func (w *auditWriter) MarkUndecided() { w.undecided = true }

// outcome names what the recorded answer says about the mutation.
func (w *auditWriter) outcome() string {
	switch {
	case w.undecided, w.status == 0, w.status == handlers.StatusClientClosedRequest:
		return handlers.AuditUnknown
	case w.status < http.StatusBadRequest:
		return handlers.AuditOK
	case w.status == http.StatusForbidden:
		return handlers.AuditDenied
	case w.status < http.StatusInternalServerError:
		return handlers.AuditRejected
	}
	return handlers.AuditFailed
}

// audit writes the mutation's audit line.
func (w *auditWriter) audit(s *handlers.Set, r *http.Request, event, target string, extra ...any) {
	s.AuditOutcome(r, event, target, w.outcome(), w.status, extra...)
}

// auditAlter writes a PATCH's audit lines: topic.alter naming the
// retention, cap and partition fields it set, and topic.schema for a
// schema change. Each field group gets the request's outcome, except a
// group that may have changed although a later one failed: one this
// node applied is ok, and on a node that forwarded the PATCH every group
// but the last is unknown, since the leader applies them in order and
// this node cannot tell how far it got (the last one either failed or
// never ran). Fields whose outcomes differ go on separate topic.alter
// lines, each with the status the client got.
func auditAlter(s *handlers.Set, r *http.Request, w *auditWriter, topicName string, req alterRequest, progress alterProgress) {
	requestOutcome := w.outcome()
	groups := req.groups()
	outcomeOf := func(i int) string {
		switch {
		case !progress.forwarded && i < progress.applied:
			return handlers.AuditOK
		case requestOutcome == handlers.AuditOK, requestOutcome == handlers.AuditUnknown:
			return requestOutcome
		case progress.forwarded && i < len(groups)-1:
			return handlers.AuditUnknown
		}
		return requestOutcome
	}

	var fields []string
	lineOutcome := ""
	flush := func() {
		if len(fields) > 0 {
			s.AuditOutcome(r, auditEventAlter, topicName, lineOutcome, w.status, "fields", strings.Join(fields, ","))
		}
		fields = nil
	}
	for i, group := range groups {
		outcome := outcomeOf(i)
		if group == alterSchema {
			flush()
			s.AuditOutcome(r, auditEventSchema, topicName, outcome, w.status, "schema_base_version", req.SchemaBaseVersion)
			continue
		}
		if outcome != lineOutcome {
			flush()
			lineOutcome = outcome
		}
		fields = append(fields, alterGroupFields(req, group)...)
	}
	flush()
}

// alterGroupFields names the request fields of one non-schema group.
func alterGroupFields(req alterRequest, group alterGroup) []string {
	switch group {
	case alterRetention:
		return []string{"retention_ms"}
	case alterCaps:
		var fields []string
		if req.MaxInFlightPerPartition != nil {
			fields = append(fields, "max_in_flight_per_partition")
		}
		if req.MaxAckedAheadPerPartition != nil {
			fields = append(fields, "max_acked_ahead_per_partition")
		}
		return fields
	case alterPartitions:
		return []string{"partitions"}
	}
	return nil
}
