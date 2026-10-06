package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
)

// RemoteRegistryDeps are the remote registry's collaborators.
type RemoteRegistryDeps struct {
	Store   *metastore.Store
	Service *remote.Service
	Plane   *RemotePlane
	Metrics *metrics.Metrics
	Log     *slog.Logger
	SelfID  string
}

// RemoteRegistry serves the remotes registry on the leader and the
// member-side checks; it is the plane's RegistryLeader and CheckRunner,
// and the service's ClusterChecks.
//
// On the leader, every write runs the upgrade gate, proposes one Raft
// op, and writes exactly one authoritative audit line per proposal
// after the apply returns, with the actor and request ID the ingress
// forwarded. Its error mapper answers a fixed message per sentinel and
// never appends err.Error().
type RemoteRegistry struct {
	d   RemoteRegistryDeps
	now func() time.Time
}

// NewRemoteRegistry builds the registry.
func NewRemoteRegistry(d RemoteRegistryDeps) *RemoteRegistry {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &RemoteRegistry{d: d, now: time.Now}
}

// checkFanoutTimeout bounds CheckEverywhere: each member's checks run
// within remote.CheckTimeout, plus a margin for the RPC.
const checkFanoutTimeout = remote.CheckTimeout + 5*time.Second

// statusFanoutTimeout bounds a status fan-out, which makes no outbound
// call.
const statusFanoutTimeout = 5 * time.Second

// ServeWrite implements RegistryLeader.
//
// Every registry write is re-authorized here against the leader's own
// user records (leaderActorContext, then admin), as a remote child write
// is: the ingress checked a replica that may lag the leader. A write
// with no caller is refused too; the ingress requires security for
// these routes, so an empty actor is never legitimate.
func (r *RemoteRegistry) ServeWrite(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	if refusal := r.authorize(ctx, req); refusal != nil {
		return *refusal
	}
	switch req.SubOp {
	case nodewire.RemoteSubCreate:
		return r.create(ctx, req)
	case nodewire.RemoteSubUpdate:
		return r.update(ctx, req)
	case nodewire.RemoteSubDelete:
		return r.delete(ctx, req)
	case nodewire.RemoteSubReencrypt:
		return r.reencrypt(ctx, req)
	default:
		return errorResponse(http.StatusBadRequest, "unsupported remote write")
	}
}

// authorize refuses (403, with a refused audit line of class denied) a
// registry write whose caller the leader does not know, or knows but
// not as an admin, or that names no caller.
func (r *RemoteRegistry) authorize(ctx context.Context, req nodewire.RemoteWriteRequest) *nodewire.Response {
	deny := func(res nodewire.Response) *nodewire.Response {
		if res.Status == http.StatusForbidden {
			remote.LeaderAudit(r.d.Log, remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Outcome: remote.OutcomeRefused, Class: "denied"})
		}
		return &res
	}
	if req.Actor == "" {
		return deny(errorResponse(http.StatusForbidden, "caller required: remotes need security on"))
	}
	actx, refusal := leaderActorContext(ctx, r.d.Store, r.d.Log, req.Actor)
	if refusal != nil {
		return deny(*refusal)
	}
	if refusal := requireAdmin(actx); refusal != nil {
		return deny(*refusal)
	}
	return nil
}

// create proposes opPutRemote.
func (r *RemoteRegistry) create(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var op metastore.PutRemoteOp
	if err := decodeStrictJSON(req.Body, &op); err != nil || !wellFormed(op.Record) {
		return errorResponse(http.StatusBadRequest, "invalid remote write")
	}
	op.Actor, op.RequestID = req.Actor, req.RequestID
	ev := remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Target: op.Record.Name, Attrs: []slog.Attr{
		slog.String("host", remote.URLHost(op.Record.URL)),
		slog.String("fingerprint", op.Record.Fingerprint),
	}}
	if err := r.RequirePosture(ctx); err != nil {
		return r.refuse(ev, err)
	}
	if err := r.d.Store.PutRemote(ctx, op); err != nil {
		return r.refuse(ev, err)
	}
	rec, err := r.d.Store.GetRemote(op.Record.Name)
	if err != nil {
		return r.refuse(ev, err)
	}
	ev.Attrs = append(ev.Attrs, slog.Uint64("credential_version", rec.CredentialVersion))
	r.commit(ev)
	return jsonResponse(http.StatusCreated, remote.ViewOf(rec))
}

// update proposes a field-scoped opUpdateRemote.
func (r *RemoteRegistry) update(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var op metastore.UpdateRemoteOp
	if err := decodeStrictJSON(req.Body, &op); err != nil || domremote.ValidateName(op.Name) != nil || op.Reseal || !wellFormedFields(op.Fields) {
		return errorResponse(http.StatusBadRequest, "invalid remote write")
	}
	op.Actor, op.RequestID = req.Actor, req.RequestID
	fields := changedFields(op)
	ev := remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Target: op.Name, Attrs: []slog.Attr{
		slog.String("fields", strings.Join(fields, ",")),
		slog.Bool("password_changed", op.Credential != nil),
	}}
	if err := r.RequirePosture(ctx); err != nil {
		return r.refuse(ev, err)
	}
	if err := r.d.Store.UpdateRemote(ctx, op); err != nil {
		return r.refuse(ev, err)
	}
	rec, err := r.d.Store.GetRemote(op.Name)
	if err != nil {
		return r.refuse(ev, err)
	}
	ev.Attrs = append(ev.Attrs,
		slog.String("host", remote.URLHost(rec.URL)),
		slog.String("fingerprint", rec.Fingerprint),
		slog.Uint64("credential_version", rec.CredentialVersion))
	r.commit(ev)
	return jsonResponse(http.StatusOK, remote.ViewOf(rec))
}

// delete proposes opDeleteRemote. It runs the release gate alone, which
// reads member records, so an emergency revocation works while a member
// is down.
func (r *RemoteRegistry) delete(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var op metastore.DeleteRemoteOp
	if err := decodeStrictJSON(req.Body, &op); err != nil || domremote.ValidateName(op.Name) != nil {
		return errorResponse(http.StatusBadRequest, "invalid remote write")
	}
	if op.Force && !remote.AllowForceDelete {
		return errorResponse(http.StatusBadRequest, "force delete of a remote is not enabled")
	}
	op.Actor, op.RequestID = req.Actor, req.RequestID
	ev := remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Target: op.Name, Attrs: []slog.Attr{slog.Bool("forced", op.Force)}}
	if err := r.RequireReleases(ctx); err != nil {
		return r.refuse(ev, err)
	}
	links, _ := r.d.Store.RemoteChildrenOf(op.Name)
	if err := r.d.Store.DeleteRemote(ctx, op); err != nil {
		return r.refuse(ev, err)
	}
	ev.Attrs = append(ev.Attrs, slog.Int("links_left", len(links)))
	if len(links) > 0 {
		ev.Attrs = append(ev.Attrs, slog.String("links", strings.Join(links, ",")))
	}
	r.commit(ev)
	return nodewire.Response{Status: http.StatusNoContent}
}

// ReencryptAnswer is the re-encrypt call's answer.
type ReencryptAnswer struct {
	KeyVersion     string            `json:"key_version"`
	Reencrypted    []string          `json:"reencrypted"`
	AlreadyCurrent []string          `json:"already_current"`
	Failed         []ReencryptFailed `json:"failed"`
}

// ReencryptFailed names a remote the re-encrypt could not move.
type ReencryptFailed struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// reencrypt re-seals every remote whose envelope is not under the
// current key: opened with the key its kv names, sealed under the
// current one with the same associated data, fingerprinted again, and
// proposed as a compare-and-swap on the credential version, so a remote
// changed in between keeps its newer ciphertext. Safe to repeat.
func (r *RemoteRegistry) reencrypt(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	summary := remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Target: "*"}
	if err := r.RequirePosture(ctx); err != nil {
		return r.refuse(summary, err)
	}
	records, err := r.d.Store.ListRemotes()
	if err != nil {
		return r.refuse(summary, err)
	}
	ans := ReencryptAnswer{Reencrypted: []string{}, AlreadyCurrent: []string{}, Failed: []ReencryptFailed{}}
	for _, rec := range records {
		rs, outcome, err := r.d.Service.PrepareReseal(rec)
		if err != nil {
			return r.refuse(summary, err)
		}
		switch outcome {
		case remote.ResealCurrent:
			ans.AlreadyCurrent = append(ans.AlreadyCurrent, rec.Name)
			continue
		case remote.ResealKeyUnread, remote.ResealOpenFail:
			ans.Failed = append(ans.Failed, ReencryptFailed{Name: rec.Name, Reason: outcome})
			continue
		}
		cv := rs.ExpectCV
		op := metastore.UpdateRemoteOp{
			Name: rec.Name, Credential: &rs.Credential, Fingerprint: rs.Fingerprint, SealedAgainst: &rs.SealedAgainst,
			ReadRevision: rec.Revision, ExpectCredentialVersion: &cv, Reseal: true,
			SealedAtMs: r.now().UnixMilli(), Actor: req.Actor, RequestID: req.RequestID,
		}
		ev := remote.AuditEvent{Event: req.SubOp, Actor: req.Actor, RequestID: req.RequestID, Target: rec.Name}
		if err := r.d.Store.UpdateRemote(ctx, op); err != nil {
			status, class, _ := r.classify(err)
			if status >= 500 {
				return r.refuse(ev, err)
			}
			ev.Outcome, ev.Class = remote.OutcomeRefused, class
			remote.LeaderAudit(r.d.Log, ev)
			ans.Failed = append(ans.Failed, ReencryptFailed{Name: rec.Name, Reason: class})
			continue
		}
		ev.Attrs = []slog.Attr{slog.String("fingerprint", rs.Fingerprint), slog.Uint64("credential_version", cv+1)}
		r.commit(ev)
		ans.Reencrypted = append(ans.Reencrypted, rec.Name)
	}
	ans.KeyVersion = r.d.Service.CurrentKeyVersion()
	return jsonResponse(http.StatusOK, ans)
}

// commit writes the committed leader audit line.
func (r *RemoteRegistry) commit(ev remote.AuditEvent) {
	ev.Outcome = remote.OutcomeCommitted
	remote.LeaderAudit(r.d.Log, ev)
}

// refuse writes the refused leader audit line and the fixed answer.
func (r *RemoteRegistry) refuse(ev remote.AuditEvent, err error) nodewire.Response {
	_, class, res := r.classify(err)
	ev.Outcome, ev.Class = remote.OutcomeRefused, class
	remote.LeaderAudit(r.d.Log, ev)
	return res
}

// classify is the fixed-message error mapper of remote writes: a status,
// the refusal's class for the audit line, and the answer. It never puts
// err.Error() in an answer; only the structured detail of the typed
// errors (the members, the links, the revision).
func (r *RemoteRegistry) classify(err error) (int, string, nodewire.Response) {
	var (
		posture *PostureError
		inUse   *metastore.RemoteInUseError
		changed *metastore.RemoteChangedError
	)
	switch {
	case errors.As(err, &posture):
		body := map[string]any{"error": posture.message(), "members": posture.Members}
		return http.StatusPreconditionFailed, posture.class(), jsonResponse(http.StatusPreconditionFailed, body)
	case errors.Is(err, metastore.ErrEntryTypeNotYetUsable):
		// A member that joined or reported an older release between the
		// gate and the proposal: nothing was proposed.
		gate := releaseGateError(err)
		body := map[string]any{"error": gate.message(), "members": gate.Members}
		return http.StatusPreconditionFailed, gate.class(), jsonResponse(http.StatusPreconditionFailed, body)
	case errors.As(err, &inUse):
		body := map[string]any{"error": fmt.Sprintf("remote is used by %d remote children", len(inUse.Links)), "links": inUse.Links}
		return http.StatusConflict, "in_use", jsonResponse(http.StatusConflict, body)
	case errors.As(err, &changed):
		body := map[string]any{"error": "remote changed, retry", "revision": changed.Revision}
		return http.StatusConflict, "changed", jsonResponse(http.StatusConflict, body)
	}
	for _, m := range remoteWriteErrors {
		if errors.Is(err, m.err) {
			msg := m.msg
			if m.status == http.StatusPreconditionFailed && (errors.Is(err, errs.ErrRemoteSecretWeak) || errors.Is(err, errs.ErrRemoteSecretMissing)) {
				// The strength rule's own text names the rule and the
				// fix, never the secret.
				msg = err.Error()
			}
			return m.status, m.class, errorResponse(m.status, msg)
		}
	}
	r.d.Log.Error("remote write failed", "err", err)
	return http.StatusInternalServerError, "error", errorResponse(http.StatusInternalServerError, "remote write failed")
}

// remoteWriteErrors is the fixed mapping, in match order.
var remoteWriteErrors = []struct {
	err    error
	status int
	class  string
	msg    string
}{
	{errs.ErrRemoteExists, http.StatusConflict, "exists", "remote already exists"},
	{errs.ErrRemoteLimit, http.StatusConflict, "limit", "cluster holds the maximum number of remotes"},
	{errs.ErrRemoteSaltRace, http.StatusConflict, "salt_race", "remote salt raced, retry"},
	{errs.ErrRemoteChanged, http.StatusConflict, "changed", "remote changed, retry"},
	{errs.ErrRemoteInUse, http.StatusConflict, "in_use", "remote is used by remote children"},
	{errs.ErrRemoteNotFound, http.StatusNotFound, "not_found", "remote not found"},
	{errs.ErrRemoteKeyExhausted, http.StatusPreconditionFailed, "key_exhausted", "encryption key seal budget exhausted; rotate the cluster secret"},
	{errs.ErrRemoteSecretWeak, http.StatusPreconditionFailed, "secret_weak", "cluster secret too weak"},
	{errs.ErrRemoteSecretMissing, http.StatusPreconditionFailed, "secret_missing", "no cluster secret"},
	{errs.ErrInvalidArgument, http.StatusBadRequest, "invalid", "invalid remote write"},
	{errs.ErrUnavailable, http.StatusServiceUnavailable, "unavailable", "control plane temporarily unavailable"},
}

// wellFormed re-checks the shape of a forwarded create: the ingress
// validated it, and the leader does not trust the wire to have.
func wellFormed(rec domremote.Record) bool {
	c, err := remote.CanonicalURL(rec.URL)
	return err == nil && c.URL == rec.URL &&
		domremote.ValidateName(rec.Name) == nil &&
		domremote.ValidateUsername(rec.Username) == nil &&
		domremote.ValidateCAPEM(rec.CAPEM) == nil &&
		rec.Limits.Validate() == nil && rec.ID != ""
}

// wellFormedFields re-checks the named fields of a forwarded change.
func wellFormedFields(f metastore.RemoteFields) bool {
	if f.URL != nil {
		if c, err := remote.CanonicalURL(*f.URL); err != nil || c.URL != *f.URL {
			return false
		}
	}
	if f.Username != nil && domremote.ValidateUsername(*f.Username) != nil {
		return false
	}
	if f.CAPEM != nil && domremote.ValidateCAPEM(*f.CAPEM) != nil {
		return false
	}
	return f.Limits == nil || f.Limits.Validate() == nil
}

// changedFields names the fields an update sets, for the audit line.
func changedFields(op metastore.UpdateRemoteOp) []string {
	var out []string
	if op.Fields.URL != nil {
		out = append(out, "url")
	}
	if op.Fields.Username != nil {
		out = append(out, "username")
	}
	if op.Fields.CAPEM != nil {
		out = append(out, "ca_pem")
	}
	if op.Fields.Limits != nil {
		out = append(out, "limits")
	}
	if op.Credential != nil {
		out = append(out, "password")
	}
	return out
}

// ServeCheck implements RegistryLeader: the checks from this member
// (limited per remote, 429 when busy or too soon) or this member's
// status (no outbound call).
func (r *RemoteRegistry) ServeCheck(ctx context.Context, req nodewire.RemoteCheckRequest) nodewire.Response {
	switch req.Mode {
	case nodewire.RemoteCheckRun:
		var creq remote.CheckRequest
		if err := decodeStrictJSON(req.Body, &creq); err != nil || domremote.ValidateName(creq.Remote) != nil || topic.ValidateName(creq.Topic) != nil {
			return errorResponse(http.StatusBadRequest, "invalid remote check request")
		}
		rep, throttled := r.d.Service.RunCheck(ctx, creq)
		if throttled {
			return jsonResponse(http.StatusTooManyRequests, rep)
		}
		return jsonResponse(http.StatusOK, rep)
	case nodewire.RemoteCheckStatus:
		return jsonResponse(http.StatusOK, r.d.Service.Status())
	default:
		return errorResponse(http.StatusBadRequest, "unsupported remote check mode")
	}
}

// memberAnswer is one member's answer to a fan-out.
type memberAnswer struct {
	node string
	res  nodewire.Response
	err  error
}

// fanOut sends req to every member that is not removed, dead ones
// included, in parallel.
func (r *RemoteRegistry) fanOut(ctx context.Context, req nodewire.RemoteCheckRequest) ([]memberAnswer, error) {
	members, err := r.d.Plane.Members()
	if err != nil {
		return nil, err
	}
	out := make([]memberAnswer, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Go(func() {
			res, err := r.d.Plane.SendCheck(ctx, m, req)
			out[i] = memberAnswer{node: m.ID, res: res, err: err}
		})
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].node < out[j].node })
	return out, nil
}

// answerClass classifies a member's answer that is not a report: an
// error is unreachable, an older release's "unsupported rpc operation"
// (or a member without the plane) is old_release.
func answerClass(a memberAnswer) string {
	switch {
	case a.err != nil:
		return remote.ClassUnreachable
	case unsupportedOperation(a.res), a.res.Status == http.StatusNotImplemented:
		return remote.ClassOldRelease
	}
	return ""
}

// CheckEverywhere implements CheckRunner: the checks on every member
// within 15 s. A member that did not answer reports unreachable; an
// older one old_release.
func (r *RemoteRegistry) CheckEverywhere(ctx context.Context, req remote.CheckRequest) ([]remote.NodeReport, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, checkFanoutTimeout)
	defer cancel()
	answers, err := r.fanOut(ctx, nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckRun, RequestID: remote.NewRequestID(), Body: body})
	if err != nil {
		return nil, err
	}
	reports := make([]remote.NodeReport, 0, len(answers))
	for _, a := range answers {
		if class := answerClass(a); class != "" {
			reports = append(reports, remote.NodeReport{Node: a.node, Result: remote.ResultFail, Class: class, Warnings: []string{}})
			continue
		}
		var rep remote.NodeReport
		if err := json.Unmarshal(a.res.Body, &rep); err != nil || rep.Result == "" {
			rep = remote.NodeReport{Result: remote.ResultFail, Class: topic.RemoteStateUnknown, Warnings: []string{}}
		}
		rep.Node = a.node
		reports = append(reports, rep)
	}
	return reports, nil
}

// StatusEverywhere implements remote.ClusterChecks: every member's
// status report, or why there is none.
func (r *RemoteRegistry) StatusEverywhere(ctx context.Context) []remote.MemberStatus {
	ctx, cancel := context.WithTimeout(ctx, statusFanoutTimeout)
	defer cancel()
	answers, err := r.fanOut(ctx, nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckStatus, RequestID: remote.NewRequestID()})
	if err != nil {
		return nil
	}
	out := make([]remote.MemberStatus, 0, len(answers))
	for _, a := range answers {
		ms := remote.MemberStatus{Node: a.node}
		if class := answerClass(a); class != "" {
			ms.Class = class
		} else {
			var rep remote.NodeStatusReport
			if a.res.Status != http.StatusOK || json.Unmarshal(a.res.Body, &rep) != nil {
				ms.Class = remote.ClassOldRelease
			} else {
				ms.Report = &rep
			}
		}
		out = append(out, ms)
	}
	return out
}

// PostureError is the upgrade gate's refusal: the members that hold
// back the remote Raft entry types, did not answer, or report a posture
// that forbids remotes. It matches errs.ErrRemoteFeatureGate or
// errs.ErrRemotePosture.
type PostureError struct {
	Err     error
	Members []string
	// Detail is the release gate's reason (metastore.Store.RemotesUsable):
	// the member holding the remote entry types back and the build it
	// reported. Empty for the other refusals.
	Detail string
}

func (e *PostureError) Error() string { return e.message() }

// Unwrap exposes the sentinel.
func (e *PostureError) Unwrap() error { return e.Err }

func (e *PostureError) message() string {
	switch {
	case errors.Is(e.Err, errs.ErrRemotePosture):
		return "a cluster member's security posture forbids remotes (security off or legacy cluster auth on)"
	case e.Detail != "":
		return "not every cluster member runs a release that applies the remote Raft entry types; upgrade or remove the member named here: " + e.Detail
	}
	return "every cluster member must answer the remotes posture check; bring the listed members back or remove them"
}

func (e *PostureError) class() string {
	if errors.Is(e.Err, errs.ErrRemotePosture) {
		return topic.RemoteStateNodeInsecure
	}
	return remote.ClassOldRelease
}

// releaseGateError turns the store's ErrEntryTypeNotYetUsable into the
// gate's refusal (412), keeping its reason and naming the member that
// holds the remote entry types back when the store says which.
func releaseGateError(err error) *PostureError {
	pe := &PostureError{Err: errs.ErrRemoteFeatureGate, Detail: strings.TrimPrefix(err.Error(), metastore.ErrEntryTypeNotYetUsable.Error()+": ")}
	if held, ok := errors.AsType[*metastore.RemotesHeldBackError](err); ok {
		pe.Detail = held.Reason
		if held.Member != "" {
			pe.Members = []string{held.Member}
		}
	}
	return pe
}

// RequirePosture implements CheckRunner: the upgrade gate. The release
// gate first (RequireReleases); then every member that is not removed,
// dead ones included, must answer with a posture that allows remotes:
// security on and legacy cluster auth off. Raft TLS is reported, not
// gated (Q23).
func (r *RemoteRegistry) RequirePosture(ctx context.Context) error {
	if err := r.RequireReleases(ctx); err != nil {
		return err
	}
	var unanswered, posture []string
	for _, ms := range r.StatusEverywhere(ctx) {
		switch {
		case ms.Report == nil:
			unanswered = append(unanswered, ms.Node)
		case !remote.PostureAllowsRemotes(ms.Report.Posture):
			posture = append(posture, ms.Node)
		}
	}
	if len(unanswered) > 0 {
		return &PostureError{Err: errs.ErrRemoteFeatureGate, Members: unanswered}
	}
	if len(posture) > 0 {
		return &PostureError{Err: errs.ErrRemotePosture, Members: posture}
	}
	return nil
}

// RequireReleases implements CheckRunner: the release gate. The remote
// entry types are usable only once every member, dead ones and Raft
// servers without a member record included, reports a release that
// applies them (metastore D6); until then every remote write is refused
// with 412 naming the member, never written another way.
func (r *RemoteRegistry) RequireReleases(context.Context) error {
	if err := r.d.Store.RemotesUsable(); err != nil {
		return releaseGateError(err)
	}
	return nil
}
