package cluster

// RemoteLinks is the leader side of remote children (package B's
// LinkLeader): create, pause, resume, skip and delete a remote child,
// and every topic delete, each forwarded to the leader as an
// OpRemoteWrite sub-op and written through Raft; and, on every member,
// the unshipped query a delete asks before it may abandon records.
//
// The ingress node authorized the request before forwarding it. The
// leader runs each write under the actor the payload names, looked up
// in its own replica (actorContext), so the topic manager re-checks the
// caller's rights under the topics' locks as it does for every
// forwarded topic write; the actor and request ID also join the audit
// line the leader writes after each apply.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
	"github.com/debanganthakuria/narad/internal/security"
)

// RemoteLinksDeps are RemoteLinks' collaborators.
type RemoteLinksDeps struct {
	Store   *metastore.Store
	Runner  *FanoutRunner
	Plane   *RemotePlane
	Ingress *ingress.Manager
	Broker  broker.Broker
	Metrics *metrics.Metrics
	Log     *slog.Logger
	SelfID  string
}

// Unshipped-check pacing (ch. 4.5).
const (
	// unshippedCheckEvery is how often the leader starts a new unshipped
	// check for one parent; a delete inside the window gets 429.
	unshippedCheckEvery = 10 * time.Second
	// unshippedScanLimit bounds a member's WAL scan; a backlog past it
	// counts as unshipped.
	unshippedScanLimit = 1_000_000
	// unshippedAskTimeout bounds the fan-out of the unshipped query.
	unshippedAskTimeout = 15 * time.Second
	// unshippedFlightTimeout bounds a whole check: the query fan-out,
	// then the cursor lag from every partition owner.
	unshippedFlightTimeout = 2 * unshippedAskTimeout
)

// RemoteLinks serves child.* and topic.delete on the leader and the
// unshipped mode on every member.
type RemoteLinks struct {
	d RemoteLinksDeps

	mu        sync.Mutex
	flights   map[string]*unshippedFlight
	lastCheck map[string]time.Time
	// flightSeq numbers unshipped checks in the order they start.
	flightSeq uint64

	now        func() time.Time
	checkEvery time.Duration
	scanLimit  int
}

// NewRemoteLinks builds the leader side of remote children.
func NewRemoteLinks(d RemoteLinksDeps) *RemoteLinks {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &RemoteLinks{
		d:          d,
		flights:    map[string]*unshippedFlight{},
		lastCheck:  map[string]time.Time{},
		now:        time.Now,
		checkEvery: unshippedCheckEvery,
		scanLimit:  unshippedScanLimit,
	}
}

// The sub-op bodies, defined by package B and stable on the wire.
type (
	childAttachBody struct {
		Parent      string `json:"parent"`
		Child       string `json:"child"`
		Remote      string `json:"remote"`
		RemoteTopic string `json:"remote_topic"`
		DelayMs     int64  `json:"delay_ms,omitempty"`
		From        string `json:"from,omitempty"`
		Lanes       int    `json:"lanes,omitempty"`
		DryRun      bool   `json:"dry_run,omitempty"`
	}
	childStateBody struct {
		Parent       string `json:"parent"`
		Child        string `json:"child"`
		Reason       string `json:"reason,omitempty"`
		AcceptTarget bool   `json:"accept_target,omitempty"`
		Partition    *int   `json:"partition,omitempty"`
		Offset       *int64 `json:"offset,omitempty"`
	}
	childDeleteBody struct {
		Parent string `json:"parent"`
		Child  string `json:"child"`
		Force  bool   `json:"force,omitempty"`
		// ExpectRemote is what the ingress node's replica showed when it
		// authorized the request: a remote child's delete needs admin or
		// the parent's owner, a local child's the owner of either side.
		ExpectRemote bool `json:"expect_remote,omitempty"`
	}
	topicDeleteBody struct {
		Topic        string `json:"topic"`
		Force        bool   `json:"force,omitempty"`
		ExpectRemote bool   `json:"expect_remote,omitempty"`
	}
	unshippedQuery struct {
		Topic   string `json:"topic"`
		TopicID string `json:"topic_id,omitempty"`
	}
	unshippedAnswer struct {
		Node     string `json:"node"`
		Count    uint64 `json:"count"`
		Complete bool   `json:"complete"`
	}
)

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

// remoteNamePattern is a remote's name rule (ch. 4.1).
var remoteNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// ServeWrite implements LinkLeader.
func (l *RemoteLinks) ServeWrite(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	ctx, refusal := l.actorContext(ctx, req.Actor)
	if refusal != nil {
		return *refusal
	}
	switch req.SubOp {
	case nodewire.RemoteSubAttach:
		return l.attach(ctx, req)
	case nodewire.RemoteSubPause, nodewire.RemoteSubResume, nodewire.RemoteSubSkip:
		return l.setState(ctx, req)
	case nodewire.RemoteSubDetach:
		return l.deleteChild(ctx, req)
	case nodewire.RemoteSubTopicDelete:
		return l.deleteTopic(ctx, req)
	}
	return errorResponse(http.StatusBadRequest, "unsupported remote write")
}

// actorContext is the context a remote child write runs under
// (leaderActorContext). Without an actor (security off on the ingress)
// the write runs with no identity.
func (l *RemoteLinks) actorContext(ctx context.Context, actor string) (context.Context, *nodewire.Response) {
	if actor == "" {
		return ctx, nil
	}
	return leaderActorContext(ctx, l.d.Store, l.d.Log, actor)
}

// leaderActorContext resolves the actor the ingress named as this node,
// the leader, knows it, looked up after the once-per-term leader
// barrier, as RPCServer.actorContext does for a forwarded topic write.
// A user the leader does not know is refused with 403 before anything
// runs. Every forwarded remote write, registry and children alike, goes
// through it, so the ingress replica's view of the caller never decides
// alone.
func leaderActorContext(ctx context.Context, store *metastore.Store, log *slog.Logger, actor string) (context.Context, *nodewire.Response) {
	if err := store.LeaderBarrier(ctx); err != nil {
		res := errorResponse(http.StatusServiceUnavailable, "control plane temporarily unavailable; retry")
		return nil, &res
	}
	u, err := store.GetUser(ctx, actor)
	if errors.Is(err, errs.ErrNotFound) {
		res := errorResponse(http.StatusForbidden, "caller unknown to the leader")
		return nil, &res
	}
	if err != nil {
		log.Error("remote write: look up the caller", "actor", actor, "err", err)
		res := errorResponse(http.StatusServiceUnavailable, "control plane temporarily unavailable; retry")
		return nil, &res
	}
	return security.WithIdentity(ctx, u), nil
}

// requireAdmin refuses (403) a request identity that is not an admin:
// attach, pause, resume and skip lend or steer the remote's credential.
// No identity (security off) passes; the ingress refuses those writes
// without security already.
func requireAdmin(ctx context.Context) *nodewire.Response {
	if id, ok := security.IdentityFrom(ctx); ok && !id.IsAdmin() {
		res := errorResponse(http.StatusForbidden, "admin privileges required")
		return &res
	}
	return nil
}

func decodeBody(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

func (b *childAttachBody) validate() error {
	if b.RemoteTopic == "" {
		b.RemoteTopic = b.Parent
	}
	for _, n := range []struct{ field, value string }{{"parent", b.Parent}, {"child", b.Child}, {"remote_topic", b.RemoteTopic}} {
		if err := topic.ValidateName(n.value); err != nil {
			return fmt.Errorf("%s: %v", n.field, err)
		}
	}
	switch {
	case !remoteNamePattern.MatchString(b.Remote):
		return errors.New("remote: must match " + remoteNamePattern.String())
	case b.Parent == b.Child:
		return errors.New("child: a topic cannot be its own child")
	case b.DelayMs < 0 || b.DelayMs > topic.MaxFanoutDelayMs:
		return fmt.Errorf("delay_ms must be between 0 and %d", topic.MaxFanoutDelayMs)
	case b.Lanes < 0 || b.Lanes > topic.MaxRemoteLanes:
		return fmt.Errorf("lanes must be between %d and %d", topic.MinRemoteLanes, topic.MaxRemoteLanes)
	case !topic.ValidRemoteFrom(b.From):
		return errors.New("from must be attach, unconsumed or earliest")
	}
	if b.From == "" {
		b.From = topic.RemoteFromAttach
	}
	if b.Lanes == 0 {
		b.Lanes = 1
	}
	return nil
}

// attach runs child.attach (ch. 4.2): the upgrade gate, the remote and
// the parent, the one-link-per-target rule, the checks from every
// member, the start offsets, then one Raft op. A dry run stops before
// the op and writes nothing.
func (l *RemoteLinks) attach(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var b childAttachBody
	if err := decodeBody(req.Body, &b); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid remote child request")
	}
	if err := b.validate(); err != nil {
		return errorResponse(http.StatusBadRequest, err.Error())
	}
	target := b.Parent + "/" + b.Child
	refuse := func(res nodewire.Response, class string) nodewire.Response {
		if !b.DryRun {
			l.audit("remote_child.create", req, target, "refused", class)
		}
		return res
	}
	if res := requireAdmin(ctx); res != nil {
		return refuse(*res, "denied")
	}
	if err := l.d.Plane.Checks.RequirePosture(ctx); err != nil {
		return refuse(postureResponse(err), "posture")
	}
	rec, err := l.d.Store.GetRemote(b.Remote)
	if errors.Is(err, errs.ErrNotFound) {
		return refuse(errorResponse(http.StatusBadRequest, "unknown remote "+strconv.Quote(b.Remote)), "remote_missing")
	}
	if err != nil {
		return refuse(l.internalError("read remote", err), "internal")
	}
	parent, err := l.d.Store.GetTopic(ctx, b.Parent)
	if errors.Is(err, errs.ErrNotFound) {
		return refuse(errorResponse(http.StatusNotFound, "parent topic not found"), "parent_missing")
	}
	if err != nil {
		return refuse(l.internalError("read parent", err), "internal")
	}
	if parent.IsChild() {
		return refuse(errorResponse(http.StatusConflict, "the parent is itself a child"), "role_conflict")
	}
	if linked, err := l.sameEndpointLink(rec.URL, b.RemoteTopic); err != nil {
		return refuse(l.internalError("list remote children", err), "internal")
	} else if linked != "" {
		return refuse(errorResponse(http.StatusConflict,
			fmt.Sprintf("this cluster already links to that remote topic through %q", linked)), topic.RemoteStateTargetHasRemoteChildren)
	}

	// Without a host allowlist answers are blind (ch. 5.8): a dry run
	// checks from the leader alone, and no answer carries what the
	// target said, only results and classes. A real attach still needs
	// every member's verdict.
	blind := !l.d.Plane.Checks.AllowlistConfigured()
	checks := l.d.Plane.Checks.CheckEverywhere
	if blind && b.DryRun {
		checks = l.d.Plane.Checks.CheckHere
	}
	reports, err := checks(ctx, l.checkRequest(ctx, b.Remote, b.RemoteTopic, parent, rec.CredentialVersion))
	if err != nil {
		return refuse(checkErrorAnswer(err, reports, rec.CredentialVersion, blind), "check")
	}
	targetID, err := remote.Verdict(reports, rec.CredentialVersion)
	if err != nil {
		return refuse(checkErrorAnswer(err, reports, rec.CredentialVersion, blind), "check")
	}

	warnings := attachWarnings(parent, reports, blind)
	offsets, err := l.d.Runner.AttachOffsetsMode(ctx, b.Parent, b.From)
	if err != nil {
		return refuse(errorResponse(http.StatusServiceUnavailable, "a parent partition owner could not be asked for its start offset; retry"), topic.RemoteStateUnavailable)
	}
	caps, capsOK := l.probeCapabilities(ctx, b.Remote, b.RemoteTopic)
	if capsOK && caps.MaxMessages <= sink.DefaultMaxChunkMessages {
		warnings = append(warnings, olderTargetBodyWarning)
	}
	if b.DryRun {
		shown := reports
		if blind {
			shown = remote.Blind(reports)
		}
		out := map[string]any{
			"dry_run":        true,
			"attach_offsets": offsets,
			"checks":         shown,
			"warnings":       warnings,
		}
		if capsOK && !blind {
			out["capabilities"] = map[string]any{"max_messages": caps.MaxMessages, "zstd": caps.Zstd}
		}
		return jsonResponse(http.StatusOK, out)
	}

	op := metastore.AttachRemoteChildOp{
		Parent: b.Parent, ParentID: parent.ID, Stub: b.Child, DelayMs: b.DelayMs, Offsets: offsets,
		Remote: topic.RemoteLink{
			Name: b.Remote, Topic: b.RemoteTopic, TargetID: targetID,
			From: b.From, Lanes: b.Lanes, CreatedBy: req.Actor,
		},
		Actor: req.Actor, RequestID: req.RequestID,
	}
	attacher, ok := l.d.Broker.(broker.RemoteChildAttacher)
	if !ok {
		return refuse(errorResponse(http.StatusNotImplemented, "remote children are not available on the leader"), "internal")
	}
	if err := attacher.AttachRemoteChild(ctx, op); err != nil {
		status, msg := l.linkError(err, b.Child)
		return refuse(errorResponse(status, msg), classOf(status))
	}
	stub, err := l.d.Store.GetTopic(ctx, b.Child)
	if err != nil {
		return l.internalError("read stub", err)
	}
	l.audit("remote_child.create", req, target, "committed", "",
		slog.String("remote", b.Remote), slog.String("remote_topic", b.RemoteTopic),
		slog.String("from", b.From), slog.Int("lanes", b.Lanes), slog.Int64("delay_ms", b.DelayMs))
	return jsonResponse(http.StatusCreated, stubWithWarnings{Topic: stub, Warnings: warnings})
}

// checkRequest is the check request of an attach and of a resume, which
// runs the attach checks again: the parent's ID for the target-is-source
// check and its current schema for the schema check, so both run on
// both paths.
func (l *RemoteLinks) checkRequest(ctx context.Context, remoteName, remoteTopic string, parent topic.Topic, credentialVersion uint64) remote.CheckRequest {
	req := remote.CheckRequest{
		Remote: remoteName, Topic: remoteTopic, Source: parent.Name, SourceID: parent.ID,
		CredentialVersion: credentialVersion,
	}
	if history, err := schema.PersistedHistory(ctx, l.d.Store, parent.Name); err == nil && len(history) > 0 {
		req.SourceSchema = history[len(history)-1].Raw
	}
	return req
}

// stubWithWarnings is the attach answer: the stub record, plus any
// warnings, flattened into one object.
type stubWithWarnings struct {
	topic.Topic
	Warnings []string `json:"warnings,omitempty"`
}

// attachWarnings are the attach's advisories: a parent retention below
// what a regional outage needs (Q15), a target too old to report remote
// children (loop detection waits for its upgrade), and the warnings the
// checks drew from the target's answers, left out when blind.
func attachWarnings(parent topic.Topic, reports []remote.NodeReport, blind bool) []string {
	warnings := []string{}
	if parent.RetentionMs > 0 && parent.RetentionMs < sink.RetentionWarnMs {
		warnings = append(warnings, fmt.Sprintf(
			"parent retention (%s) is below 72h; a remote outage longer than the retention loses records (drop-behind)",
			time.Duration(parent.RetentionMs)*time.Millisecond))
	}
	for _, r := range reports {
		if !r.TargetServesIDs {
			warnings = append(warnings, "the target does not report remote children (an older release, which cannot hold one): loop detection starts once it is upgraded")
			break
		}
	}
	if blind {
		return warnings
	}
	for _, r := range reports {
		warnings = append(warnings, r.Warnings...)
	}
	return warnings
}

// olderTargetBodyWarning is the attach's warning for a target whose
// batch produce takes 100 messages: v3.1.0, which caps a batch body at
// 1 MiB, while this cluster accepts records up to 1 MiB.
const olderTargetBodyWarning = "the target runs an older release whose batch produce takes at most 1 MiB of body: " +
	"a record over about 768 KiB (binary, sent as base64) or about 1 MiB (JSON) blocks the link as record_too_large until the target is upgraded"

// probeCapabilities asks the target, from the leader, what its batch
// produce takes; a dry run reports it.
func (l *RemoteLinks) probeCapabilities(ctx context.Context, remoteName, remoteTopic string) (sink.Capabilities, bool) {
	s := l.d.Runner.sender()
	e, _, state := s.entry(remoteName)
	if state != "" {
		return sink.Capabilities{}, false
	}
	pctx, cancel := context.WithTimeout(ctx, requestTimeout(e))
	defer cancel()
	caps, err := sink.ProbeCapabilities(pctx, e, remoteTopic, e.Limits().Compression == "zstd")
	return caps, err == nil
}

// sameEndpointLink returns a stub that already sends to remoteTopic on
// the same scheme, host and port as rawURL through any remote.
func (l *RemoteLinks) sameEndpointLink(rawURL, remoteTopic string) (string, error) {
	stubs, err := l.d.Store.RemoteChildren()
	if err != nil {
		return "", err
	}
	want, ok := endpointOf(rawURL)
	if !ok {
		return "", nil
	}
	for _, stub := range stubs {
		if stub.Remote.Topic != remoteTopic {
			continue
		}
		other, err := l.d.Store.GetRemote(stub.Remote.Name)
		if err != nil {
			continue
		}
		if got, ok := endpointOf(other.URL); ok && got == want {
			return stub.Parent + "/" + stub.Name, nil
		}
	}
	return "", nil
}

// endpointOf is a URL's scheme, host and port (443 when implicit).
func endpointOf(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Hostname()) + ":" + port, true
}

// setState runs child.pause, child.resume and child.skip: one
// field-scoped state op carrying the stub's attach epoch.
func (l *RemoteLinks) setState(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var b childStateBody
	if err := decodeBody(req.Body, &b); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid remote child request")
	}
	event := map[string]string{
		nodewire.RemoteSubPause: "remote_child.pause", nodewire.RemoteSubResume: "remote_child.resume",
		nodewire.RemoteSubSkip: "remote_child.skip",
	}[req.SubOp]
	target := b.Parent + "/" + b.Child
	if res := requireAdmin(ctx); res != nil {
		l.audit(event, req, target, "refused", "denied")
		return *res
	}
	stub, err := l.d.Store.GetTopic(ctx, b.Child)
	if err != nil || !stub.IsRemoteChild() || stub.Parent != b.Parent {
		return errorResponse(http.StatusNotFound, "remote child not found")
	}
	op := metastore.RemoteChildStateOp{Parent: b.Parent, Stub: b.Child, Epoch: stub.AttachEpoch, Actor: req.Actor, RequestID: req.RequestID}
	var attrs []slog.Attr
	switch req.SubOp {
	case nodewire.RemoteSubPause:
		if err := topic.ValidateRemotePauseReason(b.Reason); err != nil {
			return errorResponse(http.StatusBadRequest, err.Error())
		}
		op.Pause = &metastore.RemotePauseState{Paused: true, Reason: b.Reason, By: req.Actor, AtMs: l.now().UnixMilli()}
		attrs = append(attrs, slog.String("reason", b.Reason))
	case nodewire.RemoteSubResume:
		if err := l.d.Plane.Checks.RequirePosture(ctx); err != nil {
			l.audit(event, req, target, "refused", "posture")
			return postureResponse(err)
		}
		rec, err := l.d.Store.GetRemote(stub.Remote.Name)
		if err != nil {
			l.audit(event, req, target, "refused", topic.RemoteStateRemoteMissing)
			return errorResponse(http.StatusConflict, "remote "+strconv.Quote(stub.Remote.Name)+" does not exist; create it again first")
		}
		parent, err := l.d.Store.GetTopic(ctx, b.Parent)
		if err != nil {
			return errorResponse(http.StatusNotFound, "parent topic not found")
		}
		reports, err := l.d.Plane.Checks.CheckEverywhere(ctx, l.checkRequest(ctx, stub.Remote.Name, stub.Remote.Topic, parent, rec.CredentialVersion))
		if err == nil {
			var targetID string
			if targetID, err = remote.Verdict(reports, rec.CredentialVersion); err == nil {
				switch {
				case b.AcceptTarget:
					op.TargetID = &targetID
					event = "remote_child.accept_target"
				case stub.Remote.TargetID != "" && targetID != "" && targetID != stub.Remote.TargetID:
					l.audit(event, req, target, "refused", topic.RemoteStateTargetReplaced)
					return errorResponse(http.StatusConflict,
						"the target topic was replaced since the link was attached; resume with accept_target to send to the new one")
				}
			}
		}
		if err != nil {
			l.audit(event, req, target, "refused", "check")
			return checkErrorAnswer(err, reports, rec.CredentialVersion, !l.d.Plane.Checks.AllowlistConfigured())
		}
		op.Pause = &metastore.RemotePauseState{Paused: false}
	case nodewire.RemoteSubSkip:
		parent, err := l.d.Store.GetTopic(ctx, b.Parent)
		if err != nil {
			return errorResponse(http.StatusNotFound, "parent topic not found")
		}
		if b.Partition == nil || b.Offset == nil || *b.Partition < 0 || *b.Partition >= parent.Partitions || *b.Offset < 0 {
			return errorResponse(http.StatusBadRequest, "skip needs a parent partition and an offset >= 0")
		}
		if !topicsSkipEnabled {
			return errorResponse(http.StatusNotFound, "skip is not enabled")
		}
		op.Skip = &metastore.RemoteSkip{Partition: *b.Partition, Offset: *b.Offset}
		attrs = append(attrs, slog.Int("partition", *b.Partition), slog.Int64("offset", *b.Offset))
	}
	if req.SubOp != nodewire.RemoteSubResume {
		// The release gate alone (resume runs the full posture gate
		// above): it reads member records, so an emergency pause still
		// works with a member dead or unreachable.
		if err := l.d.Plane.Checks.RequireReleases(ctx); err != nil {
			l.audit(event, req, target, "refused", "posture")
			return postureResponse(err)
		}
	}
	if op.Skip != nil {
		if res, class := l.requireBlockedAt(ctx, b.Parent, b.Child, *op.Skip); res != nil {
			l.audit(event, req, target, "refused", class, attrs...)
			return *res
		}
	}
	if err := l.d.Store.SetRemoteChildState(ctx, op); err != nil {
		status, msg := l.linkError(err, b.Child)
		l.audit(event, req, target, "refused", classOf(status))
		return errorResponse(status, msg)
	}
	l.audit(event, req, target, "committed", "", attrs...)
	updated, err := l.d.Store.GetTopic(ctx, b.Child)
	if err != nil {
		return l.internalError("read stub", err)
	}
	return jsonResponse(http.StatusOK, updated)
}

// requireBlockedAt refuses a skip unless the partition's owner reports
// the link's cursor blocked on exactly that record, as rejected_record
// or record_too_large. A skip is an admin decision about one record the
// target refused; a stored skip for any other offset would drop a record
// later with no decision, the first time the target refused it. With
// several lanes blocked, the owner reports the lowest blocked record, so
// the admin skips them in order. 409 names what the cursor is blocked
// on; 503 when the owner could not be asked.
func (l *RemoteLinks) requireBlockedAt(ctx context.Context, parent, child string, skip metastore.RemoteSkip) (*nodewire.Response, string) {
	local, err := l.d.Broker.FanoutCursorStats(ctx, parent)
	if err != nil {
		res := errorResponse(http.StatusServiceUnavailable, "the partition's cursor could not be read; retry")
		return &res, topic.RemoteStateUnavailable
	}
	var peer peerClient
	if l.d.Runner != nil {
		local = l.d.Runner.OverlayRemoteCursorStats(parent, local)
		peer = l.d.Runner.peer
	}
	stats, complete, err := collectOwnerCursorStats(ctx, l.d.Store, peer, l.d.SelfID, parent, local)
	if err != nil {
		res := errorResponse(http.StatusServiceUnavailable, "the partition's owner could not be asked; retry")
		return &res, topic.RemoteStateUnavailable
	}
	for _, st := range stats {
		if st.Child != child || st.Partition != skip.Partition {
			continue
		}
		b := st.BlockedAt
		if b != nil && b.Partition == skip.Partition && b.Offset == skip.Offset &&
			(b.State == topic.RemoteStateRejectedRecord || b.State == topic.RemoteStateRecordTooLarge) {
			return nil, ""
		}
		msg := fmt.Sprintf("the link's cursor of partition %d is not blocked on offset %d", skip.Partition, skip.Offset)
		if b != nil {
			msg += fmt.Sprintf("; it is blocked on offset %d (%s)", b.Offset, b.State)
		} else if st.State != "" {
			msg += " (state " + st.State + ")"
		}
		res := jsonResponse(http.StatusConflict, map[string]any{"error": msg, "blocked_at": b})
		return &res, "not_blocked"
	}
	if complete {
		// Every owner answered and none holds a cursor of the partition
		// yet: nothing is blocked.
		res := jsonResponse(http.StatusConflict, map[string]any{"error": fmt.Sprintf(
			"the link's cursor of partition %d is not blocked on offset %d; it has not started", skip.Partition, skip.Offset), "blocked_at": nil})
		return &res, "not_blocked"
	}
	res := errorResponse(http.StatusServiceUnavailable, "the partition's owner could not be asked; retry")
	return &res, topic.RemoteStateUnavailable
}

// topicsSkipEnabled builds the skip route (Q12). False answers 404.
const topicsSkipEnabled = true

// deleteChild runs child.delete: a local child detaches as before; a
// remote child's link (and with it its stub) goes only when nothing is
// unshipped, or with force. Both are the topic manager's DetachChild,
// under the caller's identity.
func (l *RemoteLinks) deleteChild(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var b childDeleteBody
	if err := decodeBody(req.Body, &b); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid delete request")
	}
	child, err := l.d.Store.GetTopic(ctx, b.Child)
	if err != nil || !child.IsChild() || child.Parent != b.Parent {
		return errorResponse(http.StatusNotFound, "child not attached to that parent")
	}
	if !child.IsRemoteChild() {
		if err := l.d.Broker.DetachChild(ctx, b.Parent, b.Child); err != nil {
			status, msg := l.linkError(err, b.Child)
			return errorResponse(status, msg)
		}
		return nodewire.Response{Status: http.StatusNoContent}
	}
	target := b.Parent + "/" + b.Child
	if !b.ExpectRemote {
		return errorResponse(http.StatusConflict, "remote child changed since this node checked the request; retry")
	}
	parent, err := l.d.Store.GetTopic(ctx, b.Parent)
	if err != nil {
		return errorResponse(http.StatusNotFound, "parent topic not found")
	}
	var report unshippedReport
	if !b.Force {
		msg := "remote child " + strconv.Quote(b.Child) + " has unshipped records"
		if res, ok := l.refuseUnshipped(ctx, req, "remote_child.delete", target, msg, parent, []string{b.Child}, &report); !ok {
			return res
		}
	} else if r, err := l.unshippedNow(ctx, parent); err == nil {
		report = r
	}
	if err := l.d.Broker.DetachChild(ctx, b.Parent, b.Child); err != nil {
		status, msg := l.linkError(err, b.Child)
		l.audit("remote_child.delete", req, target, "refused", classOf(status))
		return errorResponse(status, msg)
	}
	l.audit("remote_child.delete", req, target, "committed", "", report.abandonedAttrs(b.Force, []string{b.Child})...)
	return nodewire.Response{Status: http.StatusNoContent}
}

// deleteTopic runs topic.delete, which the ingress sends for a stub or a
// parent with remote children as its replica shows them. Only a stub,
// or a parent with remote children, as the leader sees it now, runs the
// unshipped check; any other topic deletes exactly as before. The
// delete itself is the topic manager's, under the caller's identity: a
// parent's delete takes its stubs with it in the same entry, and the
// purge fan-out names the incarnation the delete removed.
func (l *RemoteLinks) deleteTopic(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	var b topicDeleteBody
	if err := decodeBody(req.Body, &b); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid delete request")
	}
	t, err := l.d.Store.GetTopic(ctx, b.Topic)
	if errors.Is(err, errs.ErrNotFound) {
		return errorResponse(http.StatusNotFound, "topic not found")
	}
	if err != nil {
		return l.internalError("read topic", err)
	}
	parentName, stubs := b.Topic, l.remoteChildNames(ctx, t)
	if t.IsRemoteChild() {
		parentName, stubs = t.Parent, []string{t.Name}
	}
	remoteLinked := len(stubs) > 0
	if remoteLinked && !b.ExpectRemote {
		return errorResponse(http.StatusConflict, "topic changed since this node checked the request; retry")
	}
	var report unshippedReport
	if remoteLinked {
		parent, err := l.d.Store.GetTopic(ctx, parentName)
		if err != nil {
			return errorResponse(http.StatusNotFound, "parent topic not found")
		}
		if !b.Force {
			msg := "topic " + strconv.Quote(b.Topic) + " has remote children with unshipped records"
			if t.IsRemoteChild() {
				msg = "remote child " + strconv.Quote(b.Topic) + " has unshipped records"
			}
			if res, ok := l.refuseUnshipped(ctx, req, "topic.delete", b.Topic, msg, parent, stubs, &report); !ok {
				return res
			}
		} else if r, err := l.unshippedNow(ctx, parent); err == nil {
			report = r
		}
	}
	id, err := deleteTopicReportingID(ctx, l.d.Broker, b.Topic)
	if err != nil {
		if _, ok := errors.AsType[brokertopics.PurgeError](err); !ok {
			status, msg := l.linkError(err, b.Topic)
			if remoteLinked {
				l.audit("topic.delete", req, b.Topic, "refused", classOf(status))
			}
			return errorResponse(status, msg)
		}
		l.d.Log.Warn("topic deleted but local purge failed; the startup orphan sweep reclaims the directory", "topic", b.Topic)
	}
	if router := l.d.Plane.router; router != nil {
		// The router detaches the fan-out from the request and logs the
		// members that still owe the purge.
		_ = router.BroadcastDeleteTopic(ctx, b.Topic, id)
	}
	if remoteLinked {
		l.audit("topic.delete", req, b.Topic, "committed", "", report.abandonedAttrs(b.Force, stubs)...)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// remoteChildNames lists t's children that are stubs.
func (l *RemoteLinks) remoteChildNames(ctx context.Context, t topic.Topic) []string {
	var out []string
	for _, name := range t.Children {
		if c, err := l.d.Store.GetTopic(ctx, name); err == nil && c.IsRemoteChild() {
			out = append(out, name)
		}
	}
	return out
}

// unshippedReport is one unshipped check's result for a parent: the
// cursor lag of each of its remote children, and each member's ingress
// backlog of the parent's records.
type unshippedReport struct {
	lag          map[string]int64
	lagComplete  map[string]bool
	backlog      map[string]uint64
	notAnswering []string
	scanCapped   []string
}

// unshipped reports whether any of stubs has records not yet on its
// remote: cursor lag above 0 or incomplete, any member's backlog of the
// parent above 0 or past the scan limit, or any member not answering.
func (u unshippedReport) unshipped(stubs []string) bool {
	for _, s := range stubs {
		if u.lag[s] > 0 || !u.lagComplete[s] {
			return true
		}
	}
	for _, n := range u.backlog {
		if n > 0 {
			return true
		}
	}
	return len(u.notAnswering) > 0 || len(u.scanCapped) > 0
}

func (u unshippedReport) totals(stubs []string) (lag int64, complete bool) {
	complete = true
	for _, s := range stubs {
		lag += u.lag[s]
		complete = complete && u.lagComplete[s]
	}
	return lag, complete
}

// body is the 409 answer's counts.
func (u unshippedReport) body(msg string, stubs []string) map[string]any {
	lag, complete := u.totals(stubs)
	backlog := map[string]uint64{}
	for node, n := range u.backlog {
		if n > 0 {
			backlog[node] = n
		}
	}
	out := map[string]any{
		"error":            msg,
		"lag_messages":     lag,
		"lag_complete":     complete,
		"dispatch_backlog": backlog,
	}
	if len(u.notAnswering) > 0 {
		out["not_answering"] = u.notAnswering
	}
	if len(u.scanCapped) > 0 {
		out["backlog_over_scan_limit"] = u.scanCapped
	}
	return out
}

// abandonedAttrs are the audit line's counts of what a forced delete
// abandoned.
func (u unshippedReport) abandonedAttrs(forced bool, stubs []string) []slog.Attr {
	lag, complete := u.totals(stubs)
	var backlog uint64
	for _, n := range u.backlog {
		backlog += n
	}
	return []slog.Attr{
		slog.Bool("forced", forced),
		slog.Int64("abandoned_lag_messages", lag),
		slog.Bool("lag_complete", complete),
		slog.Uint64("abandoned_dispatch_backlog", backlog),
		slog.Int("members_not_answering", len(u.notAnswering)),
	}
}

// refuseUnshipped runs the unshipped check for a delete and, when
// something is unshipped (or the check cannot run now), returns the
// answer and false. report receives the check's result either way.
func (l *RemoteLinks) refuseUnshipped(ctx context.Context, req nodewire.RemoteWriteRequest, event, target, msg string, parent topic.Topic, stubs []string, report *unshippedReport) (nodewire.Response, bool) {
	r, err := l.unshippedChecked(ctx, parent)
	if err != nil {
		var throttled *unshippedThrottled
		if errors.As(err, &throttled) {
			l.audit(event, req, target, "refused", topic.RemoteStateThrottled)
			secs := int((throttled.retryAfter + time.Second - 1) / time.Second)
			return jsonResponse(http.StatusTooManyRequests, map[string]any{
				"error":               "an unshipped check for this parent ran less than 10s ago; retry",
				"retry_after_seconds": secs,
			}), false
		}
		l.audit(event, req, target, "refused", topic.RemoteStateUnavailable)
		return errorResponse(http.StatusServiceUnavailable, "the unshipped check could not run; retry"), false
	}
	*report = r
	if !r.unshipped(stubs) {
		return nodewire.Response{}, true
	}
	l.audit(event, req, target, "refused", "unshipped", r.abandonedAttrs(false, stubs)...)
	return jsonResponse(http.StatusConflict, r.body(msg, stubs)), false
}

// unshippedThrottled is a check refused because one ran for the parent
// less than checkEvery ago.
type unshippedThrottled struct{ retryAfter time.Duration }

func (e *unshippedThrottled) Error() string { return "unshipped check throttled" }

type unshippedFlight struct {
	// seq is the flight's l.flightSeq: a check that started after a
	// caller arrived has a seq above the one the caller saw.
	seq    uint64
	done   chan struct{}
	report unshippedReport
	err    error
}

// unshippedChecked runs the parent's unshipped check under the bounded
// cost rules: one check per parent at a time, and a new one at most
// every checkEvery. A concurrent delete waits for the running check and
// shares its answer only when that check started after the delete
// arrived; a check already running may have scanned the backlogs before
// a record the delete must count was accepted. Otherwise the delete
// waits for it to end and then shares the next one, which the first
// such waiter starts, exempt from the throttle because it waited.
func (l *RemoteLinks) unshippedChecked(ctx context.Context, parent topic.Topic) (unshippedReport, error) {
	l.mu.Lock()
	arrived := l.flightSeq
	waited := false
	for {
		if f := l.flights[parent.Name]; f != nil {
			l.mu.Unlock()
			select {
			case <-f.done:
			case <-ctx.Done():
				return unshippedReport{}, ctx.Err()
			}
			if f.seq > arrived {
				return f.report, f.err
			}
			waited = true
			l.mu.Lock()
			continue
		}
		now := l.now()
		if last, ok := l.lastCheck[parent.Name]; ok && now.Sub(last) < l.checkEvery && !waited {
			l.mu.Unlock()
			return unshippedReport{}, &unshippedThrottled{retryAfter: l.checkEvery - now.Sub(last)}
		}
		l.flightSeq++
		f := &unshippedFlight{seq: l.flightSeq, done: make(chan struct{})}
		l.flights[parent.Name] = f
		l.lastCheck[parent.Name] = now
		l.mu.Unlock()

		// The check is shared, so it runs detached from the delete that
		// started it, bounded on its own: that delete's client going
		// away ends only its own wait below, never the answer every
		// other waiter gets.
		go func() {
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unshippedFlightTimeout)
			defer cancel()
			f.report, f.err = l.unshippedNow(fctx, parent)
			l.mu.Lock()
			delete(l.flights, parent.Name)
			if f.err != nil {
				// Only a check that answered paces the next one.
				delete(l.lastCheck, parent.Name)
			}
			l.mu.Unlock()
			close(f.done)
		}()
		select {
		case <-f.done:
			return f.report, f.err
		case <-ctx.Done():
			return unshippedReport{}, ctx.Err()
		}
	}
}

// unshippedNow runs the check: every member's backlog of the parent
// from the unshipped query first, then the remote children's cursor lag
// from every parent partition owner. The order is what makes the check
// whole. A record leaves a member's backlog (its dispatch checkpoint
// moves past it) only after it is committed to the parent, and a
// committed record is under the high watermark from then on. So a
// record the backlog scan missed because it was dispatched mid-check is
// counted by the lag read that follows; in the other order it could
// fall between the two reads.
func (l *RemoteLinks) unshippedNow(ctx context.Context, parent topic.Topic) (unshippedReport, error) {
	r := unshippedReport{lag: map[string]int64{}, lagComplete: map[string]bool{}, backlog: map[string]uint64{}}
	members, err := l.d.Plane.Members()
	if err != nil {
		return r, err
	}
	query := mustJSON(unshippedQuery{Topic: parent.Name, TopicID: parent.ID})
	askCtx, cancel := context.WithTimeout(ctx, unshippedAskTimeout)
	defer cancel()
	type answer struct {
		node string
		a    unshippedAnswer
		ok   bool
	}
	answers := make([]answer, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Go(func() {
			answers[i].node = m.ID
			res, err := l.d.Plane.SendCheck(askCtx, m, nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckUnshipped, Body: query})
			if err != nil || res.Status != http.StatusOK {
				return
			}
			if json.Unmarshal(res.Body, &answers[i].a) == nil {
				answers[i].ok = true
			}
		})
	}
	wg.Wait()
	for _, a := range answers {
		switch {
		case !a.ok:
			r.notAnswering = append(r.notAnswering, a.node)
		case !a.a.Complete:
			r.scanCapped = append(r.scanCapped, a.node)
			r.backlog[a.node] = a.a.Count
		default:
			r.backlog[a.node] = a.a.Count
		}
	}
	sort.Strings(r.notAnswering)
	sort.Strings(r.scanCapped)

	// Only now, with every backlog read, the cursor lag.
	stats, complete, err := l.collectCursorStats(ctx, parent.Name)
	if err != nil {
		return r, err
	}
	// Each partition counts once: the lag is whole only when every
	// partition 0..Partitions-1 has exactly one stat, from its owner. A
	// partition reported twice is not counted, so it can never stand in
	// for one nobody reported.
	seen := map[string]map[int]int{}
	for _, st := range stats {
		if seen[st.Child] == nil {
			seen[st.Child] = map[int]int{}
		}
		seen[st.Child][st.Partition]++
	}
	for _, st := range stats {
		if seen[st.Child][st.Partition] == 1 {
			r.lag[st.Child] += max(0, st.HighWatermark-st.NextOffset)
		}
	}
	for _, child := range parent.Children {
		whole := complete
		for p := 0; whole && p < parent.Partitions; p++ {
			whole = seen[child][p] == 1
		}
		r.lagComplete[child] = whole
	}
	return r, nil
}

// collectCursorStats merges the fan-out cursor stats of every owner of
// the parent's partitions; complete is false when some owner could not
// be asked.
func (l *RemoteLinks) collectCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, bool, error) {
	local, err := l.d.Broker.FanoutCursorStats(ctx, parent)
	if err != nil {
		return nil, false, err
	}
	var peer peerClient
	if l.d.Runner != nil {
		peer = l.d.Runner.peer
	}
	return collectOwnerCursorStats(ctx, l.d.Store, peer, l.d.SelfID, parent, local)
}

// ServeUnshipped implements LinkLeader on every member: this node's
// ingress backlog of one topic's records, scanned for each query. An
// answer is never reused across checks: a scan that began before a
// check started can miss a record accepted in between, and the check
// would then let a delete abandon it. The leader's per-parent single
// flight and its 10 s pacing bound how often members scan.
func (l *RemoteLinks) ServeUnshipped(_ context.Context, req nodewire.RemoteCheckRequest) nodewire.Response {
	var q unshippedQuery
	if err := decodeBody(req.Body, &q); err != nil || topic.ValidateName(q.Topic) != nil {
		return errorResponse(http.StatusBadRequest, "invalid unshipped query")
	}
	count, complete, err := l.d.Ingress.PendingForTopic(q.TopicID, q.Topic, l.scanLimit)
	if err != nil {
		l.d.Log.Warn("unshipped query: scan the ingress backlog", "topic", q.Topic, "err", err)
		return errorResponse(http.StatusServiceUnavailable, "ingress backlog scan failed")
	}
	return jsonResponse(http.StatusOK, unshippedAnswer{Node: l.d.SelfID, Count: count, Complete: complete})
}

// linkError maps a write's failure to a status and a fixed message: the
// remote child conflicts carry texts this package built from validated
// names; everything else gets a message per sentinel and never the
// error's own text.
func (l *RemoteLinks) linkError(err error, name string) (int, string) {
	switch {
	case errors.Is(err, errs.ErrRemoteChildConflict):
		return http.StatusConflict, err.Error()
	case errors.Is(err, metastore.ErrEntryTypeNotYetUsable):
		// The gate passed but a member joined or reported an older
		// release before the proposal: nothing was proposed.
		return http.StatusPreconditionFailed, releaseGateError(err).message()
	case errors.Is(err, errs.ErrTopicChanged):
		return http.StatusConflict, "topic changed since it was read; retry"
	case errors.Is(err, errs.ErrForbidden):
		return http.StatusForbidden, "the caller may not change this topic"
	case errors.Is(err, errs.ErrAlreadyExists):
		return http.StatusConflict, "a topic named " + strconv.Quote(name) + " already exists"
	case errors.Is(err, errs.ErrFanoutRoleConflict):
		return http.StatusConflict, "fan-out role conflict: the parent is itself a child, or the child is already linked"
	case errors.Is(err, errs.ErrFanoutChildLimit):
		return http.StatusConflict, "the parent has the maximum number of children"
	case errors.Is(err, errs.ErrFanoutDelayTooLong):
		return http.StatusConflict, "the parent's retention cannot buffer the delay (retention must be at least delay + 1h)"
	case errors.Is(err, errs.ErrNotFound), errors.Is(err, errs.ErrTopicNotFound):
		return http.StatusNotFound, "not found"
	case errors.Is(err, errs.ErrInvalidArgument):
		return http.StatusBadRequest, "invalid remote child request"
	case errors.Is(err, errs.ErrUnavailable), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusServiceUnavailable, "control plane temporarily unavailable; retry"
	}
	l.d.Log.Error("remote child write failed", "err", err)
	return http.StatusInternalServerError, "remote child write failed"
}

func (l *RemoteLinks) internalError(op string, err error) nodewire.Response {
	l.d.Log.Error("remote child: "+op, "err", err)
	return errorResponse(http.StatusInternalServerError, "remote child write failed")
}

// classOf is the audit class of a refusal status.
func classOf(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid"
	case http.StatusForbidden:
		return "denied"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusPreconditionFailed:
		return "precondition"
	case http.StatusTooManyRequests:
		return topic.RemoteStateThrottled
	case http.StatusServiceUnavailable:
		return topic.RemoteStateUnavailable
	}
	return "internal"
}

// postureResponse answers the upgrade gate's refusal with 412.
func postureResponse(err error) nodewire.Response {
	var posture *PostureError
	if errors.As(err, &posture) {
		return jsonResponse(http.StatusPreconditionFailed, map[string]any{"error": posture.message(), "members": posture.Members})
	}
	switch {
	case errors.Is(err, errs.ErrRemotePosture):
		return errorResponse(http.StatusPreconditionFailed, "a member reports a security posture that forbids remotes: "+err.Error())
	case errors.Is(err, errs.ErrRemoteFeatureGate):
		return errorResponse(http.StatusPreconditionFailed, "a member runs an older release or is unreachable: "+err.Error())
	}
	return errorResponse(http.StatusServiceUnavailable, "the upgrade gate could not be checked; retry")
}

// checkErrorAnswer is checkErrorResponse, blind without a host
// allowlist (ch. 5.8): the class and the members that failed, not each
// member's report.
func checkErrorAnswer(err error, reports []remote.NodeReport, credentialVersion uint64, blind bool) nodewire.Response {
	var ce *remote.CheckError
	if !blind || !errors.As(err, &ce) {
		return checkErrorResponse(err, reports)
	}
	if ce.Reports != nil {
		reports = ce.Reports
	}
	return jsonResponse(ce.Status, map[string]any{
		"error": "remote check failed: " + ce.Class, "class": ce.Class, "members": remote.Failing(reports, credentialVersion),
	})
}

// checkErrorResponse answers a failed check with its status and the
// per-member reports (classes only, never the target's text).
func checkErrorResponse(err error, reports []remote.NodeReport) nodewire.Response {
	var ce *remote.CheckError
	if errors.As(err, &ce) {
		if ce.Reports != nil {
			reports = ce.Reports
		}
		return jsonResponse(ce.Status, map[string]any{
			"error": "remote check failed: " + ce.Class, "class": ce.Class, "checks": reports,
		})
	}
	switch {
	case errors.Is(err, errs.ErrRemoteThrottled):
		return errorResponse(http.StatusTooManyRequests, "a check for this remote is already running or ran less than 5s ago; retry")
	case errors.Is(err, errs.ErrRemoteFeatureGate), errors.Is(err, errs.ErrRemotePosture):
		return postureResponse(err)
	}
	return errorResponse(http.StatusServiceUnavailable, "the remote checks could not run; retry")
}

// audit writes the leader's authoritative audit line for one proposal.
func (l *RemoteLinks) audit(event string, req nodewire.RemoteWriteRequest, target, outcome, class string, attrs ...slog.Attr) {
	remote.LeaderAudit(l.d.Log, remote.AuditEvent{
		Event: event, Actor: req.Actor, RequestID: req.RequestID, Target: target,
		Outcome: outcome, Class: class, Attrs: attrs,
	})
}
