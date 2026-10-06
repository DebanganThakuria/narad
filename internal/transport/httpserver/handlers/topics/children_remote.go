package topics

// Remote children: a fan-out child whose records go to a topic on
// another Narad cluster.
//
//	POST   /v1/topics/{parent}/children                  {"child","remote",...}
//	POST   /v1/topics/{parent}/children/{child}/pause    {"reason"}
//	POST   /v1/topics/{parent}/children/{child}/resume   {"accept_target"}
//	POST   /v1/topics/{parent}/children/{child}/skip     {"partition","offset"}
//	DELETE /v1/topics/{parent}/children/{child}[?force=true]
//
// Creating a remote child lends the remote's credential to the link:
// the target authorizes the writes by the replicator user's grants, not
// the caller's, so attach, pause, resume and skip are admin only, with
// security on, checked before any lookup. Deleting stops data leaving
// and lends nothing: an admin or the parent's owner may, security on.
// A stub has no owner and its record never grants anything.
//
// Every write travels to the leader as a remote write (OpRemoteWrite)
// carrying the caller's name and a request ID. Each request writes one
// audit line here, on the node the client called, refusals included
// (handlers.AuditWriter, as every admin mutation does); the request ID
// joins it to the leader's line.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// attachRemoteChildRequest is the attach body, decoded strictly for both
// kinds of child: with Remote it attaches a remote child; without it the
// request is today's local attach and the remote fields must be absent.
type attachRemoteChildRequest struct {
	Child string `json:"child"`
	// DelayMs, when positive, makes the child a delay child.
	DelayMs int64 `json:"delay_ms,omitempty"`
	// Remote names a remote from the registry; its presence makes this
	// a remote attach.
	Remote string `json:"remote,omitempty"`
	// RemoteTopic is the topic on the remote; the parent's name when
	// empty.
	RemoteTopic string `json:"remote_topic,omitempty"`
	// From, Lanes and DryRun are optional (Q16): the start point
	// (attach, unconsumed or earliest), the lanes per parent partition
	// (1 to 8), and a check-only run that writes nothing.
	From   string `json:"from,omitempty"`
	Lanes  int    `json:"lanes,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

// local returns the request as a local attach, refusing remote fields
// without a remote.
func (r attachRemoteChildRequest) local() (attachChildRequest, error) {
	if r.RemoteTopic != "" || r.From != "" || r.Lanes != 0 || r.DryRun {
		return attachChildRequest{}, errors.New("remote_topic, from, lanes and dry_run apply to a remote child only (name a remote)")
	}
	return attachChildRequest{Child: r.Child, DelayMs: r.DelayMs}, nil
}

// validate applies the name rules to every value that becomes a record
// name or a path segment on the target.
func (r *attachRemoteChildRequest) validate(parent string) error {
	if r.RemoteTopic == "" {
		r.RemoteTopic = parent
	}
	for _, f := range []struct{ name, value string }{{"parent", parent}, {"child", r.Child}, {"remote_topic", r.RemoteTopic}} {
		if err := topic.ValidateName(f.value); err != nil {
			return fmt.Errorf("%s: %v", f.name, err)
		}
	}
	switch {
	case !remoteNamePattern(r.Remote):
		return errors.New("remote: must be a remote's name (a lowercase letter, then up to 62 lowercase letters, digits or '-')")
	case parent == r.Child:
		return errors.New("child: a topic cannot be its own child")
	case r.DelayMs < 0 || r.DelayMs > topic.MaxFanoutDelayMs:
		return fmt.Errorf("delay_ms must be between 0 and %d (1 year)", topic.MaxFanoutDelayMs)
	case r.Lanes < 0 || r.Lanes > topic.MaxRemoteLanes:
		return fmt.Errorf("lanes must be between %d and %d", topic.MinRemoteLanes, topic.MaxRemoteLanes)
	case !topic.ValidRemoteFrom(r.From):
		return errors.New("from must be attach, unconsumed or earliest")
	}
	return nil
}

// remoteNamePattern is a remote's name rule, ^[a-z][a-z0-9-]{0,62}$.
func remoteNamePattern(name string) bool {
	if name == "" || len(name) > 63 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// canAttachRemote is the remote attach rule (Q9): admin only, security
// on. It runs before any lookup, and never falls back to the owner rules
// of a local attach.
func canAttachRemote(s *handlers.Set, w http.ResponseWriter, r *http.Request) (user.User, bool) {
	return s.RequireSecuredAdmin(w, r)
}

func attachRemote(s *handlers.Set, w http.ResponseWriter, r *http.Request, parent string, req attachRemoteChildRequest) {
	handlers.SetNoStore(w)
	requestID := remote.NewRequestID()
	target := parent + "/" + req.Child
	aw := handlers.NewAuditWriter(w)
	w = aw
	defer aw.Audit(s, r, eventRemoteChildCreate, target, "request_id", requestID, "remote", req.Remote, "dry_run", req.DryRun)
	caller, ok := canAttachRemote(s, w, r)
	if !ok {
		return
	}
	if !remotePlaneWired(s, w) {
		return
	}
	if err := req.validate(parent); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	body := map[string]any{
		"parent": parent, "child": req.Child, "remote": req.Remote, "remote_topic": req.RemoteTopic,
		"delay_ms": req.DelayMs, "from": req.From, "lanes": req.Lanes, "dry_run": req.DryRun,
	}
	sendRemoteChildWrite(s, w, r, nodewire.RemoteSubAttach, caller.Username, requestID, body, attachTimeout)
}

// Audit events of the remote child writes.
const (
	eventRemoteChildCreate = "remote_child.create"
	eventRemoteChildPause  = "remote_child.pause"
	eventRemoteChildResume = "remote_child.resume"
	eventRemoteChildSkip   = "remote_child.skip"
)

// attachTimeout bounds an attach: every member's checks (15 s) plus the
// start offsets and the Raft op.
const attachTimeout = 45 * time.Second

type pauseRequest struct {
	Reason string `json:"reason,omitempty"`
}

type resumeRequest struct {
	AcceptTarget bool `json:"accept_target,omitempty"`
}

type skipRequest struct {
	Partition *int   `json:"partition"`
	Offset    *int64 `json:"offset"`
}

// PauseChild handles POST /v1/topics/{parent}/children/{child}/pause.
func PauseChild(s *handlers.Set) http.HandlerFunc {
	return childStateHandler(s, nodewire.RemoteSubPause, eventRemoteChildPause, func(w http.ResponseWriter, r *http.Request, body map[string]any) bool {
		var req pauseRequest
		if !decodeOptionalBody(s, w, r, &req) {
			return false
		}
		if err := topic.ValidateRemotePauseReason(req.Reason); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return false
		}
		body["reason"] = req.Reason
		return true
	})
}

// ResumeChild handles POST /v1/topics/{parent}/children/{child}/resume.
// accept_target also records the target's current ID: how an admin
// accepts a recreated target topic.
func ResumeChild(s *handlers.Set) http.HandlerFunc {
	return childStateHandler(s, nodewire.RemoteSubResume, eventRemoteChildResume, func(w http.ResponseWriter, r *http.Request, body map[string]any) bool {
		var req resumeRequest
		if !decodeOptionalBody(s, w, r, &req) {
			return false
		}
		body["accept_target"] = req.AcceptTarget
		return true
	})
}

// SkipChild handles POST /v1/topics/{parent}/children/{child}/skip: the
// cursor drops the named record only while it is stuck on exactly that
// offset in rejected_record or record_too_large.
func SkipChild(s *handlers.Set) http.HandlerFunc {
	return childStateHandler(s, nodewire.RemoteSubSkip, eventRemoteChildSkip, func(w http.ResponseWriter, r *http.Request, body map[string]any) bool {
		var req skipRequest
		if !s.DecodeJSON(w, r, &req) {
			return false
		}
		if req.Partition == nil || req.Offset == nil || *req.Partition < 0 || *req.Offset < 0 {
			s.WriteError(w, http.StatusBadRequest, "partition and offset (both >= 0) are required")
			return false
		}
		body["partition"], body["offset"] = *req.Partition, *req.Offset
		return true
	})
}

// childStateHandler is the shared shape of pause, resume and skip:
// admin only, names checked, the body read by fill, one remote write.
func childStateHandler(s *handlers.Set, subOp, event string, fill func(http.ResponseWriter, *http.Request, map[string]any) bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlers.SetNoStore(w)
		parent, child := r.PathValue("parent"), r.PathValue("child")
		requestID := remote.NewRequestID()
		target := parent + "/" + child
		aw := handlers.NewAuditWriter(w)
		w = aw
		defer aw.Audit(s, r, event, target, "request_id", requestID)
		caller, ok := s.RequireSecuredAdmin(w, r)
		if !ok {
			return
		}
		if !remotePlaneWired(s, w) {
			return
		}
		if topic.ValidateName(parent) != nil || topic.ValidateName(child) != nil {
			s.WriteError(w, http.StatusBadRequest, "parent and child must be topic names")
			return
		}
		body := map[string]any{"parent": parent, "child": child}
		if !fill(w, r, body) {
			return
		}
		timeout := stateTimeout
		if subOp == nodewire.RemoteSubResume {
			timeout = attachTimeout // resume re-runs the checks from every member
		}
		sendRemoteChildWrite(s, w, r, subOp, caller.Username, requestID, body, timeout)
	}
}

// stateTimeout bounds a pause or a skip: one Raft op on the leader.
const stateTimeout = 15 * time.Second

// decodeOptionalBody decodes a JSON body strictly when there is one.
func decodeOptionalBody(s *handlers.Set, w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.ContentLength == 0 {
		return true
	}
	return s.DecodeJSON(w, r, dst)
}

// remoteChildWritesPerMinute bounds remote child writes (attach, pause,
// resume, skip) per node: each is a Raft entry, and attach and resume
// call every member and the remote.
const remoteChildWritesPerMinute = 60

// writeLimiters holds one limiter per handler set (one per node).
var writeLimiters sync.Map

type writeLimiter struct {
	mu     sync.Mutex
	window time.Time
	count  int
}

// allowRemoteChildWrite takes one write from the node's minute budget,
// or answers 429 with Retry-After.
func allowRemoteChildWrite(s *handlers.Set, w http.ResponseWriter) bool {
	v, _ := writeLimiters.LoadOrStore(s, &writeLimiter{})
	l := v.(*writeLimiter)
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.window) >= time.Minute {
		l.window, l.count = now, 0
	}
	if l.count >= remoteChildWritesPerMinute {
		retry := int((time.Minute - now.Sub(l.window) + time.Second - 1) / time.Second)
		w.Header().Set("Retry-After", strconv.Itoa(max(retry, 1)))
		s.WriteError(w, http.StatusTooManyRequests, "too many remote child writes on this node; retry later")
		return false
	}
	l.count++
	return true
}

// errLeaderTooOldText is cluster.ErrLeaderTooOld's text: this package
// cannot import cluster, so the plane's sentinel is recognized by it.
const errLeaderTooOldText = "does not serve remote writes"

func leaderTooOld(err error) bool {
	return err != nil && strings.Contains(err.Error(), errLeaderTooOldText)
}

// remotePlaneWired answers 501 on a node without the remote plane.
func remotePlaneWired(s *handlers.Set, w http.ResponseWriter) bool {
	if s.Deps.Remote.Writer != nil {
		return true
	}
	s.WriteError(w, http.StatusNotImplemented, "remote children are not available on this node")
	return false
}

// undecider is an audit writer (handlers.AuditWriter, or the topics
// package's own) that can record an answer without the leader's
// decision.
type undecider interface{ MarkUndecided() }

// writeForwardError answers a remote write whose forward failed. A
// leader that could not be reached may have committed the write, so the
// audit line says unknown; an older leader refused it outright.
func writeForwardError(s *handlers.Set, w http.ResponseWriter, err error) {
	if leaderTooOld(err) {
		s.WriteError(w, http.StatusPreconditionFailed, "the cluster leader runs an older release that does not serve remote children; finish the upgrade first")
		return
	}
	if u, ok := w.(undecider); ok {
		u.MarkUndecided()
	}
	s.WriteError(w, http.StatusServiceUnavailable, "the cluster leader could not be reached; retry")
}

// sendRemoteChildWrite forwards one remote child write to the leader and
// answers with its reply; the caller's audit writer records the status.
func sendRemoteChildWrite(s *handlers.Set, w http.ResponseWriter, r *http.Request, subOp, actor, requestID string, body map[string]any, timeout time.Duration) {
	if !remotePlaneWired(s, w) {
		return
	}
	if !allowRemoteChildWrite(s, w) {
		return
	}
	res, err := forwardRemoteWrite(r.Context(), s, subOp, actor, requestID, body, timeout)
	if err != nil {
		writeForwardError(s, w, err)
		return
	}
	writeRemoteAnswer(s, w, res)
}

func forwardRemoteWrite(ctx context.Context, s *handlers.Set, subOp, actor, requestID string, body map[string]any, timeout time.Duration) (nodewire.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nodewire.Response{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.Deps.Remote.Writer.RemoteWrite(ctx, nodewire.RemoteWriteRequest{
		SubOp: subOp, Actor: actor, RequestID: requestID, Body: raw,
	})
}

// writeRemoteAnswer writes a leader answer, turning a 429's
// retry_after_seconds into a Retry-After header.
func writeRemoteAnswer(s *handlers.Set, w http.ResponseWriter, res nodewire.Response) {
	if res.Status == http.StatusTooManyRequests {
		var body struct {
			RetryAfter int `json:"retry_after_seconds"`
		}
		if json.Unmarshal(res.Body, &body) == nil && body.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(body.RetryAfter))
		}
	}
	s.WriteRemoteResponse(w, res)
}

// forceParam reads ?force=: true abandons unshipped records on a delete.
func forceParam(s *handlers.Set, w http.ResponseWriter, r *http.Request) (bool, bool) {
	raw := r.URL.Query().Get("force")
	if raw == "" {
		return false, true
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "force must be true or false")
		return false, false
	}
	return v, true
}

// isRemoteChildOf reports whether this node's replica shows child as a
// remote child of parent.
func isRemoteChildOf(r *http.Request, s *handlers.Set, parent, child string) bool {
	c, err := s.Deps.Broker.GetTopic(r.Context(), child)
	return err == nil && c.IsRemoteChild() && c.Parent == parent
}

// authorizeStubDelete is the delete rule of a remote child: security on,
// and the caller is an admin or the owner of the parent, read from the
// parent's record. The stub never grants anything, and an admin who is
// later demoted keeps no rights over the stubs they created.
func authorizeStubDelete(s *handlers.Set, w http.ResponseWriter, r *http.Request, parent string) bool {
	id, ok := handlers.Identity(r)
	if !ok {
		s.WriteError(w, http.StatusForbidden, "remotes require security")
		return false
	}
	if id.IsAdmin() {
		return true
	}
	if p, err := s.Deps.Broker.GetTopic(r.Context(), parent); err == nil && p.Owner != "" && p.Owner == id.Username {
		return true
	}
	s.WriteError(w, http.StatusForbidden, "only an admin or the parent's owner may delete a remote child")
	return false
}

// deleteTimeout bounds a delete that travels as a remote write: the
// unshipped check asks every member (15 s), and a topic delete fans the
// purge out to the partition owners.
const deleteTimeout = 45 * time.Second

// detachThroughLeader sends the detach of a remote child as
// child.delete, which runs the unshipped check on the leader before the
// topic manager's detach. A local child never comes here: its detach
// takes the plain path, and a leader that finds a stub where this
// node's replica showed a local child refuses it (409) instead.
func detachThroughLeader(s *handlers.Set, w http.ResponseWriter, r *http.Request, parent, child string, force bool) {
	handlers.SetNoStore(w)
	body := map[string]any{"parent": parent, "child": child, "force": force, "expect_remote": true}
	res, err := forwardRemoteWrite(r.Context(), s, nodewire.RemoteSubDetach, callerName(r), remote.NewRequestID(), body, deleteTimeout)
	if err != nil {
		writeForwardError(s, w, err)
		return
	}
	writeRemoteAnswer(s, w, res)
}

// deleteThroughLeader sends the delete of a remote child's stub, or of a
// parent with remote children, as topic.delete, which runs the
// unshipped check on the leader before the topic manager's delete. Any
// other topic takes the plain path, and a leader that finds a remote
// child this node's replica did not show refuses it (409) instead.
func deleteThroughLeader(s *handlers.Set, w http.ResponseWriter, r *http.Request, name string, force bool) {
	handlers.SetNoStore(w)
	body := map[string]any{"topic": name, "force": force, "expect_remote": true}
	res, err := forwardRemoteWrite(r.Context(), s, nodewire.RemoteSubTopicDelete, callerName(r), remote.NewRequestID(), body, deleteTimeout)
	if err != nil {
		writeForwardError(s, w, err)
		return
	}
	writeRemoteAnswer(s, w, res)
}

// callerName is the authenticated caller, or "" with security off.
func callerName(r *http.Request) string {
	if id, ok := handlers.Identity(r); ok {
		return id.Username
	}
	return ""
}

// callerSeesAdminFields reports whether the caller may see the admin
// names on a stub (paused_by, created_by): an admin, or no identity at
// all (security off).
func callerSeesAdminFields(r *http.Request) bool {
	id, ok := handlers.Identity(r)
	return !ok || id.IsAdmin()
}

// redactStub strips the admin names from a stub for a caller who is no
// admin.
func redactStub(t topic.Topic, admin bool) topic.Topic {
	if t.Remote == nil || admin {
		return t
	}
	link := *t.Remote
	link.PausedBy, link.CreatedBy = "", ""
	t.Remote = &link
	return t
}

// remoteChildStatus is a remote child's part of the children listing:
// the link, its worst partition state, the recovery point, and what the
// cursors are stuck on. It names the remote, never its URL or
// credential, and every error is a state, never the target's text.
type remoteChildStatus struct {
	Remote topic.RemoteLink `json:"remote"`
	Paused bool             `json:"paused"`
	// State is the worst state across the parent's partitions.
	State string `json:"state"`
	// LagSeconds is the age of the oldest record not yet on the remote:
	// the live recovery point.
	LagSeconds float64 `json:"lag_seconds"`
	// RetentionHeadroomSeconds is the parent's retention minus the lag;
	// absent when retention is 0 (keep forever).
	RetentionHeadroomSeconds *float64 `json:"retention_headroom_seconds,omitempty"`
	// SourceDrained is true when the parent's consumer ack frontier has
	// reached the link's start on every partition.
	SourceDrained bool               `json:"source_drained"`
	BlockedAt     *topic.RemoteBlock `json:"blocked_at"`
	// TargetVerifiedAt is the oldest of the cursors' last successful
	// target checks; Unverified flags a running link with none in the
	// last 10 minutes.
	TargetVerifiedAt *string `json:"target_verified_at"`
	Unverified       bool    `json:"unverified,omitempty"`
	LastSuccessAt    *string `json:"last_success_at,omitempty"`
	// Partitions has one row per parent partition with ?partitions=true.
	Partitions []remotePartitionRow `json:"partitions,omitempty"`
}

type remotePartitionRow struct {
	Partition     int                `json:"partition"`
	Node          string             `json:"node,omitempty"`
	StartOffset   int64              `json:"start_offset"`
	NextOffset    *int64             `json:"next_offset"`
	HighWatermark *int64             `json:"high_watermark"`
	AckFrontier   *int64             `json:"ack_frontier"`
	State         string             `json:"state"`
	LastSuccessAt *string            `json:"last_success_at"`
	BlockedAt     *topic.RemoteBlock `json:"blocked_at,omitempty"`
}

// unverifiedAfter mirrors sink.UnverifiedAfter.
const unverifiedAfter = 10 * time.Minute

func rfc3339Ms(ms int64) *string {
	if ms <= 0 {
		return nil
	}
	s := time.UnixMilli(ms).UTC().Format(time.RFC3339)
	return &s
}

// remoteStart is where the link started on a parent partition: the
// recorded start offset, or 0 for a partition added after the attach.
func remoteStart(stub topic.Topic, p int) int64 {
	if p < len(stub.AttachOffsets) {
		return max(stub.AttachOffsets[p], 0)
	}
	return 0
}

// remoteStatus folds the owners' cursor stats of one remote child into
// its listing entry.
func remoteStatus(parent, stub topic.Topic, stats []topic.FanoutCursorStat, complete, admin, withPartitions bool, now time.Time) *remoteChildStatus {
	st := &remoteChildStatus{Remote: *redactStub(stub, admin).Remote, Paused: stub.Remote.Paused, SourceDrained: true}
	byPart := make(map[int]topic.FanoutCursorStat, len(stats))
	for _, s := range stats {
		byPart[s.Partition] = s
	}
	state := ""
	if !complete {
		state = topic.RemoteStateUnknown
	}
	var verified, lastSuccess int64
	for p := range parent.Partitions {
		s, ok := byPart[p]
		row := remotePartitionRow{Partition: p, StartOffset: remoteStart(stub, p), State: topic.RemoteStateUnknown}
		if ok {
			row.Node = s.Node
			next, hwm := s.NextOffset, s.HighWatermark
			row.NextOffset, row.HighWatermark, row.AckFrontier = &next, &hwm, s.AckFrontier
			row.LastSuccessAt, row.BlockedAt = rfc3339Ms(s.LastSuccessMs), s.BlockedAt
			if s.State != "" {
				row.State = s.State
			}
			st.LagSeconds = max(st.LagSeconds, s.LagSeconds)
			if s.BlockedAt != nil && (st.BlockedAt == nil || s.BlockedAt.Partition < st.BlockedAt.Partition) {
				b := *s.BlockedAt
				st.BlockedAt = &b
			}
			if s.TargetVerifiedAtMs > 0 && (verified == 0 || s.TargetVerifiedAtMs < verified) {
				verified = s.TargetVerifiedAtMs
			}
			lastSuccess = max(lastSuccess, s.LastSuccessMs)
		}
		if row.AckFrontier == nil || *row.AckFrontier < row.StartOffset {
			st.SourceDrained = false
		}
		state = topic.WorseRemoteState(state, row.State)
		if withPartitions {
			st.Partitions = append(st.Partitions, row)
		}
	}
	if state == "" || (state == topic.RemoteStateRunning && stub.Remote.Paused) {
		state = topic.RemoteStateRunning
		if stub.Remote.Paused {
			state = topic.RemoteStatePaused
		}
	}
	st.State = state
	if parent.RetentionMs > 0 {
		h := float64(parent.RetentionMs)/1000 - st.LagSeconds
		st.RetentionHeadroomSeconds = &h
	}
	st.TargetVerifiedAt = rfc3339Ms(verified)
	st.LastSuccessAt = rfc3339Ms(lastSuccess)
	st.Unverified = state == topic.RemoteStateRunning && (verified == 0 || now.Sub(time.UnixMilli(verified)) > unverifiedAfter)
	return st
}
