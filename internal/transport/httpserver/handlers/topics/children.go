package topics

// Fan-out child management:
//
//	POST   /v1/topics/{parent}/children          — attach a child
//	DELETE /v1/topics/{parent}/children/{child}  — detach a child
//	GET    /v1/topics/{parent}/children          — list children + lag
//
// Attach is an admin-or-owner operation on BOTH topics: linking a child
// rewrites the child's schema history, starts pumping the parent's
// records into it, and (with a delay) makes it unproducible, so the
// parent's owner alone must not be able to claim someone else's topic.
// Detach needs manage rights on either side, since both owners have a
// stake in the link. Listing children needs any grant on the parent.
// Like every metadata write, attach and detach are forwarded to the
// cluster leader in multi-node mode.
//
// A body that names a remote attaches a remote child instead (see
// children_remote.go): admin only, checked before anything else.

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"

	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type attachChildRequest struct {
	Child string `json:"child"`
	// DelayMs, when positive, makes the child a delay child: records
	// are delivered only once parentCommitTime+DelayMs has passed.
	DelayMs int64 `json:"delay_ms,omitempty"`
}

// Validate implements handlers.Validator.
func (r attachChildRequest) Validate() error {
	if r.Child == "" {
		return errors.New("child is required")
	}
	if r.DelayMs < 0 {
		return errors.New("delay_ms must be >= 0")
	}
	return nil
}

// AttachChild handles POST /v1/topics/{parent}/children.
func AttachChild(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		if parent == "" {
			s.WriteError(w, http.StatusBadRequest, "parent topic required")
			return
		}
		// One strict decode for both kinds, so a remote attach can never
		// fall back to the owner rules of a local one.
		var remoteReq attachRemoteChildRequest
		if !s.DecodeJSON(w, r, &remoteReq) {
			return
		}
		if remoteReq.Remote != "" {
			attachRemote(s, w, r, parent, remoteReq)
			return
		}
		req, err := remoteReq.local()
		if err == nil {
			err = req.Validate()
		}
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		aw := newAuditWriter(w)
		w = aw
		defer func() { aw.audit(s, r, auditEventAttach, parent, "child", req.Child, "delay_ms", req.DelayMs) }()
		if !s.AuthorizeTopicManage(w, r, parent) || !s.AuthorizeTopicManage(w, r, req.Child) {
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteAttachChild(r.Context(), w, r, parent, req.Child, req.DelayMs) {
				return
			}
		}
		if err := s.Deps.Broker.AttachChild(r.Context(), parent, req.Child, req.DelayMs); err != nil {
			s.WriteBrokerError(w, "attach child", err)
			return
		}
		t, err := s.Deps.Broker.GetTopic(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "attach child", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, t)
	}
}

// DetachChild handles DELETE /v1/topics/{parent}/children/{child}. A
// remote child's stub is deleted, and only while nothing of the parent
// is unshipped, unless ?force=true abandons it (see
// children_remote.go). A detach this node's replica shows as a local
// child takes the plain path; the leader refuses it (409, retry) if the
// child is a stub by then.
func DetachChild(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		child := r.PathValue("child")
		if parent == "" || child == "" {
			s.WriteError(w, http.StatusBadRequest, "parent and child topics required")
			return
		}
		force, ok := forceParam(s, w, r)
		if !ok {
			return
		}
		aw := newAuditWriter(w)
		w = aw
		// A remote child's detach is remote_child.delete, under the
		// request_id the leader's line for it carries.
		var requestID string
		defer func() {
			var extra []any
			if force {
				extra = append(extra, "force", true)
			}
			if requestID != "" {
				aw.audit(s, r, eventRemoteChildDelete, parent+"/"+child, append(extra, "request_id", requestID)...)
				return
			}
			aw.audit(s, r, auditEventDetach, parent, append([]any{"child", child}, extra...)...)
		}()
		if isRemoteChildOf(r, s, parent, child) {
			requestID = remote.NewRequestID()
			if !authorizeStubDelete(s, w, r, parent) {
				return
			}
			if s.Deps.Remote.Writer == nil {
				s.WriteError(w, http.StatusNotImplemented, "remote children are not available on this node")
				return
			}
			detachThroughLeader(s, w, r, parent, child, force, requestID)
			return
		}
		if !s.AuthorizeTopicManageAny(w, r, parent, child) {
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDetachChild(r.Context(), w, r, parent, child) {
				return
			}
		}
		if err := s.Deps.Broker.DetachChild(r.Context(), parent, child); err != nil {
			s.WriteBrokerError(w, "detach child", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// childStatus is one attached child in the list-children response.
// LagMessages sums parent-partition high-watermark minus cursor over
// every reporting cursor; LagComplete is false while some cursors have
// not reported (owner unreachable or cursor not anchored yet), making
// the lag a lower bound. A remote child also carries remoteChildStatus.
type childStatus struct {
	Name string `json:"name"`
	// DelayMs is the child's fan-out delay (0 = immediate).
	DelayMs     int64 `json:"delay_ms"`
	LagMessages int64 `json:"lag_messages"`
	LagComplete bool  `json:"lag_complete"`
	*remoteChildStatus
}

type childrenResponse struct {
	Parent string `json:"parent"`
	// ParentID is the parent's incarnation ID. Another cluster's remote
	// child that sends to this topic reads it to notice a recreate. It is
	// always present ("" for a topic created before topic IDs), so its
	// absence tells an older release.
	ParentID string        `json:"parent_id"`
	Children []childStatus `json:"children"`
}

// ListChildren handles GET /v1/topics/{parent}/children. ?partitions=true
// adds one row per parent partition to each remote child.
func ListChildren(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		if parent == "" {
			s.WriteError(w, http.StatusBadRequest, "parent topic required")
			return
		}
		if !s.AuthorizeTopicRead(w, r, parent) {
			return
		}
		withPartitions := false
		if raw := r.URL.Query().Get("partitions"); raw != "" {
			v, err := strconv.ParseBool(raw)
			if err != nil {
				s.WriteError(w, http.StatusBadRequest, "partitions must be true or false")
				return
			}
			withPartitions = v
		}
		t, err := s.Deps.Broker.GetTopic(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "list children", err)
			return
		}

		stats, err := s.Deps.Broker.FanoutCursorStats(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "list children", err)
			return
		}
		remoteComplete := true
		if s.Deps.Router != nil {
			stats, remoteComplete = s.Deps.Router.CollectFanoutCursors(r.Context(), parent, stats)
		}

		type lagAgg struct {
			lag        int64
			partitions map[int]int
			stats      []topic.FanoutCursorStat
		}
		byChild := map[string]*lagAgg{}
		for _, stat := range stats {
			agg := byChild[stat.Child]
			if agg == nil {
				agg = &lagAgg{partitions: map[int]int{}}
				byChild[stat.Child] = agg
			}
			agg.partitions[stat.Partition]++
			agg.lag += max(0, stat.HighWatermark-stat.NextOffset)
			agg.stats = append(agg.stats, stat)
		}

		admin := callerSeesAdminFields(r)
		resp := childrenResponse{Parent: parent, ParentID: t.ID, Children: []childStatus{}}
		for _, child := range t.Children {
			status := childStatus{Name: child}
			childRecord, childErr := s.Deps.Broker.GetTopic(r.Context(), child)
			if childErr == nil {
				status.DelayMs = childRecord.FanoutDelayMs
			}
			agg := byChild[child]
			if agg != nil {
				status.LagMessages = agg.lag
				// Whole only when every partition reported exactly once.
				status.LagComplete = remoteComplete && len(agg.partitions) == t.Partitions
				for _, n := range agg.partitions {
					status.LagComplete = status.LagComplete && n == 1
				}
			}
			if childErr == nil && childRecord.IsRemoteChild() {
				var childStats []topic.FanoutCursorStat
				if agg != nil {
					childStats = agg.stats
				}
				status.remoteChildStatus = remoteStatus(t, childRecord, childStats, remoteComplete, admin, withPartitions, time.Now())
			}
			resp.Children = append(resp.Children, status)
		}
		s.WriteJSON(w, http.StatusOK, resp)
	}
}
