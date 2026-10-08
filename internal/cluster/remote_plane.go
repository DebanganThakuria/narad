package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
)

// RemotePlane routes every remote mutation to the leader and serves the
// member-side checks. One per node; implements handlers.RemoteWriter.
//
// Registry serves the remotes registry (package A), Links the remote
// children (package B) and Checks the cluster-wide check fan-out and
// upgrade gate. serve.go sets all three before the node serves.
type RemotePlane struct {
	Registry RegistryLeader // package A
	Links    LinkLeader     // package B
	Checks   CheckRunner    // package A

	store  *metastore.Store
	router *Router
	peer   *PeerClient
	selfID string
	log    *slog.Logger
}

// NewRemotePlane builds the plane. router supplies the leader's address
// and the read-your-writes settle; peer forwards to the leader and
// reaches the members.
func NewRemotePlane(store *metastore.Store, router *Router, peer *PeerClient, selfID string, log *slog.Logger) *RemotePlane {
	if log == nil {
		log = slog.Default()
	}
	return &RemotePlane{store: store, router: router, peer: peer, selfID: selfID, log: log}
}

// ErrLeaderTooOld: the leader answered "unsupported rpc operation".
// Callers map it to 412, except topic.delete, which falls back to
// Router.RouteDeleteTopic (ch. 4.5). It wraps errs.ErrRemoteFeatureGate
// so the HTTP layer, which does not import this package, can tell it
// apart from an unreachable leader.
var ErrLeaderTooOld = fmt.Errorf("cluster: the leader does not serve remote writes: %w", errs.ErrRemoteFeatureGate)

// remoteWriteForwardTimeout bounds a forwarded remote write. An attach
// on the leader waits for every member's checks (15 s), the start
// offsets of every parent partition and the Raft apply, so it sits well
// above the peer client's default reply timeout.
const remoteWriteForwardTimeout = 60 * time.Second

// RemoteWrite runs req on the leader: in process when this node leads
// (Router.leaderMemberAddr() == ""), where raft Apply already waited
// for this replica, else forwarded as OpRemoteWrite and settled on the
// local replica. The settle is strict (settleForwardedWriteStrict): a
// 2xx this node cannot confirm it applied comes back as the leader's
// answer with an error wrapping errs.ErrNotAppliedHere, so the client
// is never told a change is done that a read here would not show. A
// dry run writes nothing and keeps the best-effort settle.
func (p *RemotePlane) RemoteWrite(ctx context.Context, req nodewire.RemoteWriteRequest) (nodewire.Response, error) {
	addr := ""
	if p.router != nil {
		addr = p.router.leaderMemberAddr()
	}
	if addr == "" {
		return p.ServeWrite(ctx, req), nil
	}
	fwdCtx, cancel := longWaitRPCContext(ctx, remoteWriteForwardTimeout)
	defer cancel()
	res, err := p.peer.RemoteWrite(fwdCtx, addr, req)
	if err != nil {
		return nodewire.Response{}, err
	}
	if unsupportedOperation(res) {
		return res, ErrLeaderTooOld
	}
	if isDryRun(req) {
		p.router.settleForwardedWrite(ctx, addr, res)
		return res, nil
	}
	if err := p.router.settleForwardedWriteStrict(ctx, addr, res); err != nil {
		p.log.Warn("remote write committed on the leader but not applied here yet",
			"sub_op", req.SubOp, "request_id", req.RequestID, "status", res.Status, "err", err)
		return res, err
	}
	return res, nil
}

// isDryRun reports a remote write that asks only for the checks (an
// attach's dry_run): it commits nothing to wait for.
func isDryRun(req nodewire.RemoteWriteRequest) bool {
	var body struct {
		DryRun bool `json:"dry_run"`
	}
	return req.SubOp == nodewire.RemoteSubAttach && json.Unmarshal(req.Body, &body) == nil && body.DryRun
}

// unsupportedOperation reports an older peer's answer to an op it does
// not know (RPCServer.serveOther's default case).
func unsupportedOperation(res nodewire.Response) bool {
	if res.Status != http.StatusBadRequest {
		return false
	}
	var body struct {
		Error string `json:"error"`
	}
	return json.Unmarshal(res.Body, &body) == nil && strings.HasPrefix(body.Error, "unsupported rpc operation")
}

// Members returns every member that is not removed, dead ones included.
func (p *RemotePlane) Members() ([]metastore.Member, error) {
	return p.store.ListMembers()
}

// SelfID is this node's member ID.
func (p *RemotePlane) SelfID() string { return p.selfID }

// SendCheck sends one OpRemoteCheck to m (self is served in process).
func (p *RemotePlane) SendCheck(ctx context.Context, m metastore.Member, req nodewire.RemoteCheckRequest) (nodewire.Response, error) {
	if m.ID == p.selfID {
		return p.ServeCheck(ctx, req), nil
	}
	if strings.TrimSpace(m.Addr) == "" {
		return nodewire.Response{}, errors.New("cluster: member has no address")
	}
	return p.peer.RemoteCheck(ctx, m.Addr, req)
}

// ServeWrite dispatches a remote write on the leader: remote.* to the
// registry, child.* and topic.delete to the links.
func (p *RemotePlane) ServeWrite(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	switch {
	case strings.HasPrefix(req.SubOp, "remote."):
		if p.Registry == nil {
			return errorResponse(http.StatusNotImplemented, "remotes are not available on this node")
		}
		return p.Registry.ServeWrite(ctx, req)
	case strings.HasPrefix(req.SubOp, "child."), req.SubOp == nodewire.RemoteSubTopicDelete:
		if p.Links == nil {
			return errorResponse(http.StatusNotImplemented, "remote children are not available on this node")
		}
		return p.Links.ServeWrite(ctx, req)
	default:
		return errorResponse(http.StatusBadRequest, "unsupported remote write")
	}
}

// ServeCheck dispatches a member-side check: check and status to the
// registry, unshipped to the links.
func (p *RemotePlane) ServeCheck(ctx context.Context, req nodewire.RemoteCheckRequest) nodewire.Response {
	switch req.Mode {
	case nodewire.RemoteCheckRun, nodewire.RemoteCheckStatus:
		if p.Registry == nil {
			return errorResponse(http.StatusNotImplemented, "remotes are not available on this node")
		}
		return p.Registry.ServeCheck(ctx, req)
	case nodewire.RemoteCheckUnshipped:
		if p.Links == nil {
			return errorResponse(http.StatusNotImplemented, "remote children are not available on this node")
		}
		return p.Links.ServeUnshipped(ctx, req)
	default:
		return errorResponse(http.StatusBadRequest, "unsupported remote check mode")
	}
}

// RegistryLeader serves the remote.* sub-ops on the leader and the
// check and status modes on every member. Package A.
type RegistryLeader interface {
	ServeWrite(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response
	ServeCheck(ctx context.Context, req nodewire.RemoteCheckRequest) nodewire.Response
}

// LinkLeader serves child.* and topic.delete on the leader and the
// unshipped mode on every member. Package B.
type LinkLeader interface {
	ServeWrite(ctx context.Context, req nodewire.RemoteWriteRequest) nodewire.Response
	ServeUnshipped(ctx context.Context, req nodewire.RemoteCheckRequest) nodewire.Response
}

// CheckRunner is the cluster-wide check fan-out and the upgrade gate.
// Package A implements it; B calls it at attach and resume.
type CheckRunner interface {
	// CheckEverywhere runs the ch. 4.7 checks on every member within 15 s.
	CheckEverywhere(ctx context.Context, req remote.CheckRequest) ([]remote.NodeReport, error)
	// CheckHere runs the checks on this node alone: a dry run without a
	// host allowlist, which must not probe from every member.
	CheckHere(ctx context.Context, req remote.CheckRequest) ([]remote.NodeReport, error)
	// AllowlistConfigured reports whether remotes.allowed_hosts is set
	// on this node; without it answers are blind (ch. 5.8).
	AllowlistConfigured() bool
	// RequirePosture runs the release gate, then fails with
	// errs.ErrRemoteFeatureGate (a member did not answer) or
	// errs.ErrRemotePosture (a member has security off or legacy cluster
	// auth on), naming the members.
	RequirePosture(ctx context.Context) error
	// RequireReleases is the release gate alone: it fails with
	// errs.ErrRemoteFeatureGate, naming the member that holds them back,
	// while some member does not apply the remote Raft entry types
	// (metastore.Store.RemotesUsable). It reads the member records, so a
	// pause still works while a member is dead or unreachable.
	RequireReleases(ctx context.Context) error
}
