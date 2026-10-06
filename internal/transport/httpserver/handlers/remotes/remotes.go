// Package remotes holds the HTTP handlers of the /v1/remotes surface
// and the cluster re-encrypt call. Every route is admin-only with
// security on (RequireSecuredAdmin, no dev-mode bypass), answers
// Cache-Control: no-store, and writes exactly one audit line per
// request on the node the client called (handlers.AuditWriter, as every
// admin mutation does), refused ones included, carrying the request ID
// that joins it to the leader's line. No line carries a password. A remote write
// is validated and its password sealed here, on the node that received
// it; only the ciphertext is forwarded to the leader, which proposes it
// through Raft and writes the authoritative audit line.
package remotes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// MaxBodyBytes caps a remotes request body.
const MaxBodyBytes int64 = 128 << 10

// Audit events of the remotes API.
const (
	eventCreate    = "remote.create"
	eventUpdate    = "remote.update"
	eventDelete    = "remote.delete"
	eventTest      = "remote.test"
	eventReencrypt = "remote.reencrypt"
	eventList      = "remote.list"
	eventGet       = "remote.get"
)

// call is one request to the remotes API: its request ID, and the one
// audit line it writes when it ends.
type call struct {
	s       *handlers.Set
	w       http.ResponseWriter
	aw      *handlers.AuditWriter
	r       *http.Request
	id      string
	event   string
	target  string
	attrs   []slog.Attr
	written bool
}

func begin(s *handlers.Set, w http.ResponseWriter, r *http.Request, event string) *call {
	handlers.SetNoStore(w)
	aw := handlers.NewAuditWriter(w)
	return &call{s: s, w: aw, aw: aw, r: r, id: remote.NewRequestID(), event: event, target: r.PathValue("name")}
}

// fail answers status with msg and audits it.
func (c *call) fail(status int, msg string) {
	c.s.WriteError(c.w, status, msg)
	c.audit(status)
}

// audit writes the request's audit line, once. The outcome is the
// AuditWriter's reading of the status the client got.
func (c *call) audit(int) {
	if c.written {
		return
	}
	c.written = true
	extra := make([]any, 0, 2+len(c.attrs))
	extra = append(extra, "request_id", c.id)
	for _, a := range c.attrs {
		extra = append(extra, a)
	}
	c.aw.Audit(c.s, c.r, c.event, c.target, extra...)
}

// admin runs RequireSecuredAdmin and the wiring check; false means the
// call was answered.
func (c *call) admin() (string, bool) {
	caller, ok := c.s.RequireSecuredAdmin(c.w, c.r)
	if !ok {
		c.audit(http.StatusForbidden)
		return "", false
	}
	if c.s.Deps.Remote.Service == nil || c.s.Deps.Remote.Writer == nil || c.s.Deps.Metastore == nil {
		c.fail(http.StatusNotImplemented, "remotes are not available on this node")
		return "", false
	}
	return caller.Username, true
}

// releaseGate refuses (412) a remote write while some member, as this
// node's replica shows the member records, does not apply the remote
// Raft entry types: nothing is sealed or forwarded then. The leader
// runs the same gate again before it proposes.
func (c *call) releaseGate() bool {
	err := c.s.Deps.Metastore.RemotesUsable()
	if err == nil {
		return true
	}
	reason := strings.TrimPrefix(err.Error(), metastore.ErrEntryTypeNotYetUsable.Error()+": ")
	members := []string{}
	if held, ok := errors.AsType[*metastore.RemotesHeldBackError](err); ok {
		reason = held.Reason
		if held.Member != "" {
			members = append(members, held.Member)
		}
	}
	// The body has the leader gate's shape: the member in `members`.
	c.s.WriteJSON(c.w, http.StatusPreconditionFailed, map[string]any{
		"error":   "not every cluster member runs a release that applies the remote Raft entry types; upgrade or remove the member named here: " + reason,
		"members": members,
	})
	c.audit(http.StatusPreconditionFailed)
	return false
}

// write forwards one remote write to the leader and relays its answer.
func (c *call) write(actor, subOp string, op any) {
	body, err := json.Marshal(op)
	if err != nil {
		c.fail(http.StatusInternalServerError, "encode remote write")
		return
	}
	res, err := c.s.Deps.Remote.Writer.RemoteWrite(c.r.Context(), nodewire.RemoteWriteRequest{
		SubOp: subOp, Actor: actor, RequestID: c.id, Body: body,
	})
	switch {
	case errors.Is(err, errs.ErrRemoteFeatureGate):
		c.fail(http.StatusPreconditionFailed, "the metastore leader runs a release without remotes; finish the upgrade first")
		return
	case err != nil:
		// The write may still have committed on the leader: the outcome
		// is unknown, and the request ID joins this line to the leader's.
		c.aw.MarkUndecided()
		c.fail(http.StatusServiceUnavailable, "the metastore leader could not be reached; retry")
		return
	}
	c.s.WriteRemoteResponse(c.w, res)
	c.audit(res.Status)
}

// decode reads and strictly decodes a body of at most MaxBodyBytes. Its
// errors quote nothing from the input: the JSON decoder's own text can
// quote a character of a password.
func (c *call) decode(dst any, allowEmpty bool) bool {
	body, ok := c.s.ReadBody(c.w, c.r, MaxBodyBytes)
	if !ok {
		c.audit(http.StatusBadRequest)
		return false
	}
	defer clear(body)
	if allowEmpty && len(bytes.TrimSpace(body)) == 0 {
		return true
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil {
		if _, terr := dec.Token(); terr != io.EOF {
			err = errors.New("trailing data")
		}
	}
	switch {
	case errors.Is(err, domremote.ErrSecretInvalid):
		c.fail(http.StatusBadRequest, domremote.ErrSecretInvalid.Error())
		return false
	case err != nil:
		c.fail(http.StatusBadRequest, "invalid json: a field is unknown, malformed or of the wrong type")
		return false
	}
	return true
}

// prepareError answers a failed validation or seal.
func (c *call) prepareError(err error) {
	var inv *remote.InvalidError
	switch {
	case errors.As(err, &inv):
		c.fail(http.StatusBadRequest, inv.Error())
	case errors.Is(err, errs.ErrRemoteNotFound):
		c.fail(http.StatusNotFound, "remote not found")
	case errors.Is(err, errs.ErrRemoteHopUnencrypted):
		c.fail(http.StatusPreconditionFailed, "remote writes carry a password, so this node needs an encrypted API hop: set remotes.api_hop_encrypted once the ingress-to-pod hop is mesh mTLS or re-encrypted TLS")
	case errors.Is(err, errs.ErrRemoteSecretWeak), errors.Is(err, errs.ErrRemoteSecretMissing):
		c.fail(http.StatusPreconditionFailed, err.Error())
	case errors.Is(err, errs.ErrRemoteThrottled):
		c.w.Header().Set("Retry-After", "6")
		c.fail(http.StatusTooManyRequests, "at most 10 remote writes a minute on this node")
	default:
		c.s.Deps.Logger.Error("remote write preparation failed", "event", c.event, "err", err)
		c.fail(http.StatusInternalServerError, "remote write failed")
	}
}

// Create handles POST /v1/remotes.
func Create(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventCreate)
		actor, ok := c.admin()
		if !ok {
			return
		}
		if !c.releaseGate() {
			return
		}
		svc := s.Deps.Remote.Service
		if err := svc.AllowWrite(); err != nil {
			c.prepareError(err)
			return
		}
		var req remote.CreateRequest
		if !c.decode(&req, false) {
			req.Password.Wipe()
			return
		}
		c.target = req.Name
		sealed, err := svc.PrepareCreate(r.Context(), req)
		if err != nil {
			c.prepareError(err)
			return
		}
		c.attrs = []slog.Attr{slog.String("host", remote.URLHost(sealed.Record.URL)), slog.String("fingerprint", sealed.Record.Fingerprint)}
		c.write(actor, nodewire.RemoteSubCreate, metastore.PutRemoteOp{
			Record: sealed.Record, Salt: sealed.Salt, SealedAtMs: sealed.SealedAtMs, Actor: actor, RequestID: c.id,
		})
	}
}

// Update handles PATCH /v1/remotes/{name}: only the fields named
// change, and a change of url, username or ca_pem needs the password.
func Update(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventUpdate)
		actor, ok := c.admin()
		if !ok {
			return
		}
		if !c.validName() {
			return
		}
		if !c.releaseGate() {
			return
		}
		svc := s.Deps.Remote.Service
		if err := svc.AllowWrite(); err != nil {
			c.prepareError(err)
			return
		}
		var req remote.UpdateRequest
		if !c.decode(&req, false) {
			if req.Password != nil {
				req.Password.Wipe()
			}
			return
		}
		up, err := svc.PrepareUpdate(r.Context(), c.target, req)
		if err != nil {
			c.prepareError(err)
			return
		}
		c.attrs = []slog.Attr{slog.Bool("password_changed", up.PasswordChanged)}
		if up.PasswordChanged {
			c.attrs = append(c.attrs, slog.String("fingerprint", up.Fingerprint))
		}
		c.write(actor, nodewire.RemoteSubUpdate, metastore.UpdateRemoteOp{
			Name:          up.Name,
			Fields:        metastore.RemoteFields{URL: up.URL, Username: up.Username, CAPEM: up.CAPEM, Limits: up.Limits},
			Credential:    up.Credential,
			Fingerprint:   up.Fingerprint,
			SealedAgainst: up.SealedAgainst,
			ReadRevision:  up.ReadRevision,
			SealedAtMs:    up.SealedAtMs,
			Actor:         actor,
			RequestID:     c.id,
		})
	}
}

// Delete handles DELETE /v1/remotes/{name}[?force=true].
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventDelete)
		actor, ok := c.admin()
		if !ok {
			return
		}
		if !c.validName() {
			return
		}
		force := false
		switch r.URL.Query().Get("force") {
		case "", "false":
		case "true":
			force = true
		default:
			c.fail(http.StatusBadRequest, "force must be true or false")
			return
		}
		if !c.releaseGate() {
			return
		}
		if err := s.Deps.Remote.Service.AllowWrite(); err != nil {
			c.prepareError(err)
			return
		}
		c.attrs = []slog.Attr{slog.Bool("forced", force)}
		c.write(actor, nodewire.RemoteSubDelete, metastore.DeleteRemoteOp{Name: c.target, Force: force, Actor: actor, RequestID: c.id})
	}
}

// Reencrypt handles POST /v1/cluster/reencrypt-remotes: after a cluster
// secret rotation, the leader re-seals every remote still under the
// previous key. Safe to repeat.
func Reencrypt(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventReencrypt)
		c.target = "*"
		actor, ok := c.admin()
		if !ok {
			return
		}
		var empty struct{}
		if !c.decode(&empty, true) {
			return
		}
		if !c.releaseGate() {
			return
		}
		if err := s.Deps.Remote.Service.AllowWrite(); err != nil {
			c.prepareError(err)
			return
		}
		c.write(actor, nodewire.RemoteSubReencrypt, struct{}{})
	}
}

func (c *call) validName() bool {
	if err := domremote.ValidateName(c.target); err != nil {
		c.fail(http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

// testRequest is the POST /v1/remotes/{name}/test body.
type testRequest struct {
	Topic  string `json:"topic"`
	Source string `json:"source,omitempty"`
}

// testAnswer is the test's answer: the verdict and every member's
// report.
type testAnswer struct {
	Remote string              `json:"remote"`
	Result string              `json:"result"`
	Class  string              `json:"class,omitempty"`
	Checks []remote.NodeReport `json:"checks"`
}

// Test handles POST /v1/remotes/{name}/test: the ch. 4.7 checks from
// every member, writing nothing on either cluster. Without a host
// allowlist it runs on this node only and reports no rtt (the API says
// as little as possible about hosts nobody vetted).
func Test(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventTest)
		if _, ok := c.admin(); !ok {
			return
		}
		if !c.validName() {
			return
		}
		var req testRequest
		if !c.decode(&req, false) {
			return
		}
		if err := topic.ValidateName(req.Topic); err != nil {
			c.fail(http.StatusBadRequest, "topic: "+err.Error())
			return
		}
		if req.Source != "" {
			if err := topic.ValidateName(req.Source); err != nil {
				c.fail(http.StatusBadRequest, "source: "+err.Error())
				return
			}
		}
		rec, err := s.Deps.Metastore.GetRemote(c.target)
		if err != nil {
			c.fail(http.StatusNotFound, "remote not found")
			return
		}
		creq := remote.CheckRequest{Remote: rec.Name, Topic: req.Topic, Source: req.Source, CredentialVersion: rec.CredentialVersion}
		if req.Source != "" {
			d, err := s.Deps.Broker.GetTopicDetails(r.Context(), req.Source)
			if err != nil {
				c.fail(http.StatusNotFound, "source topic not found")
				return
			}
			creq.SourceID, creq.SourceSchema = d.ID, d.Schema
		}
		svc := s.Deps.Remote.Service
		var reports []remote.NodeReport
		if cl := svc.Cluster(); cl != nil && svc.AllowlistConfigured() {
			reports, err = cl.CheckEverywhere(r.Context(), creq)
			if err != nil {
				c.fail(http.StatusServiceUnavailable, "cluster members could not be listed; retry")
				return
			}
		} else {
			rep, throttled := svc.RunCheck(r.Context(), creq)
			if throttled {
				c.w.Header().Set("Retry-After", "5")
				c.fail(http.StatusTooManyRequests, "a check of this remote ran less than 5 s ago on this node")
				return
			}
			reports = []remote.NodeReport{rep}
		}
		ans := testAnswer{Remote: rec.Name, Result: remote.ResultPass, Checks: reports}
		status := http.StatusOK
		if _, err := remote.Verdict(reports, rec.CredentialVersion); err != nil {
			var ce *remote.CheckError
			if errors.As(err, &ce) {
				ans.Result, ans.Class = remote.ResultFail, ce.Class
				if ce.Status == http.StatusTooManyRequests {
					status = http.StatusTooManyRequests
				}
			}
		}
		c.attrs = []slog.Attr{slog.String("result", ans.Result)}
		if ans.Class != "" {
			c.attrs = append(c.attrs, slog.String("class", ans.Class))
		}
		s.WriteJSON(c.w, status, ans)
		c.audit(status)
	}
}

// keyView is the key block of GET /v1/remotes: the current key version
// and its age from its first seal. The key version appears only in this
// admin-only API, never in metrics, logs or audit lines.
type keyView struct {
	KeyVersion    string `json:"key_version"`
	FirstSealedAt string `json:"first_sealed_at,omitempty"`
	AgeSeconds    int64  `json:"age_seconds,omitempty"`
}

// lingeringView is a remote some node still holds although the live
// registry no longer has it (the node has not applied the delete), and
// the nodes that could not be asked.
type lingeringView struct {
	Remote       string   `json:"remote"`
	Holding      []string `json:"holding"`
	NotAnswering []string `json:"not_answering"`
}

type listAnswer struct {
	Allowlist string              `json:"allowlist"`
	Key       *keyView            `json:"key,omitempty"`
	Remotes   []remote.RemoteView `json:"remotes"`
	Lingering []lingeringView     `json:"lingering"`
	// NotAnswering names every member that was asked and did not
	// answer, whether or not any answering member still holds a
	// deleted remote: a silent member may still hold one.
	NotAnswering []string `json:"not_answering"`
}

// List handles GET /v1/remotes[?nodes=false].
func List(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventList)
		c.target = "*"
		if _, ok := c.admin(); !ok {
			return
		}
		records, err := s.Deps.Metastore.ListRemotes()
		if err != nil {
			c.fail(http.StatusServiceUnavailable, "remotes could not be read; retry")
			return
		}
		ans := listAnswer{Allowlist: "none", Remotes: []remote.RemoteView{}, Lingering: []lingeringView{}, NotAnswering: []string{}}
		if s.Deps.Remote.Service.AllowlistConfigured() {
			ans.Allowlist = "set"
		}
		ans.Key = keyBlock(s)
		statuses := memberStatuses(r.Context(), s, r.URL.Query().Get("nodes") != "false")
		for _, rec := range records {
			ans.Remotes = append(ans.Remotes, viewWithNodes(s, rec, statuses))
		}
		ans.Lingering, ans.NotAnswering = lingering(records, statuses)
		s.WriteJSON(c.w, http.StatusOK, ans)
		c.audit(http.StatusOK)
	}
}

// Get handles GET /v1/remotes/{name}[?nodes=false].
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := begin(s, w, r, eventGet)
		if _, ok := c.admin(); !ok {
			return
		}
		if !c.validName() {
			return
		}
		rec, err := s.Deps.Metastore.GetRemote(c.target)
		if err != nil {
			c.fail(http.StatusNotFound, "remote not found")
			return
		}
		statuses := memberStatuses(r.Context(), s, r.URL.Query().Get("nodes") != "false")
		s.WriteJSON(c.w, http.StatusOK, viewWithNodes(s, rec, statuses))
		c.audit(http.StatusOK)
	}
}

func keyBlock(s *handlers.Set) *keyView {
	kv := s.Deps.Remote.Service.CurrentKeyVersion()
	if kv == "" {
		return nil
	}
	k := &keyView{KeyVersion: kv}
	keys, err := s.Deps.Metastore.RemoteKeys()
	if err == nil {
		if v, ok := keys.Versions[kv]; ok && v.FirstSealedAtMs > 0 {
			first := time.UnixMilli(v.FirstSealedAtMs)
			k.FirstSealedAt = first.UTC().Format(time.RFC3339)
			k.AgeSeconds = int64(time.Since(first).Seconds())
		}
	}
	return k
}

// memberStatuses asks every member for its cache's status, unless the
// caller asked for no nodes.
func memberStatuses(ctx context.Context, s *handlers.Set, want bool) []remote.MemberStatus {
	if !want {
		return nil
	}
	if cl := s.Deps.Remote.Service.Cluster(); cl != nil {
		return cl.StatusEverywhere(ctx)
	}
	rep := s.Deps.Remote.Service.Status()
	return []remote.MemberStatus{{Node: rep.Node, Report: &rep}}
}

func viewWithNodes(s *handlers.Set, rec domremote.Record, statuses []remote.MemberStatus) remote.RemoteView {
	v := remote.ViewOf(rec)
	v.Links, _ = s.Deps.Metastore.RemoteChildrenOf(rec.Name)
	for _, ms := range statuses {
		if ms.Report == nil {
			v.Nodes = append(v.Nodes, remote.NodeView{Node: ms.Node, State: topic.RemoteStateUnknown, LastError: ms.Class})
			continue
		}
		var st *remote.NodeRemoteStatus
		for i := range ms.Report.Remotes {
			if ms.Report.Remotes[i].Remote == rec.Name {
				st = &ms.Report.Remotes[i]
				break
			}
		}
		v.Nodes = append(v.Nodes, remote.NodeViewOf(ms.Node, st, rec))
	}
	return v
}

// lingering lists the remotes some node's cache still holds that the
// live registry does not, and the nodes that did not answer. The silent
// nodes are returned on their own too, so they are reported even when
// no answering node holds anything.
func lingering(records []domremote.Record, statuses []remote.MemberStatus) ([]lingeringView, []string) {
	live := map[string]bool{}
	for _, r := range records {
		live[r.Name] = true
	}
	silent := []string{}
	holding := map[string][]string{}
	for _, ms := range statuses {
		if ms.Report == nil {
			silent = append(silent, ms.Node)
			continue
		}
		for _, st := range ms.Report.Remotes {
			if !live[st.Remote] {
				holding[st.Remote] = append(holding[st.Remote], ms.Node)
			}
		}
	}
	sort.Strings(silent)
	out := []lingeringView{}
	for name, nodes := range holding {
		sort.Strings(nodes)
		out = append(out, lingeringView{Remote: name, Holding: nodes, NotAnswering: append([]string{}, silent...)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Remote < out[j].Remote })
	return out, silent
}
