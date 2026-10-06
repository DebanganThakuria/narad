package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// Check timing.
const (
	// CheckTimeout bounds one member's whole run of the checks.
	CheckTimeout = 15 * time.Second
	// certExpiryWarning is how close to expiry a target certificate
	// gets a warning.
	certExpiryWarning = 14 * 24 * time.Hour
	// serviceTimeMs is the assumed target service time in the lane
	// capacity estimate, 100 / (rtt + s).
	serviceTimeMs = 5
)

// Checker runs the ch. 4.7 checks from this member with its cached
// entry, so they exercise exactly what the data path will use.
type Checker struct {
	Lookup  Lookup
	Guard   *Guard
	Posture Posture
	NodeID  string
	// Observe records the outcome on the cache (nil: not recorded).
	Observe func(name, class string, certNotAfter time.Time, rttMs *int64)
	now     func() time.Time
}

// describeAnswer is the part of a target's topic describe the checks
// read. Unknown fields are ignored, so a newer target does not break an
// older source.
type describeAnswer struct {
	ID            string          `json:"id"`
	CreatedAt     int64           `json:"created_at"`
	FanoutDelayMs int64           `json:"fanout_delay_ms"`
	Remote        json.RawMessage `json:"remote"`
	Schema        json.RawMessage `json:"schema"`
}

// childrenAnswer is the part of a target's children listing the checks
// read: a top-level parent_id, and a remote object on each remote child
// (a target on this release shows both; an older one neither).
type childrenAnswer struct {
	ParentID *string `json:"parent_id"`
	Children []struct {
		Remote json.RawMessage `json:"remote"`
	} `json:"children"`
}

// Run runs the checks and returns this member's report. It never
// returns the target's error text: only a class, and warnings the
// checks themselves word.
func (c *Checker) Run(ctx context.Context, req CheckRequest) NodeReport {
	rep := NodeReport{Node: c.NodeID, Result: ResultFail, Warnings: []string{}, Posture: c.Posture}
	fail := func(class string) NodeReport {
		rep.Class = class
		if c.Observe != nil {
			c.Observe(req.Remote, class, time.Time{}, nil)
		}
		return rep
	}
	// Check 1: the remote, its credential at the record's version, and
	// a posture that allows remotes.
	if !PostureAllowsRemotes(c.Posture) {
		return fail(topic.RemoteStateNodeInsecure)
	}
	e, err := c.Lookup.Get(req.Remote)
	switch {
	case errors.Is(err, ErrRemoteMissing):
		return fail(topic.RemoteStateRemoteMissing)
	case errors.Is(err, ErrCredentialUnreadable):
		return fail(topic.RemoteStateCredentialUnreadable)
	case errors.Is(err, ErrNodeInsecure):
		return fail(topic.RemoteStateNodeInsecure)
	case err != nil:
		return fail(ClassStale)
	}
	rep.CredentialVersion = e.CredentialVersion()
	if e.CredentialVersion() != req.CredentialVersion {
		return fail(ClassStale)
	}
	describe, err := TopicPath(req.Topic)
	if err != nil {
		return fail(topic.RemoteStateTargetMissing)
	}
	children, _ := TopicPath(req.Topic, "children")
	batch, _ := TopicPath(req.Topic, "produce", "batch")
	ctx, cancel := context.WithTimeout(ctx, CheckTimeout)
	defer cancel()

	// Check 2: the dial passes the guard, and TCP and TLS succeed with
	// the chain and hostname verified. The TCP connect time is the rtt,
	// reported only when the node has a host allowlist (blind by design
	// otherwise).
	rtt, class := c.measureRTT(ctx, e)
	if class != "" {
		return fail(class)
	}
	if c.Guard != nil && c.Guard.AllowlistConfigured() {
		ms := rtt.Milliseconds()
		capacity := int64(100_000 / (ms + serviceTimeMs))
		rep.RTTMs, rep.LaneCapacityPerS = &ms, &capacity
	}

	// Check 3: the target without credentials answers 401.
	resp, err := e.do(ctx, Outbound{Method: http.MethodGet, Path: describe}, false)
	if err != nil {
		return fail(transportClass(err))
	}
	_, _ = ReadBody(resp, MaxReadAnswerBytes)
	if resp.StatusCode != http.StatusUnauthorized {
		if cl := redirectOr(resp, ""); cl != "" {
			return fail(cl)
		}
		return fail(ClassTargetSecurityOff)
	}

	// Check 4: the topic with credentials: 200, not a delay child, not a
	// stub, not the source itself, and a schema that matches.
	resp, err = e.Do(ctx, Outbound{Method: http.MethodGet, Path: describe})
	if err != nil {
		return fail(transportClass(err))
	}
	body, _ := ReadBody(resp, MaxReadAnswerBytes)
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		notAfter := resp.TLS.PeerCertificates[0].NotAfter
		rep.CertNotAfter = notAfter.UTC().Format(time.RFC3339)
		if notAfter.Sub(c.clock()) < certExpiryWarning {
			rep.Warnings = append(rep.Warnings, "the target's certificate expires within 14 days")
		}
		if c.Observe != nil {
			c.Observe(req.Remote, "", notAfter, rep.RTTMs)
		}
	}
	if cl := readClass(resp, body); cl != "" {
		return fail(cl)
	}
	var d describeAnswer
	if err := json.Unmarshal(body, &d); err != nil {
		return fail(topic.RemoteClassEdge)
	}
	switch {
	case d.FanoutDelayMs > 0:
		return fail(ClassTargetIsDelayChild)
	case len(d.Remote) > 0 && !bytes.Equal(d.Remote, []byte("null")):
		return fail(ClassTargetIsStub)
	case targetIsSource(d, req):
		return fail(ClassTargetIsSource)
	}
	rep.TargetID = d.ID
	if schemaClass, warn := compareSchemas(req.SourceSchema, d.Schema); schemaClass != "" {
		return fail(schemaClass)
	} else if warn != "" {
		rep.Warnings = append(rep.Warnings, warn)
	}

	// Check 5: no child of the target is itself a remote child (the
	// loop and chain rule, Q7).
	resp, err = e.Do(ctx, Outbound{Method: http.MethodGet, Path: children})
	if err != nil {
		return fail(transportClass(err))
	}
	body, _ = ReadBody(resp, MaxReadAnswerBytes)
	if cl := readClass(resp, body); cl != "" {
		return fail(cl)
	}
	var ch childrenAnswer
	if err := json.Unmarshal(body, &ch); err != nil {
		return fail(topic.RemoteClassEdge)
	}
	rep.TargetServesIDs = ch.ParentID != nil
	if !topic.RemoteChainsAllowed {
		for _, child := range ch.Children {
			if len(child.Remote) > 0 && !bytes.Equal(child.Remote, []byte("null")) {
				return fail(topic.RemoteStateTargetHasRemoteChildren)
			}
		}
	}
	if !rep.TargetServesIDs {
		rep.Warnings = append(rep.Warnings, "the target does not serve remote and parent_id (an older release, which cannot hold remote children): loop and chain detection start once it is upgraded; recreate detection reads the topic id")
	}
	if rep.TargetID == "" {
		rep.Warnings = append(rep.Warnings, "the target topic has no id (created before v2.2.0): the link records none, and stops in target_replaced if the target later reports one (the topic was recreated, or the remote points at another cluster)")
	}

	// Check 6: an empty batch proves the credential, the produce grant
	// and the batch route without writing anything. There is no
	// fallback to single produce.
	resp, err = e.Do(ctx, Outbound{Method: http.MethodPost, Path: batch, Body: []byte(`{"messages":[]}`), ContentType: "application/json"})
	if err != nil {
		return fail(transportClass(err))
	}
	body, _ = ReadBody(resp, MaxProduceAnswerBytes)
	if cl := batchClass(resp, body); cl != "" {
		return fail(cl)
	}

	// Check 7: the credential is not an admin on the target (least
	// privilege). A target built without a metastore has no users
	// route and cannot tell.
	resp, err = e.Do(ctx, Outbound{Method: http.MethodGet, Path: UsersPath()})
	if err != nil {
		return fail(transportClass(err))
	}
	body, _ = ReadBody(resp, MaxReadAnswerBytes)
	switch {
	case resp.StatusCode == http.StatusForbidden:
	case resp.StatusCode == http.StatusOK:
		return fail(ClassAdminCredential)
	case resp.StatusCode == http.StatusNotFound:
		rep.Warnings = append(rep.Warnings, "the target has no users route: cannot tell whether the credential is an admin")
	default:
		if cl := readClass(resp, body); cl != "" {
			return fail(cl)
		}
		return fail(topic.RemoteStateUnknown)
	}

	rep.Result, rep.Class = ResultPass, ""
	if c.Observe != nil {
		c.Observe(req.Remote, "none", time.Time{}, rep.RTTMs)
	}
	return rep
}

func (c *Checker) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// measureRTT times one TCP connect through the entry's own (guarded)
// dialer and closes it. A refused dial or a failed connect is the
// check's class.
func (c *Checker) measureRTT(ctx context.Context, e *Entry) (time.Duration, string) {
	start := time.Now()
	conn, err := e.pools.Load().transport.DialContext(ctx, "tcp", e.host)
	if err != nil {
		return 0, transportClass(err)
	}
	rtt := time.Since(start)
	_ = conn.Close()
	return rtt, ""
}

// transportClass classifies a failed request or dial.
func transportClass(err error) string {
	if _, ok := DestinationRefused(err); ok {
		return topic.RemoteStateDestinationRefused
	}
	if IsTLSError(err) {
		return topic.RemoteStateTLSFailed
	}
	return topic.RemoteStateUnavailable
}

// redirectOr returns redirect_refused for a 3xx, else class.
func redirectOr(resp *http.Response, class string) string {
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return topic.RemoteStateRedirectRefused
	}
	return class
}

// ReadClass is readClass for the runtime target check.
func ReadClass(resp *http.Response, body []byte) string { return readClass(resp, body) }

// readClass classifies a read answer: "" for 200, else a class. 401,
// 403 and 404 count only in Narad's JSON shape; any other shape is an
// edge (a load balancer, WAF or proxy answered, not the target).
func readClass(resp *http.Response, body []byte) string {
	if resp.StatusCode == http.StatusOK {
		return ""
	}
	if cl := redirectOr(resp, ""); cl != "" {
		return cl
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return topic.RemoteStateThrottled
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
		return topic.RemoteStateUnavailable
	}
	if _, ok := NaradError(resp, body); !ok {
		return topic.RemoteClassEdge
	}
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return topic.RemoteStateAuthFailed
	case http.StatusForbidden:
		return topic.RemoteStateForbidden
	case http.StatusNotFound:
		return topic.RemoteStateTargetMissing
	}
	return topic.RemoteStateUnknown
}

// batchClass classifies check 6's answer: 400 "messages required" in
// Narad's shape passes. Go's exact not-found means the target predates
// batch produce (checks 4 and 5 just answered in Narad's shape from
// the same target); any other non-JSON 404 is an edge.
func batchClass(resp *http.Response, body []byte) string {
	msg, narad := NaradError(resp, body)
	switch {
	case resp.StatusCode == http.StatusBadRequest && narad && msg == "messages required":
		return ""
	case resp.StatusCode == http.StatusNotFound && IsGoNotFound(resp, body):
		return topic.RemoteStateNoBatchProduce
	}
	if cl := readClass(resp, body); cl != "" {
		return cl
	}
	return topic.RemoteStateUnknown
}

// compareSchemas applies check 4's schema rule: refuse when both topics
// have a schema and they differ (compared compacted, so formatting does
// not count); warn when only the target has one.
func compareSchemas(source, target json.RawMessage) (class, warning string) {
	hasSource, hasTarget := hasSchema(source), hasSchema(target)
	switch {
	case hasSource && hasTarget:
		var a, b bytes.Buffer
		if json.Compact(&a, source) != nil || json.Compact(&b, target) != nil || !bytes.Equal(a.Bytes(), b.Bytes()) {
			return ClassSchemaMismatch, ""
		}
	case hasTarget:
		return "", "the target topic has a schema and the source has none: records that do not match it will be rejected"
	}
	return "", ""
}

func hasSchema(s json.RawMessage) bool {
	t := strings.TrimSpace(string(s))
	return t != "" && t != "null"
}

// targetIsSource reports a target topic that is the source itself: the
// remote points back at this cluster. Two topics with IDs are the same
// only with the same ID; a topic with an ID is never one without. Two
// topics created before topic IDs existed (both IDs empty) are judged
// by name and creation time, which no other record of that name on
// this cluster can share.
func targetIsSource(d describeAnswer, req CheckRequest) bool {
	switch {
	case d.ID != "" || req.SourceID != "":
		return d.ID == req.SourceID
	case req.Source == "" || req.SourceCreatedAt == 0:
		return false
	default:
		return req.Topic == req.Source && d.CreatedAt == req.SourceCreatedAt
	}
}
