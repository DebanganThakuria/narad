package sink

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
)

// UnverifiedAfter is how long a link runs without a successful target
// check before the listing flags it unverified.
const UnverifiedAfter = 10 * time.Minute

// TargetView is what a target's children listing says about the topic a
// link sends to. A target on this release always serves parent_id ("" for
// a topic created before topic IDs). An older one omits parent_id and the
// remote objects: ServesIDs is false and no loop is possible through it
// (it cannot hold a remote child). Recreate detection then reads the
// topic's id from its describe answer, which v3.1.0 serves for a topic
// that has one.
type TargetView struct {
	ParentID       string
	ServesIDs      bool
	RemoteChildren int
}

// listingShape is the target's GET /v1/topics/{t}/children answer, as
// far as the check reads it. Unknown fields are ignored, so a newer
// target does not break an older source.
type listingShape struct {
	Parent   *string `json:"parent"`
	ParentID *string `json:"parent_id"`
	Children []struct {
		Remote json.RawMessage `json:"remote"`
	} `json:"children"`
}

func parseListing(body []byte) (TargetView, bool) {
	var l listingShape
	if json.Unmarshal(body, &l) != nil || l.Parent == nil || l.Children == nil {
		return TargetView{}, false
	}
	v := TargetView{}
	if l.ParentID != nil {
		v.ParentID, v.ServesIDs = *l.ParentID, true
	}
	for _, c := range l.Children {
		if len(c.Remote) > 0 && string(c.Remote) != "null" {
			v.RemoteChildren++
		}
	}
	return v, true
}

// isChildrenListing reports a 200 answer in the shape of Narad's
// children listing.
func isChildrenListing(resp *http.Response, body []byte) bool {
	if !isJSON(resp) {
		return false
	}
	_, ok := parseListing(body)
	return ok
}

// TargetResult is one runtime target check's outcome.
type TargetResult struct {
	// State is target_has_remote_children or target_replaced when the
	// check found a reason to stop sending, "" otherwise.
	State string
	// Verified is true when the target answered a listing.
	Verified bool
	View     TargetView
	// Class is the failure class of a check that errored (it never
	// stalls sending).
	Class string
}

// CheckOutcome is what a runtime target check tells the remote's gate.
type CheckOutcome int

const (
	// CheckReached: the target answered in Narad's shape (a listing, or
	// a refusal of the topic or the grant): the remote is reachable.
	CheckReached CheckOutcome = iota
	// CheckFailed: a remote-wide or transient failure (refused
	// credentials, throttling, TLS, an outage, an edge answer), which
	// the gate counts as a chunk's answer of the same class.
	CheckFailed
	// CheckUnsent: nothing went out (the dial was refused on this side).
	CheckUnsent
)

// GateVerdict maps the check's outcome onto the remote's gate, as the
// answer to a chunk of the same class would: the verdict is for
// Gate.Failed when the outcome is CheckFailed.
func (r TargetResult) GateVerdict() (Verdict, CheckOutcome) {
	if r.Verified {
		return Verdict{}, CheckReached
	}
	switch r.Class {
	case topic.RemoteStateAuthFailed, topic.RemoteStateThrottled, topic.RemoteStateTLSFailed:
		return verdict(ActGate, r.Class, 0), CheckFailed
	case topic.RemoteStateUnavailable, topic.RemoteClassEdge:
		v := verdict(ActRetry, topic.RemoteStateUnavailable, 0)
		v.Class = r.Class
		return v, CheckFailed
	case topic.RemoteStateDestinationRefused:
		return Verdict{}, CheckUnsent
	}
	return Verdict{}, CheckReached
}

// FetchListing reads the target's children listing for topicName with
// the entry's credentials.
func FetchListing(ctx context.Context, e *remote.Entry, topicName string) (*http.Response, []byte, error) {
	path, err := remote.TopicPath(topicName, "children")
	if err != nil {
		return nil, nil, err
	}
	resp, err := e.Do(ctx, remote.Outbound{Method: http.MethodGet, Path: path})
	if err != nil {
		return nil, nil, err
	}
	body, err := remote.ReadBody(resp, remote.MaxReadAnswerBytes)
	return resp, body, err
}

// CheckTarget runs the runtime target check of ch. 6.8: a remote child
// anywhere on the target topic stops the link (no loops, no chains), and
// so does a target ID other than the one the link recorded (the topic
// was recreated, or the remote now points at another cluster). A check
// that errors never stops sending.
func CheckTarget(ctx context.Context, e *remote.Entry, topicName, recordedTargetID string, c Classifier) TargetResult {
	resp, body, err := FetchListing(ctx, e, topicName)
	if err != nil {
		if resp == nil {
			return TargetResult{Class: c.classifyTransport(err).Class}
		}
		return TargetResult{Class: topic.RemoteStateUnavailable}
	}
	if resp.StatusCode != http.StatusOK || !isJSON(resp) {
		return TargetResult{Class: checkClass(resp, body)}
	}
	view, ok := parseListing(body)
	if !ok {
		return TargetResult{Class: topic.RemoteClassEdge}
	}
	res := TargetResult{Verified: true, View: view}
	// An empty recorded ID is a link attached to a topic created before
	// topic IDs (v2.1 and earlier). A topic never gains an ID except by
	// being recreated, so any ID the target reports later is a new
	// topic, as is a recorded ID the target no longer reports.
	targetID, knowsID := view.ParentID, view.ServesIDs
	if !knowsID {
		targetID, knowsID = describeID(ctx, e, topicName)
	}
	switch {
	case view.RemoteChildren > 0 && !topic.RemoteChainsAllowed:
		res.State = topic.RemoteStateTargetHasRemoteChildren
	case knowsID && targetID != recordedTargetID:
		res.State = topic.RemoteStateTargetReplaced
	}
	return res
}

// describeID reads the target topic's id from GET /v1/topics/{t}, for a
// target whose children listing carries no parent_id (v3.1.0). ok is
// false when the answer carries no id (an ID-less topic, or no answer);
// the check then cannot compare.
func describeID(ctx context.Context, e *remote.Entry, topicName string) (id string, ok bool) {
	path, err := remote.TopicPath(topicName)
	if err != nil {
		return "", false
	}
	resp, err := e.Do(ctx, remote.Outbound{Method: http.MethodGet, Path: path})
	if err != nil {
		return "", false
	}
	body, err := remote.ReadBody(resp, remote.MaxReadAnswerBytes)
	if err != nil || resp.StatusCode != http.StatusOK || !isJSON(resp) {
		return "", false
	}
	var d struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(body, &d) != nil || d.ID == "" {
		return "", false
	}
	return d.ID, true
}

// checkClass classifies a failed check's answer as the create-time
// checks do: 401, 403 and 404 in Narad's shape are auth_failed,
// forbidden and target_missing, so a quiet link can say why.
func checkClass(resp *http.Response, body []byte) string {
	if cl := remote.ReadClass(resp, body); cl != "" && cl != topic.RemoteStateUnknown {
		return cl
	}
	if _, narad := remote.NaradError(resp, body); !narad {
		return topic.RemoteClassEdge
	}
	return topic.RemoteStateUnavailable
}

// CheckInterval jitters a remote's check_interval_ms by +-20% so the
// cursors of one remote do not check in lockstep.
func CheckInterval(intervalMs int64) time.Duration {
	base := time.Duration(max(intervalMs, 1000)) * time.Millisecond
	spread := int64(base) * 2 / 5
	return base - time.Duration(spread/2) + time.Duration(rand.Int64N(spread+1))
}
