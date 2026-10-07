package sink

import (
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
)

// Action is what a lane does with a chunk's answer (ch. 6.5).
type Action int

const (
	// ActCommitted: every message was accepted; send the next chunk.
	ActCommitted Action = iota
	// ActResendPrefix: the target refused message Index and stored
	// nothing; resend the messages before it alone, retry that one once
	// as base64 if it went raw, then block the lane on it.
	ActResendPrefix
	// ActBisect: a refusal without an index; split the chunk in halves
	// down to one record to find the culprit.
	ActBisect
	// ActBlock: block the lane on its first record (the target's topic
	// became a delay child or a stub) and re-run the target check.
	ActBlock
	// ActSplitTooLarge: the body was too large; halve the chunk. A single
	// record that still gets it is record_too_large.
	ActSplitTooLarge
	// ActRetry: nothing is known to have been stored, or the chunk may
	// have landed (Ambiguous); resend after the lane's backoff.
	ActRetry
	// ActGate: a remote-wide failure (auth, throttling, TLS); close the
	// remote's gate.
	ActGate
	// ActStall: a failure only a change fixes (a refused grant, a missing
	// topic, a refused destination, a redirect); stall the cursor and
	// retry every StallRetry or at once on a change.
	ActStall
	// ActResolveRoute: Go's own 404 from the batch route; read the
	// children listing to tell a target without batch produce from an
	// edge answer.
	ActResolveRoute
	// ActUncompressed: the target could not decode a compressed chunk;
	// resend it uncompressed at once and stop compressing.
	ActUncompressed
	// ActRecapacity: the target refused the chunk's message count ("too
	// many messages"): it takes fewer per request than its capabilities
	// said (rolled back, or an older pod behind its load balancer).
	// Nothing was stored and no record is at fault; forget the
	// capabilities and resend at the default size.
	ActRecapacity
)

// Verdict is a classified answer.
type Verdict struct {
	Action Action
	// State is the link state the answer puts the lane or cursor in
	// (topic.RemoteState*); "" when committed.
	State string
	// Class labels narad_remote_errors_total: the state, or edge or
	// encoding.
	Class string
	// Status is the HTTP status, 0 for a transport failure.
	Status int
	// Index is N of a "message N:" answer, -1 otherwise.
	Index int
	// Shrink halves the lane's chunk byte cap (a timeout, or the target's
	// read deadline cutting an upload).
	Shrink bool
	// Ambiguous means the chunk may have landed: resending it can make
	// duplicates, counted as resent records.
	Ambiguous bool
	// RetryAfter is a 429's Retry-After, capped at MaxRetryAfter.
	RetryAfter time.Duration
}

// MaxRetryAfter caps how long a target's Retry-After can hold a gate.
const MaxRetryAfter = 60 * time.Second

// Answer is one chunk's outcome as the lane saw it. Body is read through
// remote.ReadBody with remote.MaxProduceAnswerBytes; only its shape, its
// accepted count, its message index and its fixed prefixes are used.
type Answer struct {
	Resp       *http.Response
	Body       []byte
	Err        error
	Chunk      int
	Compressed bool
}

// Classifier maps answers to verdicts. The zero value uses the remote
// package's predicates; tests replace them.
type Classifier struct {
	DestinationRefused func(error) (string, bool)
	IsTLSError         func(error) bool
}

func (c Classifier) destinationRefused(err error) bool {
	f := c.DestinationRefused
	if f == nil {
		f = remote.DestinationRefused
	}
	_, ok := f(err)
	return ok
}

func (c Classifier) tlsError(err error) bool {
	if c.IsTLSError != nil {
		return c.IsTLSError(err)
	}
	return remote.IsTLSError(err)
}

func verdict(action Action, state string, status int) Verdict {
	return Verdict{Action: action, State: state, Class: state, Status: status, Index: -1}
}

// Classify applies the rows of ch. 6.5 in their order: first Go's own
// 404 from the batch route, then edge answers (a 400, 401, 403 or 404
// that is not in Narad's error shape), then a compressed chunk the
// target could not decode, then the rest.
func (c Classifier) Classify(a Answer) Verdict {
	if a.Err != nil {
		return c.classifyTransport(a.Err)
	}
	resp := a.Resp
	status := resp.StatusCode
	if status == http.StatusAccepted {
		if n, ok := acceptedCount(resp, a.Body); ok && n == a.Chunk {
			v := verdict(ActCommitted, "", status)
			return v
		}
		// A different count proves nothing about which messages landed.
		v := verdict(ActRetry, topic.RemoteStateUnavailable, status)
		v.Ambiguous = true
		return v
	}
	if status >= 200 && status < 300 {
		v := verdict(ActRetry, topic.RemoteStateUnavailable, status)
		v.Ambiguous = true
		return v
	}
	if status >= 300 && status < 400 {
		return verdict(ActStall, topic.RemoteStateRedirectRefused, status)
	}
	msg, narad := remote.NaradError(resp, a.Body)
	if status == http.StatusNotFound && !narad {
		if remote.IsGoNotFound(resp, a.Body) {
			return verdict(ActResolveRoute, topic.RemoteStateNoBatchProduce, status)
		}
		return edge(status)
	}
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		if !narad {
			return edge(status)
		}
	}
	if a.Compressed && (status == http.StatusUnsupportedMediaType || (status == http.StatusBadRequest && strings.HasPrefix(msg, "invalid json"))) {
		v := verdict(ActUncompressed, topic.RemoteStateUnavailable, status)
		v.Class = topic.RemoteClassEncoding
		return v
	}
	switch {
	case status == http.StatusRequestEntityTooLarge:
		v := verdict(ActSplitTooLarge, topic.RemoteStateRecordTooLarge, status)
		if narad {
			v.Index = messageIndex(msg)
		}
		return v
	case status == http.StatusTooManyRequests:
		v := verdict(ActGate, topic.RemoteStateThrottled, status)
		v.RetryAfter = retryAfter(resp)
		return v
	case status == http.StatusBadRequest && strings.HasPrefix(msg, "too many messages"):
		return verdict(ActRecapacity, "", status)
	case status == http.StatusBadRequest:
		if i := messageIndex(msg); i >= 0 {
			v := verdict(ActResendPrefix, topic.RemoteStateRejectedRecord, status)
			v.Index = i
			return v
		}
		if strings.HasPrefix(msg, "read body:") {
			// The target's read deadline cut the upload (or it broke):
			// nothing was stored. Never split to find a culprit.
			v := verdict(ActRetry, topic.RemoteStateUnavailable, status)
			v.Shrink = true
			return v
		}
		return verdict(ActBisect, topic.RemoteStateRejectedRecord, status)
	case status == http.StatusUnauthorized:
		return verdict(ActGate, topic.RemoteStateAuthFailed, status)
	case status == http.StatusForbidden:
		return verdict(ActStall, topic.RemoteStateForbidden, status)
	case status == http.StatusNotFound:
		return verdict(ActStall, topic.RemoteStateTargetMissing, status)
	case status == http.StatusConflict:
		return verdict(ActBlock, topic.RemoteStateRejectedRecord, status)
	case status == http.StatusRequestTimeout, status >= 500:
		v := verdict(ActRetry, topic.RemoteStateUnavailable, status)
		v.Ambiguous = true
		return v
	case status == http.StatusUnsupportedMediaType:
		// An uncompressed chunk always carries application/json and
		// X-Narad-Client; a 415 then comes from something in front.
		return edge(status)
	case narad && status >= 400 && status < 500:
		return verdict(ActBisect, topic.RemoteStateRejectedRecord, status)
	}
	return edge(status)
}

// edge is an answer from something in front of the target: retried like
// unavailable, counted as class edge.
func edge(status int) Verdict {
	v := verdict(ActRetry, topic.RemoteStateUnavailable, status)
	v.Class = topic.RemoteClassEdge
	return v
}

func (c Classifier) classifyTransport(err error) Verdict {
	switch {
	case c.destinationRefused(err):
		return verdict(ActStall, topic.RemoteStateDestinationRefused, 0)
	case c.tlsError(err):
		return verdict(ActGate, topic.RemoteStateTLSFailed, 0)
	}
	v := verdict(ActRetry, topic.RemoteStateUnavailable, 0)
	v.Ambiguous = true
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		v.Shrink = true
	}
	return v
}

// ResolveRoute finishes an ActResolveRoute verdict from the target's
// children listing for the topic: a 200 in Narad's shape proves the
// target is up and has the topic, so it lacks the batch route
// (no_batch_produce); anything else is an edge answer.
func ResolveRoute(listing *http.Response, body []byte, err error) Verdict {
	if err == nil && listing.StatusCode == http.StatusOK && isChildrenListing(listing, body) {
		return verdict(ActStall, topic.RemoteStateNoBatchProduce, http.StatusNotFound)
	}
	return edge(http.StatusNotFound)
}

// acceptedCount reads {"accepted":N} from a 202.
func acceptedCount(resp *http.Response, body []byte) (int, bool) {
	if !isJSON(resp) {
		return 0, false
	}
	var shape struct {
		Accepted *int `json:"accepted"`
	}
	if json.Unmarshal(body, &shape) != nil || shape.Accepted == nil {
		return 0, false
	}
	return *shape.Accepted, true
}

// messageIndex parses the N of a "message N: ..." error, -1 when msg has
// no such prefix. Only the integer is used; the text after it can echo
// payload values and is never kept.
func messageIndex(msg string) int {
	rest, ok := strings.CutPrefix(msg, "message ")
	if !ok {
		return -1
	}
	digits, _, ok := strings.Cut(rest, ":")
	if !ok || digits == "" || len(digits) > 9 {
		return -1
	}
	n, err := strconv.Atoi(digits)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// retryAfter reads a Retry-After of whole seconds, capped at
// MaxRetryAfter; 0 when absent or not a number.
func retryAfter(resp *http.Response) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After")))
	if err != nil || n <= 0 {
		return 0
	}
	return min(time.Duration(n)*time.Second, MaxRetryAfter)
}

func isJSON(resp *http.Response) bool {
	mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}
