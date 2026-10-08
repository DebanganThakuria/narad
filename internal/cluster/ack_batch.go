package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// ackBatcher is the batch surface of the peer client: several
// ack-shaped records for one owner in one RPC. *PeerClient implements
// it. A peer client without it (a test fake) has every ack sent on its
// own, which is always correct, only slower.
type ackBatcher interface {
	AckBatchWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckBatchRequest) (nodewire.Response, error)
}

// AckBatchWithin sends several ack-shaped records to the owner at addr
// in one RPC on the ack lane, bounded by timeout (see peerClient). A
// 200 reply's body carries one nodewire.AckResult per record; see
// nodewire.DecodeAckBatchReply. An owner that predates the op answers
// 400 "unsupported rpc operation"; see isUnsupportedOp.
func (c *PeerClient) AckBatchWithin(ctx context.Context, addr string, timeout time.Duration, req nodewire.AckBatchRequest) (nodewire.Response, error) {
	payload, err := nodewire.EncodeAckBatchRequest(req)
	return c.sendWithin(ctx, addr, "ack_batch", laneAck, timeout, payload, err)
}

// isUnsupportedOp reports whether res is an owner refusing an operation
// it does not know: a node on a release before the operation existed.
func isUnsupportedOp(res nodewire.Response) bool {
	return res.Status == http.StatusBadRequest && bytes.Contains(res.Body, []byte("unsupported rpc operation"))
}

// IsTrailingFieldRefusal reports whether res is a peer refusing a
// request for a field it does not know: a node on a release before the
// field existed rejects the whole payload with 400 and
// nodewire.TrailingPayloadError. It matches only "trailing", the part
// every release sends. The heartbeat and join senders (cmd/narad) use it
// to resend the frame without the new fields.
func IsTrailingFieldRefusal(res nodewire.Response) bool {
	return res.Status == http.StatusBadRequest && bytes.Contains(res.Body, []byte("trailing"))
}

// legacyAckBatchTTL is how long an owner that refused OpAckBatch gets
// every ack on its own. As with legacyClaimTTL: long enough that a
// rolling upgrade costs one refused batch per owner per TTL, short
// enough that an upgraded owner is batched again within minutes.
const legacyAckBatchTTL = 2 * time.Minute

// ackBatchLegacy remembers the owners (by address) that refused
// OpAckBatch, each with the time the refusal stops counting. count lets
// the per-ack check skip the map entirely on a fully upgraded cluster,
// which is the only state that lasts.
type ackBatchLegacy struct {
	peers sync.Map // addr -> time.Time
	count atomic.Int64
}

// note records that addr refused the op.
func (l *ackBatchLegacy) note(addr string) {
	if _, loaded := l.peers.Swap(addr, time.Now().Add(legacyAckBatchTTL)); !loaded {
		l.count.Add(1)
	}
}

// is reports whether acks to addr must go out one at a time, and
// forgets an entry whose TTL has passed so the op is tried again.
func (l *ackBatchLegacy) is(addr string) bool {
	if l.count.Load() == 0 {
		return false
	}
	v, ok := l.peers.Load(addr)
	if !ok {
		return false
	}
	if time.Now().Before(v.(time.Time)) {
		return true
	}
	if l.peers.CompareAndDelete(addr, v) {
		l.count.Add(-1)
	}
	return false
}

// Ack operations named on the batch route, the op parameter of
// RouteAckBatch. The HTTP layer passes the same strings.
const (
	ackOpAck    = "ack"
	ackOpExtend = "extend"
	ackOpNack   = "nack"
)

// ackModeOf maps a batch-route op to its wire mode. An op it does not
// know is refused, not taken as an ack: the strings cross a package
// boundary, and a nack or extend misread as a commit would delete a
// record its consumer wanted back.
func ackModeOf(op string) (nodewire.AckMode, bool) {
	switch op {
	case ackOpAck:
		return nodewire.AckModeAck, true
	case ackOpExtend:
		return nodewire.AckModeExtend, true
	case ackOpNack:
		return nodewire.AckModeNack, true
	default:
		return 0, false
	}
}

// ownerDownMessage is what writeOwnerDown answers a single ack with.
const ownerDownMessage = "partition owner is down; retry later"

// RouteAckBatch settles the handles of one HTTP batch ack whose
// partitions other nodes own: one OpAckBatch per owner, the owners
// asked concurrently, so one slow or failed owner costs only its own
// records. op is "ack", "extend" or "nack"; any other op settles
// nothing and fails every handle not already rejected with a 500.
//
// statuses and msgs are parallel to handles. An entry whose status is
// already set (the caller rejected that handle) is skipped. For every
// other handle the router fills in the status and, for a failure, the
// message a single ack of that handle would have answered, exactly:
// 204, the owner's 410/400/421/..., 503 for an owner that is down, 502
// for a transport failure. A handle this node owns is left at status 0
// for the caller to apply locally.
func (rt *Router) RouteAckBatch(ctx context.Context, topicName, op string, handles []consumer.Handle, statuses []int, msgs []string) {
	mode, ok := ackModeOf(op)
	if !ok {
		for i := range handles {
			if statuses[i] == 0 {
				statuses[i], msgs[i] = http.StatusInternalServerError, "unknown ack op "+op
			}
		}
		return
	}
	var groups []ackGroup
	for i, h := range handles {
		if statuses[i] != 0 {
			continue
		}
		addr, unavailable := rt.ownerRoute(topicName, h.Partition)
		switch {
		case unavailable:
			statuses[i], msgs[i] = http.StatusServiceUnavailable, ownerDownMessage
		case addr == "":
			// Local: the caller applies it.
		default:
			groups = addToAckGroup(groups, addr, i)
		}
	}
	switch len(groups) {
	case 0:
	case 1:
		rt.settleAckGroup(ctx, groups[0], topicName, mode, handles, statuses, msgs)
	default:
		// Each group writes only its own indices of statuses and msgs.
		var wg sync.WaitGroup
		for _, g := range groups[1:] {
			wg.Go(func() { rt.settleAckGroup(ctx, g, topicName, mode, handles, statuses, msgs) })
		}
		rt.settleAckGroup(ctx, groups[0], topicName, mode, handles, statuses, msgs)
		wg.Wait()
	}
}

// ackGroup is the handles of one batch bound for one owner.
type ackGroup struct {
	addr string
	idx  []int
}

func addToAckGroup(groups []ackGroup, addr string, i int) []ackGroup {
	for g := range groups {
		if groups[g].addr == addr {
			groups[g].idx = append(groups[g].idx, i)
			return groups
		}
	}
	return append(groups, ackGroup{addr: addr, idx: []int{i}})
}

// settleAckGroup sends one owner's share of a batch ack as one
// OpAckBatch, or record by record to an owner that does not speak it.
func (rt *Router) settleAckGroup(ctx context.Context, g ackGroup, topicName string, mode nodewire.AckMode, handles []consumer.Handle, statuses []int, msgs []string) {
	item := func(i int) nodewire.AckBatchItem {
		h := handles[i]
		return nodewire.AckBatchItem{Topic: topicName, Partition: h.Partition, Offset: h.Offset, Nonce: h.Nonce, Mode: mode}
	}
	batcher, ok := rt.peer.(ackBatcher)
	if ok && len(g.idx) > 1 && !rt.acks.legacy.is(g.addr) {
		req := nodewire.AckBatchRequest{Items: make([]nodewire.AckBatchItem, len(g.idx))}
		for k, i := range g.idx {
			req.Items[k] = item(i)
		}
		res, err := batcher.AckBatchWithin(ctx, g.addr, ackForwardTimeout, req)
		if err == nil && isUnsupportedOp(res) {
			rt.acks.legacy.note(g.addr)
		} else {
			results, rerr := ackBatchResults(ctx, res, err, len(g.idx))
			if rerr != nil && rerr.cause != nil {
				rt.noteAckFailure(g.addr, mode, rerr.status, rerr.cause, len(g.idx))
			}
			for k, i := range g.idx {
				if rerr != nil {
					statuses[i], msgs[i] = rerr.status, rerr.msg
					continue
				}
				statuses[i], msgs[i] = results[k].Status, results[k].Error
			}
			return
		}
	}
	for _, i := range g.idx {
		res, err := rt.sendSingleAck(ctx, g.addr, ackForwardTimeout, item(i))
		if err != nil {
			statuses[i], msgs[i] = rt.ackFailureFor(ctx, g.addr, mode, err)
			continue
		}
		statuses[i], msgs[i] = ackOutcome(res)
	}
}

// ackBatchFailure is an OpAckBatch round trip that did not produce
// per-record results; every record of the batch gets this outcome.
type ackBatchFailure struct {
	status int
	msg    string
	// cause is the transport error or undecodable reply behind a
	// forward that failed; nil for an owner's own refusal of the batch.
	cause error
}

// ackBatchResults turns an OpAckBatch round trip into n per-record
// results, or the one outcome all n records share when the batch as a
// whole failed: a transport error is what a single forwarded ack's
// would be (see ackForwardFailure), a non-200 reply (an owner that gave
// up on the batch while it waited for a slot, say) is that reply, and a
// reply that does not decode is an unknown outcome, since the owner ran
// the records.
func ackBatchResults(ctx context.Context, res nodewire.Response, err error, n int) ([]nodewire.AckResult, *ackBatchFailure) {
	if err != nil {
		status, msg := ackForwardFailure(ctx, err)
		return nil, &ackBatchFailure{status: status, msg: msg, cause: err}
	}
	if res.Status != http.StatusOK {
		status, msg := ackOutcome(res)
		return nil, &ackBatchFailure{status: status, msg: msg}
	}
	results, err := nodewire.DecodeAckBatchReply(res.Body, nil)
	if err == nil && len(results) != n {
		err = errors.New("wrong record count")
	}
	if err != nil {
		err = fmt.Errorf("invalid ack batch reply: %w", err)
		status, msg := ackForwardFailure(ctx, err)
		return nil, &ackBatchFailure{status: status, msg: msg, cause: err}
	}
	return results, nil
}

// sendSingleAck forwards one ack-shaped record as the op of its own
// mode, exactly as RouteAck, RouteExtendAck and RouteNack always have.
func (rt *Router) sendSingleAck(ctx context.Context, addr string, timeout time.Duration, item nodewire.AckBatchItem) (nodewire.Response, error) {
	req := nodewire.AckRequest{Topic: item.Topic, Partition: item.Partition, Offset: item.Offset, Nonce: item.Nonce}
	switch item.Mode {
	case nodewire.AckModeExtend:
		return rt.peer.ExtendAckWithin(ctx, addr, timeout, req)
	case nodewire.AckModeNack:
		return rt.peer.NackWithin(ctx, addr, timeout, req)
	default:
		return rt.peer.AckWithin(ctx, addr, timeout, req)
	}
}

// ackOutcome is the status and error message an owner's reply to a
// forwarded ack comes to: what writePeerResponse would have written. A
// round trip that failed is ackForwardFailure's.
func ackOutcome(res nodewire.Response) (int, string) {
	status := res.Status
	if status == 0 {
		status = http.StatusOK
	}
	if status < http.StatusMultipleChoices {
		return status, ""
	}
	return status, errorBodyText(res.ContentType, res.Body)
}

// statusClientClosedRequest is the non-standard 499 the HTTP layer
// answers a request whose client went away with (handlers cannot be
// imported from here; see handlers.StatusClientClosedRequest).
const statusClientClosedRequest = 499

// What a forwarded ack, extend or nack that got no answer from its owner
// tells the client. Neither names the owner or carries the transport
// error, which are this node's business; ackForwardFailure's callers log
// them.
const (
	ackNotSentMessage      = "the ack did not reach the partition owner and was not applied; retry"
	ackUnknownMessage      = "the partition owner did not answer; the ack may have been applied; retry"
	clientClosedAckMessage = "client closed request"
)

// ackForwardFailure is the status and message a forwarded ack-shaped
// record answers when its round trip to the owner failed. A request
// whose client left is 499: nobody reads the answer, and it is not the
// owner's failure. A request that never left this node (it waited out
// its queue, or the transport refused it before writing a byte, see
// clusterrpc.ErrNotSent) is 503 with nothing applied, and the single
// route adds Retry-After. Anything else may have reached the owner and
// been applied, so it is 502, the code that already says so; a retry of
// an ack that landed answers 410.
func ackForwardFailure(ctx context.Context, err error) (int, string) {
	switch {
	case ctx.Err() != nil:
		return statusClientClosedRequest, clientClosedAckMessage
	case errors.Is(err, clusterrpc.ErrNotSent):
		return http.StatusServiceUnavailable, ackNotSentMessage
	default:
		return http.StatusBadGateway, ackUnknownMessage
	}
}

// errorBodyText extracts the message from an error body: the "error"
// field of a JSON one, else the trimmed text.
func errorBodyText(contentType string, body []byte) string {
	if contentType == nodewire.ContentTypeJSON {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return e.Error
		}
	}
	return strings.TrimSpace(string(body))
}
