package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// purgeBroadcaster fans a topic purge out to the other cluster members.
// *Router implements it; the RPC server holds it so a delete that was
// forwarded to the leader over RPC still triggers the same owner-pod purge
// the HTTP handler does for a leader-direct delete.
type purgeBroadcaster interface {
	BroadcastDeleteTopic(ctx context.Context, topicName, id string) error
}

// RPCServer serves node-to-node RPC frames: it decodes each request payload,
// invokes the local broker, and encodes the response frame. It is the peer
// side of PeerClient.
type RPCServer struct {
	broker      broker.Broker
	store       *metastore.Store
	logger      *slog.Logger
	broadcaster purgeBroadcaster

	// maxConsumeWait caps a wire-supplied consume wait. Zero means
	// defaultMaxConsumeWait; serve.go wires the configured
	// http.max_consume_wait via SetMaxConsumeWait so the RPC-side clamp
	// agrees with the router's and the HTTP handlers'.
	maxConsumeWait time.Duration

	// messagingSem bounds how many messaging handlers (produce commits,
	// acks, non-blocking consumes) execute at once. The read loop still
	// spawns a goroutine per frame so it never blocks; the goroutine
	// waits here before touching the broker. nil disables gating
	// (zero-value servers in tests); NewRPCServer sizes it to
	// 4*GOMAXPROCS. Ops that call peers (delete_topic's purge broadcast)
	// are never gated: they could hold a slot while waiting on a node
	// that is itself waiting on us.
	messagingSem chan struct{}

	// transferSem bounds the partition-transfer ops (segment listing and
	// chunk reads): each chunk read pins up to storage.MaxSegmentReadBytes,
	// so an unbounded number of them from one peer is an unbounded amount
	// of memory and disk reads. controlSem bounds the remaining control
	// ops (topic lookups, stats, membership, moves, users, fan-out
	// cursors) so a peer cannot run thousands of them at once either. Ops
	// that call OTHER peers or may park for a long time (delete_topic's
	// purge broadcast, create_topic behind the startup create gate) are
	// never gated: a held slot waiting on another node is how deadlocks
	// and heartbeat starvation start. nil disables a gate.
	transferSem chan struct{}
	controlSem  chan struct{}

	// deliveries remembers messages handed to forwarded consumes whose
	// client may cancel after the reply was already sent; see
	// HandleStreamCancel. deliveryExpiry is the same records in insertion
	// order with their deadlines, so expiring old ones is a pop from the
	// front, never a scan of the map. now is the clock (tests inject one).
	deliveriesMu   sync.Mutex
	deliveries     map[requestKey]delivery
	deliveryExpiry []deliveryDeadline
	now            func() time.Time
}

// deliveryDeadline is one entry of the delivery expiry queue.
type deliveryDeadline struct {
	key      requestKey
	expireAt time.Time
}

// NewRPCServer constructs an RPCServer around the local broker.
func NewRPCServer(br broker.Broker, store *metastore.Store, logger *slog.Logger) *RPCServer {
	s := &RPCServer{broker: br, store: store, logger: logger}
	s.SetMessagingConcurrency(defaultMessagingConcurrency())
	s.SetTransferConcurrency(defaultTransferConcurrency)
	s.SetControlConcurrency(defaultControlConcurrency)
	return s
}

// defaultTransferConcurrency is the transfer-op ceiling: at
// storage.MaxSegmentReadBytes per read that is at most 32 MiB of chunk
// buffers in flight per node, while the mover (one sequential chunk
// stream per move) never queues behind it in practice.
const defaultTransferConcurrency = 8

// defaultControlConcurrency is the control-op ceiling. Control ops are
// short local reads and metastore writes; the bound only has to stop a
// runaway peer, not shape normal traffic.
const defaultControlConcurrency = 64

// defaultMessagingConcurrency is the messaging-handler ceiling applied
// by NewRPCServer: bounded so a burst of forwarded traffic degrades into
// queueing instead of thousands of goroutines contending for the same
// partition locks and fsyncs. Commit handlers mostly wait on fdatasync
// rather than CPU, so the bound has a floor well above the core count:
// a small pod must still absorb several ingress nodes' dispatchers (16
// concurrent batches each) without queueing them behind one another.
func defaultMessagingConcurrency() int {
	return max(minMessagingConcurrency, 4*runtime.GOMAXPROCS(0))
}

const minMessagingConcurrency = 64

// SetMessagingConcurrency bounds concurrently executing messaging
// handlers to n; n <= 0 disables the bound. Call before serving.
func (s *RPCServer) SetMessagingConcurrency(n int) {
	if n <= 0 {
		s.messagingSem = nil
		return
	}
	s.messagingSem = make(chan struct{}, n)
}

// SetTransferConcurrency bounds concurrently executing partition-transfer
// handlers to n; n <= 0 disables the bound. Call before serving.
func (s *RPCServer) SetTransferConcurrency(n int) {
	s.transferSem = newSemaphore(n)
}

// SetControlConcurrency bounds concurrently executing control handlers
// to n; n <= 0 disables the bound. Call before serving.
func (s *RPCServer) SetControlConcurrency(n int) {
	s.controlSem = newSemaphore(n)
}

func newSemaphore(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// SetMaxConsumeWait wires the configured long-poll consume wait ceiling
// (http.max_consume_wait). Values <= 0 keep the defaultMaxConsumeWait
// fallback, matching the router and the HTTP handlers.
func (s *RPCServer) SetMaxConsumeWait(d time.Duration) {
	if d > 0 {
		s.maxConsumeWait = d
	}
}

func (s *RPCServer) consumeWaitCeiling() time.Duration {
	if s.maxConsumeWait > 0 {
		return s.maxConsumeWait
	}
	return defaultMaxConsumeWait
}

// SetBroadcaster wires the purge fan-out used when a topic delete is
// forwarded to this node as the leader. Without it, a delete that arrives
// over RPC (i.e. from a follower) deletes the metastore record and purges
// only this node's files, leaving the owner pods' partition directories
// orphaned until the next startup sweep.
func (s *RPCServer) SetBroadcaster(b purgeBroadcaster) {
	s.broadcaster = b
}

// HandleStreamFrame serves a node RPC frame under a background context.
// The stream server prefers HandleStreamRequest; this form remains for
// callers without a per-request context (tests, older transports).
func (s *RPCServer) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	return s.HandleStreamRequest(context.Background(), frame, respond)
}

// HandleStreamRequest serves a node RPC frame, replying asynchronously
// via respond. ctx is cancelled when the client sends a cancel for this
// request or its stream ends, so a forwarded long-poll stops parking on
// behalf of a client that is gone. It reports whether the frame was one
// this server handles.
func (s *RPCServer) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	go func() {
		res := s.dispatch(ctx, requestKey{stream: clusterrpc.StreamIDFromContext(ctx), request: frame.RequestID}, frame.Payload)
		payload, err := nodewire.EncodeResponse(res)
		if err != nil {
			payload, _ = nodewire.EncodeResponse(errorResponse(http.StatusInternalServerError, "encode rpc response failed"))
		}
		respond(clusterwire.StreamFrame{
			Type:      clusterwire.StreamFrameNodeReply,
			RequestID: frame.RequestID,
			Payload:   payload,
		})
	}()
	return true
}

// HandleStreamCancel gives back a message that a consume delivered for a
// request whose client stopped waiting (see clusterwire.StreamFrameCancel).
// Without it the reservation would hide the message until its lease
// expired, for a consumer that never saw it.
func (s *RPCServer) HandleStreamCancel(streamID, requestID uint64) {
	d, ok := s.takeDelivery(requestKey{stream: streamID, request: requestID})
	if !ok {
		return
	}
	s.releaseDelivery(d)
}

// requestKey identifies one in-flight or just-answered RPC request.
type requestKey struct {
	stream, request uint64
}

// delivery is a message a consume handler handed to a client that may
// have stopped listening.
type delivery struct {
	topic    string
	handle   consumer.Handle
	at       time.Time
	expireAt time.Time
}

// deliveryCancelGrace bounds how long a delivered handle is remembered
// after the consume handler produced it. The record only matters for a
// cancel that races the reply (the client stopped waiting as the reply
// was being written); a client that read the reply never cancels. So
// the window is the reply's flight time, not the visibility timeout.
// Under load a node hands out tens of thousands of forwarded messages
// per second, and remembering each for 30 s (the previous TTL) kept
// hundreds of thousands of records that every consume then swept under
// the mutex: the profile showed 8 to 11% of broker CPU in that sweep
// and every forwarded consume serialised behind it.
const deliveryCancelGrace = 2 * time.Second

// deliveryExpiryBudget bounds how many expired records one call may
// drop so a burst never makes a single consume pay for the backlog.
const deliveryExpiryBudget = 64

func (s *RPCServer) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// rememberDelivery records a handle a forwarded consume is about to
// answer with, so a cancel that races the reply can give it back.
func (s *RPCServer) rememberDelivery(key requestKey, topicName string, h consumer.Handle) {
	now := s.clock()
	s.deliveriesMu.Lock()
	if s.deliveries == nil {
		s.deliveries = make(map[requestKey]delivery)
	}
	s.expireDeliveriesLocked(now)
	expireAt := now.Add(deliveryCancelGrace)
	s.deliveries[key] = delivery{topic: topicName, handle: h, at: now, expireAt: expireAt}
	s.deliveryExpiry = append(s.deliveryExpiry, deliveryDeadline{key: key, expireAt: expireAt})
	s.deliveriesMu.Unlock()
}

// expireDeliveriesLocked drops records whose grace elapsed, oldest first.
// A queue entry whose record was already taken (cancelled) or replaced
// by a later delivery under the same key is skipped. Must hold
// deliveriesMu.
func (s *RPCServer) expireDeliveriesLocked(now time.Time) {
	n := 0
	for n < len(s.deliveryExpiry) && n < deliveryExpiryBudget && !s.deliveryExpiry[n].expireAt.After(now) {
		e := s.deliveryExpiry[n]
		if d, ok := s.deliveries[e.key]; ok && d.expireAt.Equal(e.expireAt) {
			delete(s.deliveries, e.key)
		}
		n++
	}
	if n > 0 {
		// Shift in place; the queue is a FIFO whose head moves forward,
		// and copying the remainder keeps the backing array from growing
		// without bound.
		s.deliveryExpiry = append(s.deliveryExpiry[:0], s.deliveryExpiry[n:]...)
	}
}

// forgetDelivery drops the record once it is clear the client either got
// the reply or was already handled.
func (s *RPCServer) forgetDelivery(key requestKey) {
	s.deliveriesMu.Lock()
	delete(s.deliveries, key)
	s.deliveriesMu.Unlock()
}

func (s *RPCServer) takeDelivery(key requestKey) (delivery, bool) {
	s.deliveriesMu.Lock()
	defer s.deliveriesMu.Unlock()
	d, ok := s.deliveries[key]
	if ok {
		delete(s.deliveries, key)
	}
	return d, ok
}

// releaseDelivery nacks a delivered message so it is redeliverable now.
// A stale handle (already acked or released) is not an error.
func (s *RPCServer) releaseDelivery(d delivery) {
	if err := s.broker.Nack(rpcRequestContext(), d.topic, d.handle); err != nil && !errors.Is(err, consumer.ErrHandleStale) && s.logger != nil {
		s.logger.Warn("release consume delivery after client cancel", "topic", d.topic, "err", err)
	}
}

// withMessagingSlot runs handle under the messaging concurrency bound.
func (s *RPCServer) withMessagingSlot(handle func() nodewire.Response) nodewire.Response {
	if sem := s.messagingSem; sem != nil {
		sem <- struct{}{}
		defer func() { <-sem }()
	}
	return handle()
}

// withSlot runs handle under sem, or answers 503 if the request is
// cancelled (client gone, stream closed) before a slot frees up. A nil
// sem runs handle directly.
func (s *RPCServer) withSlot(ctx context.Context, sem chan struct{}, handle func() nodewire.Response) nodewire.Response {
	if sem == nil {
		return handle()
	}
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot")
	}
	defer func() { <-sem }()
	return handle()
}

func (s *RPCServer) dispatch(ctx context.Context, key requestKey, payload []byte) nodewire.Response {
	op, err := nodewire.OperationOf(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid rpc request")
	}
	var res nodewire.Response
	switch op {
	case nodewire.OpProduce:
		res = s.handleProduce(ctx, payload)
	case nodewire.OpCommitProduce:
		res = s.withMessagingSlot(func() nodewire.Response { return s.handleCommitProduce(ctx, payload) })
	case nodewire.OpCommitProduceBatch:
		res = s.withMessagingSlot(func() nodewire.Response { return s.handleCommitProduceBatch(ctx, payload) })
	case nodewire.OpConsume:
		// Gated inside the handler: only non-blocking scans take a slot.
		res = s.handleConsume(ctx, key, payload)
	case nodewire.OpAck:
		res = s.withMessagingSlot(func() nodewire.Response { return s.handleAck(ctx, payload) })
	case nodewire.OpExtendAck:
		res = s.withMessagingSlot(func() nodewire.Response { return s.handleExtendAck(ctx, payload) })
	case nodewire.OpNack:
		res = s.withMessagingSlot(func() nodewire.Response { return s.handleNack(ctx, payload) })
	case nodewire.OpListPartitionSegments:
		res = s.withSlot(ctx, s.transferSem, func() nodewire.Response { return s.handleListPartitionSegments(payload) })
	case nodewire.OpFetchSegmentChunk:
		res = s.withSlot(ctx, s.transferSem, func() nodewire.Response { return s.handleFetchSegmentChunk(payload) })
	case nodewire.OpCreateTopic:
		// Ungated: may park behind the startup create gate for up to the
		// forwarded-create timeout.
		res = s.handleCreateTopic(payload)
	case nodewire.OpDeleteTopic:
		// Ungated: broadcasts the purge to the partition owners.
		res = s.handleDeleteTopic(payload)
	default:
		handle, ok := s.controlHandler(op)
		if !ok {
			res = errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", op))
			break
		}
		res = s.withSlot(ctx, s.controlSem, func() nodewire.Response { return handle(payload) })
	}
	return res
}

// controlHandler maps a control op to its handler; ok is false for an
// op this server does not serve. Control ops run under controlSem.
func (s *RPCServer) controlHandler(op nodewire.Operation) (handle func([]byte) nodewire.Response, ok bool) {
	switch op {
	case nodewire.OpGetTopic:
		return s.handleGetTopic, true
	case nodewire.OpJoinCluster:
		return s.handleJoinCluster, true
	case nodewire.OpPrepareHandoff:
		return s.handlePrepareHandoff, true
	case nodewire.OpDecommissionMember:
		return s.handleDecommissionMember, true
	case nodewire.OpCompleteMove:
		return s.handleCompleteMove, true
	case nodewire.OpAbortMove:
		return s.handleAbortMove, true
	case nodewire.OpGetAssignment:
		return s.handleGetAssignment, true
	case nodewire.OpAlterTopic:
		return s.handleAlterTopic, true
	case nodewire.OpPurgeTopic:
		return s.handlePurgeTopic, true
	case nodewire.OpTopicPartitionStats:
		return s.handleTopicPartitionStats, true
	case nodewire.OpRegisterMember:
		return s.handleRegisterMember, true
	case nodewire.OpCreateUser:
		return s.handleCreateUser, true
	case nodewire.OpUpdateUser:
		return s.handleUpdateUser, true
	case nodewire.OpDeleteUser:
		return s.handleDeleteUser, true
	case nodewire.OpAttachChild:
		return s.handleAttachChild, true
	case nodewire.OpDetachChild:
		return s.handleDetachChild, true
	case nodewire.OpFanoutCursors:
		return s.handleFanoutCursors, true
	default:
		return nil, false
	}
}

// brokerError maps a broker failure onto the RPC status vocabulary shared
// with the HTTP layer. Unrecognized errors are logged and reported as opaque
// 500s so internal details never cross the wire.
func (s *RPCServer) brokerError(op string, err error) nodewire.Response {
	switch {
	case errors.Is(err, errs.ErrTopicNotFound):
		return errorResponse(http.StatusNotFound, "topic not found")
	case errors.Is(err, errs.ErrTopicAlreadyExists):
		return errorResponse(http.StatusConflict, "topic already exists")
	case errors.Is(err, errs.ErrHandleMalformed):
		return errorResponse(http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrHandleStale):
		return errorResponse(http.StatusGone, err.Error())
	case errors.Is(err, errs.ErrAckedAheadFull):
		return errorResponse(http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, errs.ErrInvalidArgument),
		errors.Is(err, errs.ErrPartitionRequired):
		return errorResponse(http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrNotPartitionOwner):
		return errorResponse(http.StatusMisdirectedRequest, err.Error())
	case errors.Is(err, errs.ErrFanoutRoleConflict),
		errors.Is(err, errs.ErrFanoutChildLimit),
		errors.Is(err, errs.ErrFanoutSchemaMismatch),
		errors.Is(err, errs.ErrFanoutSchemaManaged),
		errors.Is(err, errs.ErrDelayedChildProduce),
		errors.Is(err, errs.ErrFanoutDelayTooLong), // 409, as the HTTP layer maps it
		errors.Is(err, errs.ErrSchemaVersionConflict),
		errors.Is(err, errs.ErrSchemaHistoryFull),
		errors.Is(err, errs.ErrAlreadyExists):
		return errorResponse(http.StatusConflict, err.Error())
	case errors.Is(err, errs.ErrNotFound):
		return errorResponse(http.StatusNotFound, err.Error())
	default:
		if s.logger != nil {
			s.logger.Error(op, "err", err)
		}
		return errorResponse(http.StatusInternalServerError, op+" failed")
	}
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func jsonResponse(status int, v any) nodewire.Response {
	body, err := json.Marshal(v)
	if err != nil {
		return errorResponse(http.StatusInternalServerError, "encode response failed")
	}
	body = append(body, '\n')
	return nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON, Body: body}
}

func errorResponse(status int, msg string) nodewire.Response {
	body, _ := json.Marshal(map[string]string{"error": msg})
	body = append(body, '\n')
	return nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON, Body: body}
}

// RPC frames do not carry a caller context. The transport layer owns request
// timeouts, so broker operations run under a fresh internal context here.
func rpcRequestContext() context.Context {
	return context.Background()
}
