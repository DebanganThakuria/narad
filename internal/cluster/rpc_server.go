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
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
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

	// purgeApplyWait overrides purgeApplyWaitTimeout, how long a purge
	// waits for the local replica to reflect the deletion. Zero (every
	// production server) means the default; tests shorten it.
	purgeApplyWait time.Duration

	// messagingSem bounds how many messaging handlers (acks, extends,
	// nacks, non-blocking consumes) execute at once, and commitSem how
	// many produce commits do. The read loop still spawns a goroutine
	// per frame so it never blocks; the goroutine waits here before
	// touching the broker. The two are separate because a commit holds
	// its slot across a segment fsync and an HWM fdatasync, while an ack
	// or a probe is in-memory bookkeeping and at most one read: sharing
	// one gate queued them behind the disk whenever commit fan-in
	// approached its size. nil disables gating (zero-value servers in
	// tests); NewRPCServer sizes both to max(64, 4*GOMAXPROCS). Ops that
	// call peers (delete_topic's purge broadcast) are never gated: they
	// could hold a slot while waiting on a node that is itself waiting
	// on us.
	messagingSem chan struct{}
	commitSem    chan struct{}

	// transferSem bounds the partition-transfer ops (segment listing and
	// chunk reads): each chunk read pins up to storage.MaxSegmentReadBytes,
	// so an unbounded number of them from one peer is an unbounded amount
	// of memory and disk reads. A transfer op keeps its slot until its
	// reply has been written, since encoding and writing the reply is
	// where the chunk's memory lives. controlSem bounds the remaining control
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
	// order with their deadlines, from deliveryHead on, so expiring old
	// ones is a pop from the front, never a scan of the map. now is the
	// clock (tests inject one).
	// tokens holds the standing interest peers have registered with this
	// node (the owner half of the token protocol); demand is how an
	// inbound notification reaches a consumer parked here (the requester
	// half). Both nil disables the protocol, and peers fall back to the
	// polling path.
	tokens *tokenHolder
	demand localDemand

	deliveriesMu   sync.Mutex
	deliveries     map[requestKey]delivery
	deliveryExpiry []deliveryDeadline
	deliveryHead   int
	now            func() time.Time

	// olderReleaseLogged is when the leader last logged refusing each
	// joiner ID as older than every member (logOlderReleaseJoin); entries
	// older than olderReleaseLogEvery are dropped on the next insert.
	olderReleaseMu     sync.Mutex
	olderReleaseLogged map[string]time.Time
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
// replies in flight per node (twice that for an instant while a reply
// is encoded from its read buffer), while the mover (one sequential
// chunk stream per move) never queues behind it in practice.
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
// handlers to n, and concurrently executing produce commits to another
// n; n <= 0 disables both bounds. Call before serving.
func (s *RPCServer) SetMessagingConcurrency(n int) {
	s.messagingSem = newSemaphore(n)
	s.commitSem = newSemaphore(n)
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
		res, held := s.serve(ctx, requestKey{stream: clusterrpc.StreamIDFromContext(ctx), request: frame.RequestID}, frame.Payload)
		writeReply(frame.RequestID, res, held, respond)
	}()
	return true
}

// writeReply encodes res into a recycled buffer and hands it to respond.
// respond writes the reply and keeps no reference to it (see
// clusterrpc.StreamFrameHandler), so once it returns the buffer, and any
// slot the reply held, is free again.
//
// It is kept out of line so its frame is not part of the request
// goroutine's while the handler runs: every request starts on a fresh
// minimum-size stack, and a handler path that outgrows it pays a stack
// copy per request.
//
//go:noinline
func writeReply(requestID uint64, res nodewire.Response, held chan struct{}, respond func(clusterwire.StreamFrame)) {
	buf := replyBuffers.Get().(*[]byte)
	payload, err := nodewire.AppendResponse((*buf)[:0], res)
	if err != nil {
		payload, _ = nodewire.AppendResponse((*buf)[:0], errorResponse(http.StatusInternalServerError, "encode rpc response failed"))
	}
	respond(clusterwire.StreamFrame{
		Type:      clusterwire.StreamFrameNodeReply,
		RequestID: requestID,
		Payload:   payload,
	})
	if held != nil {
		<-held
	}
	if cap(payload) <= maxPooledReplyBytes {
		*buf = payload[:0]
		replyBuffers.Put(buf)
	}
}

// replyBuffers recycles the buffers node RPC replies are encoded into,
// so a reply costs no allocation of its own. A buffer grown past
// maxPooledReplyBytes (a segment chunk, a large consume reply) is left
// to the collector rather than pinned in the pool.
var replyBuffers = sync.Pool{New: func() any {
	buf := make([]byte, 0, 512)
	return &buf
}}

const maxPooledReplyBytes = 64 << 10

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
// have stopped listening. rest holds the receipt handles of the other
// records of a batch reply, as they went out; a single record has none.
// They stay encoded because a cancel is rare: decoding one per record
// on every batch would cost more than the record keeping.
type delivery struct {
	topic    string
	handle   consumer.Handle
	rest     []string
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
	s.rememberDeliveries(key, topicName, h, nil)
}

// rememberDeliveries is rememberDelivery for a reply that carries
// several records: h is the first one's handle, and rest the receipt
// handles of the others. A cancel that races the reply gives them all
// back.
func (s *RPCServer) rememberDeliveries(key requestKey, topicName string, h consumer.Handle, rest []string) {
	now := s.clock()
	s.deliveriesMu.Lock()
	if s.deliveries == nil {
		s.deliveries = make(map[requestKey]delivery)
	}
	s.expireDeliveriesLocked(now)
	expireAt := now.Add(deliveryCancelGrace)
	s.deliveries[key] = delivery{topic: topicName, handle: h, rest: rest, expireAt: expireAt}
	s.deliveryExpiry = append(s.deliveryExpiry, deliveryDeadline{key: key, expireAt: expireAt})
	s.deliveriesMu.Unlock()
}

// expireDeliveriesLocked drops records whose grace elapsed, oldest first.
// A queue entry whose record was already taken (cancelled) or replaced
// by a later delivery under the same key is skipped. Must hold
// deliveriesMu.
//
// The queue's head moves forward over the spent entries rather than
// shifting the rest down on every pop: once the queue is full, nearly
// every delivery expires one, and the shift cost a copy of the whole
// queue each time (57 us per delivery at 50k a second). The live
// entries move to the front only once the spent ones are more than
// half the array, which the pops since the last move have paid for, so
// a delivery costs O(1) amortized and the array stays within about
// twice the live entries.
func (s *RPCServer) expireDeliveriesLocked(now time.Time) {
	q, head := s.deliveryExpiry, s.deliveryHead
	for n := 0; head < len(q) && n < deliveryExpiryBudget && !q[head].expireAt.After(now); n++ {
		e := q[head]
		if d, ok := s.deliveries[e.key]; ok && d.expireAt.Equal(e.expireAt) {
			delete(s.deliveries, e.key)
		}
		head++
	}
	switch {
	case head == s.deliveryHead:
	case head == len(q):
		s.deliveryExpiry, s.deliveryHead = q[:0], 0
	case head > len(q)/2:
		s.deliveryExpiry, s.deliveryHead = append(q[:0], q[head:]...), 0
	default:
		s.deliveryHead = head
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

// releaseDelivery nacks a delivered message, and every other record of
// its batch, so they are redeliverable now. A stale handle (already
// acked or released) is not an error.
func (s *RPCServer) releaseDelivery(d delivery) {
	s.releaseHandle(d.topic, d.handle)
	for _, rh := range d.rest {
		if h, err := consumer.DecodeHandle(rh); err == nil {
			s.releaseHandle(d.topic, h)
		}
	}
}

// releaseHandle nacks one delivered record; see releaseDelivery.
func (s *RPCServer) releaseHandle(topicName string, h consumer.Handle) {
	if err := s.broker.Nack(rpcRequestContext(), topicName, h); err != nil && !errors.Is(err, consumer.ErrHandleStale) && s.logger != nil {
		s.logger.Warn("release consume delivery after client cancel", "topic", topicName, "err", err)
	}
}

// acquireMessagingSlot is acquireSlot on the messaging concurrency
// bound. A true result must be paired with releaseMessagingSlot.
func (s *RPCServer) acquireMessagingSlot(ctx context.Context) bool {
	return acquireSlot(ctx, s.messagingSem)
}

// acquireSlot takes a slot of sem, or reports false if ctx ends first:
// the requester gave up (its budget ran out, or its client left), and
// the caller answers without touching the broker. The uncontended case
// is one non-blocking send, and never asks the request context for its
// Done channel, which the transport makes only on demand. A nil sem
// always succeeds; a true result on a non-nil sem must be paired with
// a receive from it.
func acquireSlot(ctx context.Context, sem chan struct{}) bool {
	if sem == nil {
		return true
	}
	select {
	case sem <- struct{}{}:
		return true
	default:
	}
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *RPCServer) releaseMessagingSlot() {
	if sem := s.messagingSem; sem != nil {
		<-sem
	}
}

// withCommitSlot runs handle under the produce-commit concurrency bound,
// or answers 503 unapplied if the request is cancelled while it waits
// for a slot: the requester gave up (its budget ran out, or its client
// left) and will never read the reply. The broker would refuse it
// anyway, since a commit refuses a cancelled context at entry; refusing
// it here lets go of the frame's records when the cancel arrives rather
// than when a slot frees, which on an owner whose disk has stalled can
// be many seconds later. A commit that got its slot is never abandoned
// by the gate: from then on, what happens to a batch the requester
// stopped waiting for is the produce path's decision. The uncontended
// case is one non-blocking send and never asks ctx for its Done channel
// (see acquireSlot), so withSlot is not used here.
func (s *RPCServer) withCommitSlot(ctx context.Context, handle func() nodewire.Response) nodewire.Response {
	sem := s.commitSem
	if !acquireSlot(ctx, sem) {
		return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a commit slot")
	}
	if sem != nil {
		defer func() { <-sem }()
	}
	return handle()
}

// handleAckFamily runs an ack, extend or nack under the messaging
// bound. One whose requester gave up while it waited for a slot is
// answered 503 unapplied. Skipping it is safe: acks are idempotent by
// nonce, the requester already answered its client with an error, and a
// retry is applied on its own; applying the stale one as well would
// only make the retry look stale.
func (s *RPCServer) handleAckFamily(ctx context.Context, op nodewire.Operation, payload []byte) nodewire.Response {
	if !s.acquireMessagingSlot(ctx) {
		return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot")
	}
	defer s.releaseMessagingSlot()
	switch op {
	case nodewire.OpAck:
		return s.handleAck(ctx, payload)
	case nodewire.OpExtendAck:
		return s.handleExtendAck(ctx, payload)
	default:
		return s.handleNack(ctx, payload)
	}
}

// withSlot runs handle under sem, or answers 503 if the request is
// cancelled (client gone, stream closed) before a slot frees up. A nil
// sem runs handle directly.
func (s *RPCServer) withSlot(ctx context.Context, sem chan struct{}, handle func() nodewire.Response) nodewire.Response {
	res, held := s.withHeldSlot(ctx, sem, handle)
	if held != nil {
		<-held
	}
	return res
}

// withHeldSlot is withSlot for a handler whose reply must keep the slot
// until it has been written: the slot is returned still held, and the
// caller frees it (a receive from held) once the reply is out. held is
// nil when no slot was taken.
func (s *RPCServer) withHeldSlot(ctx context.Context, sem chan struct{}, handle func() nodewire.Response) (res nodewire.Response, held chan struct{}) {
	if sem == nil {
		return handle(), nil
	}
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot"), nil
	}
	return handle(), sem
}

func (s *RPCServer) dispatch(ctx context.Context, key requestKey, payload []byte) nodewire.Response {
	res, held := s.serve(ctx, key, payload)
	if held != nil {
		<-held
	}
	return res
}

// serve runs one request. held, when not nil, is a semaphore slot the
// reply still holds; the caller frees it once the reply is written.
func (s *RPCServer) serve(ctx context.Context, key requestKey, payload []byte) (res nodewire.Response, held chan struct{}) {
	op, err := nodewire.OperationOf(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid rpc request"), nil
	}
	switch op {
	case nodewire.OpConsume:
		// Gated inside the handler: only non-blocking scans take a slot.
		reserveConsumeStack()
		return s.handleConsume(ctx, key, payload), nil
	case nodewire.OpAck, nodewire.OpExtendAck, nodewire.OpNack:
		return s.handleAckFamily(ctx, op, payload), nil
	case nodewire.OpAckBatch:
		// Here rather than in serveOther for the same reason as the ack
		// family: a batch of two is common under load, and serveOther's
		// frame would cost it a stack growth.
		return s.handleAckBatch(ctx, payload), nil
	default:
		return s.serveOther(ctx, op, payload)
	}
}

// consumeStackReserve is the frame reserveConsumeStack takes: enough
// that the one growth it causes on a minimum-size stack goes straight
// to the 8 KiB a broker consume ends up using.
const consumeStackReserve = 3 << 10

// reserveConsumeStack grows the request goroutine's stack before a
// consume runs, while the only frames on it are serve's and its
// caller's. Every request runs on a new goroutine with a small stack,
// and a broker consume outgrows it twice on the way down
// (Engine.Consume's own frame is nearly 1 KiB). Each growth copies
// every frame above it, and left to itself the second one struck ten
// frames deep inside the scan. Growing once here, with two frames to
// copy, took an empty probe against a real engine from 1.8 to 1.1 us.
//
//go:noinline
func reserveConsumeStack() {
	var reserve [consumeStackReserve]byte
	touchStack(&reserve)
}

// touchStack keeps reserveConsumeStack's array, and so its frame, from
// being optimized away.
//
//go:noinline
func touchStack(*[consumeStackReserve]byte) {}

// serveOther runs every op but the per-message consume and ack family,
// which serve keeps to a small frame of their own (see writeReply).
func (s *RPCServer) serveOther(ctx context.Context, op nodewire.Operation, payload []byte) (res nodewire.Response, held chan struct{}) {
	switch op {
	case nodewire.OpProduce:
		res = s.handleProduce(ctx, payload)
	case nodewire.OpCommitProduce:
		res = s.withCommitSlot(ctx, func() nodewire.Response { return s.handleCommitProduce(ctx, payload) })
	case nodewire.OpCommitProduceBatch:
		res = s.withCommitSlot(ctx, func() nodewire.Response { return s.handleCommitProduceBatch(ctx, payload) })
	case nodewire.OpListPartitionSegments:
		return s.withHeldSlot(ctx, s.transferSem, func() nodewire.Response { return s.handleListPartitionSegments(payload) })
	case nodewire.OpFetchSegmentChunk:
		return s.withHeldSlot(ctx, s.transferSem, func() nodewire.Response { return s.handleFetchSegmentChunk(payload) })
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
	return res, nil
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
	case nodewire.OpTokenRegister:
		return s.handleTokenRegister, true
	case nodewire.OpTokenNotify:
		return s.handleTokenNotify, true
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
	case nodewire.OpAppliedIndex:
		return s.handleAppliedIndex, true
	default:
		return nil, false
	}
}

// brokerError maps a broker failure onto the RPC status vocabulary shared
// with the HTTP layer. Unrecognized errors are logged and reported as opaque
// 500s so internal details never cross the wire.
func (s *RPCServer) brokerError(op string, err error) nodewire.Response {
	return errorResponse(s.brokerErrorStatus(op, err))
}

// brokerErrorStatus is brokerError's mapping as a status and message, for
// a reply that carries several outcomes in one body (see handleAckBatch).
func (s *RPCServer) brokerErrorStatus(op string, err error) (int, string) {
	switch {
	case errors.Is(err, errs.ErrForbidden):
		return http.StatusForbidden, err.Error()
	case errors.Is(err, errs.ErrTopicNotFound):
		return http.StatusNotFound, "topic not found"
	case errors.Is(err, errs.ErrTopicAlreadyExists):
		if err != errs.ErrTopicAlreadyExists {
			return http.StatusConflict, err.Error()
		}
		return http.StatusConflict, "topic already exists"
	case errors.Is(err, errs.ErrHandleMalformed):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, errs.ErrHandleStale):
		return http.StatusGone, err.Error()
	case errors.Is(err, errs.ErrAckedAheadFull):
		return http.StatusServiceUnavailable, err.Error()
	case errors.Is(err, errs.ErrUnavailable):
		// Retryable, as the HTTP layer maps it: no leader, a replica
		// still catching up, or a create the leader refused because
		// every live member is being decommissioned. The message says
		// which, so a forwarded request's caller sees it.
		if s.logger != nil {
			s.logger.Warn(op+" unavailable", "err", err)
		}
		return http.StatusServiceUnavailable, err.Error()
	case errors.Is(err, errs.ErrInvalidArgument),
		errors.Is(err, errs.ErrPartitionRequired):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, errs.ErrNotPartitionOwner):
		return http.StatusMisdirectedRequest, err.Error()
	case errors.Is(err, errs.ErrFanoutRoleConflict),
		errors.Is(err, errs.ErrFanoutChildLimit),
		errors.Is(err, errs.ErrFanoutSchemaMismatch),
		errors.Is(err, errs.ErrFanoutSchemaManaged),
		errors.Is(err, errs.ErrDelayedChildProduce),
		errors.Is(err, errs.ErrFanoutDelayTooLong), // 409, as the HTTP layer maps it
		errors.Is(err, errs.ErrSchemaVersionConflict),
		errors.Is(err, errs.ErrSchemaHistoryFull),
		errors.Is(err, errs.ErrAlreadyExists):
		return http.StatusConflict, err.Error()
	case errors.Is(err, errs.ErrNotFound):
		return http.StatusNotFound, err.Error()
	case errors.Is(err, brokermsg.ErrTopicIncarnationMismatch):
		// Records accepted for another incarnation of the topic than the
		// one this node holds under the name: a delete and recreate raced
		// them, or this replica or the sender's lags. Expected and
		// retriable, so it gets its own status, which the produce
		// dispatcher recognizes (see commitRemote), and no error line.
		if s.logger != nil {
			s.logger.Info(op+" refused: records accepted for another topic incarnation", "err", err)
		}
		return http.StatusPreconditionFailed, err.Error()
	default:
		if s.logger != nil {
			s.logger.Error(op, "err", err)
		}
		return http.StatusInternalServerError, op + " failed"
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
