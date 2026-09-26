package clusterrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

const (
	quicDialTimeout = time.Second

	// Streams per lane; see Lane.
	quicProduceLanes = 16
	quicConsumeLanes = 16
	quicAckLanes     = 16
	quicControlLanes = 4

	quicMaxIncomingStreams = 1024

	// Flow-control windows. The stream windows are sized so a single
	// commit batch or segment chunk (hundreds of KiB to a few MiB) does
	// not stall on window updates mid-frame; the connection windows
	// cover the 16 bulk streams per lane running concurrently.
	quicInitialStreamReceiveWindow     = 1 << 20
	quicMaxStreamReceiveWindow         = 8 << 20
	quicInitialConnectionReceiveWindow = 4 << 20
	quicMaxConnectionReceiveWindow     = 32 << 20

	// Dial failure backoff. After a failed dial, requests to that address
	// fail fast with the cached error until the backoff elapses; each
	// consecutive failure doubles it up to the cap. A single dial attempt
	// is shared by every request that arrives while it is in flight.
	quicDialBackoffMin = 250 * time.Millisecond
	quicDialBackoffMax = 2 * time.Second

	// quicStreamPingTimeout bounds the liveness ping sent on a stream
	// whose reply wait hit the fallback client timeout. A live peer
	// answers a ping from its stream read loop, independent of any
	// slow handler; no answer means the connection is dead.
	quicStreamPingTimeout = time.Second
)

// poolConn is the connection surface the pool needs; *quic.Conn is
// adapted by quicPoolConn, tests supply in-memory fakes.
type poolConn interface {
	openStream(ctx context.Context) (streamConn, error)
	// tlsState is the negotiated TLS session the auth proofs are bound
	// to; ok is false for a transport without one (in-memory fakes).
	tlsState() (cs tls.ConnectionState, ok bool)
	// done is closed once the connection is dead (peer close, idle
	// timeout, stateless reset), so the pool can evict it proactively.
	done() <-chan struct{}
	close(cause error)
}

type quicPoolConn struct {
	conn *quic.Conn
}

func (c quicPoolConn) openStream(ctx context.Context) (streamConn, error) {
	return c.conn.OpenStreamSync(ctx)
}

func (c quicPoolConn) tlsState() (tls.ConnectionState, bool) {
	return c.conn.ConnectionState().TLS, true
}

func (c quicPoolConn) done() <-chan struct{} {
	return c.conn.Context().Done()
}

func (c quicPoolConn) close(cause error) {
	_ = c.conn.CloseWithError(0, cause.Error())
}

// streamKey identifies one pooled stream client: a shard of a lane on a
// peer address. A struct key avoids building a string per request.
type streamKey struct {
	addr  string
	lane  Lane
	shard int
}

// pooledStream pairs a stream client with the connection it was opened
// on, so a dead stream can take its whole connection down.
type pooledStream struct {
	client *streamClient
	conn   poolConn
}

// dialCall is an in-flight dial that concurrent requests wait on
// instead of dialing in parallel (singleflight).
type dialCall struct {
	done chan struct{}
	conn poolConn
	err  error
}

// openCall is an in-flight stream open that concurrent requests for the
// same stream key wait on instead of each opening their own (singleflight).
type openCall struct {
	done chan struct{}
	ps   *pooledStream
	err  error
}

// dialFailure is the cached outcome of the last failed dial to an
// address, honoured until `until`.
type dialFailure struct {
	err     error
	until   time.Time
	backoff time.Duration
}

// quicClientPool keeps one QUIC connection per peer address and a set
// of multiplexed stream clients per (address, lane, shard). Requests
// round-robin across a lane's shards so no single stream serializes
// all traffic.
type quicClientPool struct {
	timeout     time.Duration
	dialTimeout time.Duration
	pingTimeout time.Duration
	secret      string
	// allowLegacy lets this client fall back to the fixed-token
	// handshake with a peer that only speaks the legacy ALPN.
	allowLegacy bool

	// dial establishes a connection; the default dials QUIC through the
	// pool's shared transport. Tests substitute in-memory fakes.
	dial func(ctx context.Context, addr string) (poolConn, error)
	now  func() time.Time

	// ctx is the pool's own context, cancelled by close. Shared work that
	// many callers wait on (dials, stream opens) runs under it rather than
	// under any one caller's context, so one caller giving up cannot fail
	// it for the rest.
	ctx    context.Context
	cancel context.CancelFunc

	nextShard atomic.Uint64

	mu       sync.Mutex
	conns    map[string]poolConn
	streams  map[streamKey]*pooledStream
	opening  map[streamKey]*openCall
	dialing  map[string]*dialCall
	dialFail map[string]dialFailure
	closed   bool

	// transport is the client-side QUIC transport: one UDP socket for
	// every outgoing connection, created on first dial under mu. It
	// carries the stateless reset key so peers can recognise this node's
	// resets.
	transport     *quic.Transport
	transportConn *net.UDPConn
}

func newQUICClientPool(timeout time.Duration, secret string, allowLegacy bool) *quicClientPool {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &quicClientPool{
		timeout:     timeout,
		dialTimeout: quicDialTimeout,
		pingTimeout: quicStreamPingTimeout,
		secret:      secret,
		allowLegacy: allowLegacy,
		now:         time.Now,
		conns:       make(map[string]poolConn),
		streams:     make(map[streamKey]*pooledStream),
		opening:     make(map[streamKey]*openCall),
		dialing:     make(map[string]*dialCall),
		dialFail:    make(map[string]dialFailure),
		ctx:         ctx,
		cancel:      cancel,
	}
	p.dial = p.dialQUIC
	return p
}

func (p *quicClientPool) request(ctx context.Context, addr string, lane Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return p.requestWithin(ctx, addr, lane, 0, frameType, payload)
}

// requestWithin is request with a budget of the caller's own: a positive
// timeout bounds the whole call, the wait for a dial or stream open
// included, alongside ctx (see QUICFrameClient.RequestOnLaneTimeout).
func (p *quicClientPool) requestWithin(ctx context.Context, addr string, lane Lane, timeout time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	lane = lane.normalize()
	key := streamKey{
		addr:  quicAddr(addr),
		lane:  lane,
		shard: int((p.nextShard.Add(1) - 1) % uint64(lane.width())),
	}

	// Fast path: a live pooled stream. No context derivation, no
	// allocation; just a map lookup under the mutex.
	p.mu.Lock()
	ps := p.streams[key]
	p.mu.Unlock()
	if ps == nil || ps.client.isClosed() {
		var err error
		ps, err = p.stream(ctx, deadline, key)
		if err != nil {
			return clusterwire.StreamFrame{}, err
		}
	}

	frame, _, err := ps.client.roundTrip(ctx, deadline, frameType, payload)
	if err != nil {
		switch {
		case ps.client.isClosed():
			p.closeStream(key, ps, err)
		case errors.Is(err, errFallbackReplyTimeout):
			p.probeAfterTimeout(key, ps)
		}
	}
	return frame, err
}

// probeAfterTimeout runs when a reply wait hit the fallback client
// timeout (the caller supplied no deadline). A slow handler and a dead
// connection look identical from the reply wait, so ping the stream: the
// server answers pings from its read loop without touching any handler.
// No pong within pingTimeout means the connection is gone (a peer that
// restarted without a stateless reset reaching us, a black-holed path);
// close it so every pooled stream on it fails now and the next request
// re-dials instead of waiting out MaxIdleTimeout.
func (p *quicClientPool) probeAfterTimeout(key streamKey, ps *pooledStream) {
	pingCtx, cancel := context.WithTimeout(context.Background(), p.pingTimeout)
	defer cancel()
	if _, err := ps.client.requestFrame(pingCtx, clusterwire.StreamFramePing, nil); err != nil {
		p.closeConn(key.addr, ps.conn, fmt.Errorf("peer %s unresponsive after reply timeout: %w", key.addr, err))
	}
}

// stream returns the pooled stream for key, opening it when there is
// none. Concurrent callers for one key share a single open (a burst on a
// cold pool opens each shard once, not once per request). The open runs
// on its own goroutine under the pool's context and timeout, never under
// a caller's: every caller waits for it or for its own context, so a
// caller that is cancelled or runs out of time fails alone. Were the open
// run under the caller's context, its cancellation would look like a dead
// connection and take every stream on it down (see openStream). A caller
// whose context has already ended does not start an open at all. A
// non-zero deadline is the caller's own budget (see requestWithin): the
// caller stops waiting there with errRequestTimeout.
func (p *quicClientPool) stream(ctx context.Context, deadline time.Time, key streamKey) (*pooledStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	if ps := p.streams[key]; ps != nil && !ps.client.isClosed() {
		p.mu.Unlock()
		return ps, nil
	}
	call := p.opening[key]
	if call == nil {
		call = &openCall{done: make(chan struct{})}
		p.opening[key] = call
		go p.openShared(key, call)
	}
	p.mu.Unlock()
	var timeoutCh <-chan time.Time
	if !deadline.IsZero() {
		timer := getTimer(time.Until(deadline))
		defer putTimer(timer)
		timeoutCh = timer.C
	}
	select {
	case <-call.done:
		return call.ps, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timeoutCh:
		return nil, errRequestTimeout
	}
}

// openShared runs the one open of key that stream's callers wait on,
// pools the result and publishes it to them. A stream opened for callers
// that have all gone is pooled all the same, for the next request.
func (p *quicClientPool) openShared(key streamKey, call *openCall) {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	client, conn, err := p.openStream(ctx, key)
	cancel()

	p.mu.Lock()
	delete(p.opening, key)
	var orphan *streamClient
	switch {
	case p.closed:
		// The pool closed while we were opening (which is also why an
		// open fails at this point): the stream has no home.
		orphan, err = client, errPoolClosed
	case err != nil:
	default:
		call.ps = &pooledStream{client: client, conn: conn}
		p.streams[key] = call.ps
	}
	call.err = err
	p.mu.Unlock()
	close(call.done)
	if orphan != nil {
		orphan.closeWithError(errPoolClosed)
	}
}

// openStream opens and authenticates a new stream for key. ctx is the
// pool-owned open context, so an error here is the connection's (or the
// pool's, once it closes), never a caller's. A stream open that fails on
// a pooled connection evicts that connection and retries once on a fresh
// dial, so a connection that died silently costs one extra round trip
// rather than a failed request.
func (p *quicClientPool) openStream(ctx context.Context, key streamKey) (*streamClient, poolConn, error) {
	for attempt := 0; ; attempt++ {
		conn, err := p.getConn(ctx, key.addr)
		if err != nil {
			return nil, nil, err
		}
		stream, err := conn.openStream(ctx)
		if err != nil {
			p.closeConn(key.addr, conn, err)
			if attempt == 0 && ctx.Err() == nil {
				continue
			}
			return nil, nil, err
		}
		// Run the auth handshake as the stream's first exchange. Safe to
		// do synchronously: the stream is not yet shared (readLoop
		// unstarted, not in the pool map). A server that cannot prove
		// the secret is an impostor, so the whole connection is dropped,
		// not just the stream.
		if p.secret != "" {
			if err := p.authenticateStream(ctx, conn, stream); err != nil {
				abortStream(stream)
				p.closeConn(key.addr, conn, err)
				return nil, nil, err
			}
		}

		client := newStreamClient(stream, p.timeout)
		go client.readLoop()
		return client, conn, nil
	}
}

// authenticateStream runs the client side of the stream auth handshake:
// send the session-bound client proof, then read and verify the server's
// proof before the stream carries any request. The server's reply is
// read under the same 64-byte cap the server applies to ours. On the
// legacy ALPN (only reachable with allowLegacy) it sends the fixed token
// and expects no reply, as old servers send none.
func (p *quicClientPool) authenticateStream(ctx context.Context, conn poolConn, stream streamConn) error {
	cs, ok := conn.tlsState()
	if !ok {
		return errNoTLSSession
	}
	binding, err := sessionBindingFrom(cs, p.allowLegacy)
	if err != nil {
		return err
	}
	if binding.legacy {
		return clusterwire.WriteStreamFrame(stream, authFrame(legacyAuthToken(p.secret)))
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(p.timeout)
	}
	_ = stream.SetDeadline(deadline)
	defer func() { _ = stream.SetDeadline(time.Time{}) }()
	if err := clusterwire.WriteStreamFrame(stream, authFrame(clientProof(p.secret, binding.key))); err != nil {
		return err
	}
	reply, err := clusterwire.ReadStreamFrame(stream, maxAuthFramePayloadBytes)
	if err != nil {
		return fmt.Errorf("cluster rpc: read server auth proof: %w", err)
	}
	if !verifyAuthToken(serverProof(p.secret, binding.key), reply) {
		return errors.New("cluster rpc: peer failed to prove the cluster secret; refusing to use it")
	}
	return nil
}

// getConn returns the pooled connection for addr, dialing when there is
// none. Concurrent callers share one dial (singleflight). The dial runs
// on its own goroutine under the pool's context and dial timeout, never
// under a caller's: every caller, the first included, waits for it or
// for its own context, so a caller that gives up (or arrives already
// cancelled) fails alone and cannot make the others fail or arm the
// backoff with its cancellation. After a failed dial, callers fail fast
// with the cached error until the backoff window elapses.
func (p *quicClientPool) getConn(ctx context.Context, addr string) (poolConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}
	if conn := p.conns[addr]; conn != nil {
		p.mu.Unlock()
		return conn, nil
	}
	if failure, ok := p.dialFail[addr]; ok && p.now().Before(failure.until) {
		p.mu.Unlock()
		return nil, fmt.Errorf("quic dial %s backing off (%s): %w", addr, failure.backoff, failure.err)
	}
	call := p.dialing[addr]
	if call == nil {
		call = &dialCall{done: make(chan struct{})}
		p.dialing[addr] = call
		go p.dialShared(addr, call)
	}
	p.mu.Unlock()
	select {
	case <-call.done:
		return call.conn, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialShared runs the one dial to addr that getConn's callers wait on
// and publishes its outcome to them. No caller's context reaches it, so
// any failure, a timeout included, is the peer's and arms the backoff.
func (p *quicClientPool) dialShared(addr string, call *dialCall) {
	dialCtx, cancel := context.WithTimeout(p.ctx, p.dialTimeout)
	conn, err := p.dial(dialCtx, addr)
	cancel()

	p.mu.Lock()
	delete(p.dialing, addr)
	var orphan poolConn
	switch {
	case p.closed:
		// The pool closed while we were dialing; the connection has no
		// home. Fail the waiters and drop it once the lock is released.
		orphan, conn, err = conn, nil, errPoolClosed
	case err != nil:
		p.recordDialFailure(addr, err)
	default:
		delete(p.dialFail, addr)
		p.conns[addr] = conn
		go p.watchConn(addr, conn)
	}
	call.conn, call.err = conn, err
	p.mu.Unlock()
	close(call.done)
	if orphan != nil {
		orphan.close(errPoolClosed)
	}
}

var errPoolClosed = errors.New("quic client pool closed")

// recordDialFailure caches err for addr and doubles the backoff on each
// consecutive failure, capped at quicDialBackoffMax. Caller holds p.mu.
func (p *quicClientPool) recordDialFailure(addr string, err error) {
	failure := p.dialFail[addr]
	if failure.backoff == 0 {
		failure.backoff = quicDialBackoffMin
	} else {
		failure.backoff = min(failure.backoff*2, quicDialBackoffMax)
	}
	failure.err = err
	failure.until = p.now().Add(failure.backoff)
	p.dialFail[addr] = failure
}

// watchConn evicts conn from the pool as soon as it dies, so a stateless
// reset or idle timeout is reflected immediately instead of on the next
// failed request.
func (p *quicClientPool) watchConn(addr string, conn poolConn) {
	<-conn.done()
	p.closeConn(addr, conn, errors.New("quic connection closed"))
}

// dialQUIC is the default dialer: one connection through the pool's
// shared transport.
func (p *quicClientPool) dialQUIC(ctx context.Context, addr string) (poolConn, error) {
	tr, err := p.clientTransport()
	if err != nil {
		return nil, err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := tr.Dial(ctx, udpAddr, quicClientTLSConfig(p.allowLegacy), quicConfig())
	if err != nil {
		return nil, err
	}
	return quicPoolConn{conn: conn}, nil
}

// clientTransport lazily creates the pool's QUIC transport. It binds one
// wildcard UDP socket (the same choice quic.DialAddr makes) and carries
// the stateless reset key derived from the cluster secret. Creation is
// under mu so a dial still running when the pool closes cannot create a
// transport that close has already passed over.
func (p *quicClientPool) clientTransport() (*quic.Transport, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errPoolClosed
	}
	if p.transport == nil {
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err != nil {
			return nil, err
		}
		p.transportConn = udpConn
		p.transport = &quic.Transport{
			Conn:              udpConn,
			StatelessResetKey: statelessResetKey(p.secret),
		}
	}
	return p.transport, nil
}

// closeConn removes the connection and every stream client pooled on it,
// then fails those clients' in-flight requests with cause.
func (p *quicClientPool) closeConn(addr string, conn poolConn, cause error) {
	var streams []*pooledStream
	p.mu.Lock()
	if p.conns[addr] == conn {
		delete(p.conns, addr)
	}
	for key, ps := range p.streams {
		if ps.conn == conn {
			delete(p.streams, key)
			streams = append(streams, ps)
		}
	}
	p.mu.Unlock()
	for _, ps := range streams {
		ps.client.closeWithError(cause)
	}
	conn.close(cause)
}

func (p *quicClientPool) closeStream(key streamKey, ps *pooledStream, cause error) {
	p.mu.Lock()
	removed := p.streams[key] == ps
	if removed {
		delete(p.streams, key)
	}
	p.mu.Unlock()
	if removed {
		ps.client.closeWithError(cause)
	}
}

// close tears down every connection and the transport. Idempotent.
func (p *quicClientPool) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancel()
	conns := p.conns
	p.conns = make(map[string]poolConn)
	tr, trConn := p.transport, p.transportConn
	p.mu.Unlock()

	for addr, conn := range conns {
		p.closeConn(addr, conn, errPoolClosed)
	}
	var err error
	if tr != nil {
		err = tr.Close()
		if closeErr := trConn.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// quicAddr normalizes an address that may have been copied from an HTTP
// peer URL down to the bare host:port QUIC dials. The common case (a
// bare host:port) returns the input unchanged without allocating.
func quicAddr(addr string) string {
	if !strings.HasPrefix(addr, "http") && !strings.HasSuffix(addr, "/") && strings.TrimSpace(addr) == addr {
		return addr
	}
	addr = strings.TrimSpace(strings.TrimRight(addr, "/"))
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	return addr
}

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:                 30 * time.Second,
		KeepAlivePeriod:                10 * time.Second,
		MaxIncomingStreams:             quicMaxIncomingStreams,
		MaxIncomingUniStreams:          -1,
		InitialStreamReceiveWindow:     quicInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         quicMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: quicInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     quicMaxConnectionReceiveWindow,
	}
}
