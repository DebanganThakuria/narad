package clusterrpc

import (
	"context"
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
	pingTimeout time.Duration
	secret      string

	// dial establishes a connection; the default dials QUIC through the
	// pool's shared transport. Tests substitute in-memory fakes.
	dial func(ctx context.Context, addr string) (poolConn, error)
	now  func() time.Time

	nextShard atomic.Uint64

	mu       sync.Mutex
	conns    map[string]poolConn
	streams  map[streamKey]*pooledStream
	dialing  map[string]*dialCall
	dialFail map[string]dialFailure
	closed   bool

	// transport is the client-side QUIC transport: one UDP socket for
	// every outgoing connection, created on first dial. It carries the
	// stateless reset key so peers can recognise this node's resets.
	transportOnce sync.Once
	transport     *quic.Transport
	transportConn *net.UDPConn
	transportErr  error
}

func newQUICClientPool(timeout time.Duration, secret string) *quicClientPool {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	p := &quicClientPool{
		timeout:     timeout,
		pingTimeout: quicStreamPingTimeout,
		secret:      secret,
		now:         time.Now,
		conns:       make(map[string]poolConn),
		streams:     make(map[streamKey]*pooledStream),
		dialing:     make(map[string]*dialCall),
		dialFail:    make(map[string]dialFailure),
	}
	p.dial = p.dialQUIC
	return p
}

func (p *quicClientPool) request(ctx context.Context, addr string, lane Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
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
		ps, err = p.openStream(ctx, key)
		if err != nil {
			return clusterwire.StreamFrame{}, err
		}
	}

	frame, err := ps.client.requestFrame(ctx, frameType, payload)
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

// openStream opens (or, racing another caller, adopts) the pooled stream
// for key. A stream open that fails on a pooled connection evicts that
// connection and retries once on a fresh dial, so a connection that died
// silently costs one extra round trip rather than a failed request.
func (p *quicClientPool) openStream(ctx context.Context, key streamKey) (*pooledStream, error) {
	opCtx, cancel := p.operationContext(ctx)
	defer cancel()

	for attempt := 0; ; attempt++ {
		conn, err := p.getConn(opCtx, key.addr)
		if err != nil {
			return nil, err
		}
		stream, err := conn.openStream(opCtx)
		if err != nil {
			p.closeConn(key.addr, conn, err)
			if attempt == 0 && opCtx.Err() == nil {
				continue
			}
			return nil, err
		}
		// Prove secret knowledge as the stream's first frame; the server
		// rejects the stream otherwise. Safe to write directly: the stream
		// is not yet shared (readLoop unstarted, not in the pool map).
		if p.secret != "" {
			if err := clusterwire.WriteStreamFrame(stream, authFrame(p.secret)); err != nil {
				abortStream(stream)
				return nil, err
			}
		}

		client := newStreamClient(stream, p.timeout)
		go client.readLoop()

		p.mu.Lock()
		defer p.mu.Unlock()
		if existing := p.streams[key]; existing != nil && !existing.client.isClosed() {
			client.closeWithError(errors.New("superseded by existing quic stream client"))
			return existing, nil
		}
		ps := &pooledStream{client: client, conn: conn}
		p.streams[key] = ps
		return ps, nil
	}
}

// getConn returns the pooled connection for addr, dialing when there is
// none. Concurrent callers share one dial (singleflight); after a failed
// dial, callers fail fast with the cached error until the backoff
// window elapses.
func (p *quicClientPool) getConn(ctx context.Context, addr string) (poolConn, error) {
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
	if call := p.dialing[addr]; call != nil {
		p.mu.Unlock()
		select {
		case <-call.done:
			return call.conn, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &dialCall{done: make(chan struct{})}
	p.dialing[addr] = call
	p.mu.Unlock()

	conn, err := p.dialConn(ctx, addr)

	p.mu.Lock()
	delete(p.dialing, addr)
	var orphan poolConn
	switch {
	case err != nil:
		p.recordDialFailure(addr, err)
	case p.closed:
		// The pool closed while we were dialing; the connection has no
		// home. Fail the waiters and drop it once the lock is released.
		orphan, conn, err = conn, nil, errPoolClosed
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
	return conn, err
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

func (p *quicClientPool) dialConn(ctx context.Context, addr string) (poolConn, error) {
	dialCtx, cancel := p.dialContext(ctx)
	defer cancel()
	return p.dial(dialCtx, addr)
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
	conn, err := tr.Dial(ctx, udpAddr, quicClientTLSConfig(), quicConfig())
	if err != nil {
		return nil, err
	}
	return quicPoolConn{conn: conn}, nil
}

// clientTransport lazily creates the pool's QUIC transport. It binds one
// wildcard UDP socket (the same choice quic.DialAddr makes) and carries
// the stateless reset key derived from the cluster secret.
func (p *quicClientPool) clientTransport() (*quic.Transport, error) {
	p.transportOnce.Do(func() {
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err != nil {
			p.transportErr = err
			return
		}
		p.transportConn = udpConn
		p.transport = &quic.Transport{
			Conn:              udpConn,
			StatelessResetKey: statelessResetKey(p.secret),
		}
	})
	return p.transport, p.transportErr
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
	conns := p.conns
	p.conns = make(map[string]poolConn)
	p.mu.Unlock()

	for addr, conn := range conns {
		p.closeConn(addr, conn, errPoolClosed)
	}
	var err error
	if p.transport != nil {
		err = p.transport.Close()
		if closeErr := p.transportConn.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// operationContext bounds callers that pass no deadline by the pool
// timeout so a stalled open/dial cannot block forever.
func (p *quicClientPool) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || p.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.timeout)
}

func (p *quicClientPool) dialContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= quicDialTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, quicDialTimeout)
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
