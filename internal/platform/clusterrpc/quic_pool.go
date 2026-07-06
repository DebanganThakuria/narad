package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

const (
	quicDialTimeout = time.Second

	// Streams per lane. Bulk lanes are wide so concurrent produces,
	// consumes, and acks spread across streams; control traffic is
	// light and gets a few.
	quicProduceLanes = 16
	quicConsumeLanes = 16
	quicAckLanes     = 16
	quicControlLanes = 4

	quicMaxIncomingStreams             = 1024
	quicInitialStreamReceiveWindow     = 256 << 10
	quicMaxStreamReceiveWindow         = 2 << 20
	quicInitialConnectionReceiveWindow = 1 << 20
	quicMaxConnectionReceiveWindow     = 8 << 20
)

// quicClientPool keeps one QUIC connection per peer address and a set
// of multiplexed stream clients per (address, lane, shard). Requests
// round-robin across a lane's shards so no single stream serializes
// all traffic.
type quicClientPool struct {
	timeout time.Duration
	secret  string

	nextShard atomic.Uint64

	mu      sync.Mutex
	conns   map[string]*quic.Conn
	streams map[string]*streamClient
}

func newQUICClientPool(timeout time.Duration, secret string) *quicClientPool {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	return &quicClientPool{
		timeout: timeout,
		secret:  secret,
		conns:   make(map[string]*quic.Conn),
		streams: make(map[string]*streamClient),
	}
}

func (p *quicClientPool) request(ctx context.Context, addr, lane string, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	client, err := p.getStream(ctx, addr, lane)
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	frame, err := client.requestFrame(ctx, frameType, payload)
	if err != nil && client.isClosed() {
		p.closeStream(addr, lane, client, err)
	}
	return frame, err
}

func (p *quicClientPool) getStream(ctx context.Context, addr, lane string) (*streamClient, error) {
	opCtx, cancel := p.operationContext(ctx)
	defer cancel()

	lane = normalizeQUICLane(lane)
	shard := int((p.nextShard.Add(1) - 1) % uint64(quicLaneWidth(lane)))
	key := quicStreamPoolKey(addr, lane, shard)

	p.mu.Lock()
	if client := p.streams[key]; client != nil && !client.isClosed() {
		p.mu.Unlock()
		return client, nil
	}
	p.mu.Unlock()

	conn, err := p.getConn(opCtx, addr)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(opCtx)
	if err != nil {
		p.closeConn(addr, conn, err)
		return nil, err
	}
	// Prove secret knowledge as the stream's first frame; the server
	// rejects the stream otherwise. Safe to write directly: the stream
	// is not yet shared (readLoop unstarted, not in the pool map).
	if p.secret != "" {
		if err := clusterwire.WriteStreamFrame(stream, authFrame(p.secret)); err != nil {
			_ = stream.Close()
			return nil, err
		}
	}

	client := &streamClient{
		conn:    stream,
		reader:  bufio.NewReader(stream),
		timeout: p.timeout,
		pending: make(map[uint64]chan streamResult),
	}
	go client.readLoop()

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.streams[key]; existing != nil && !existing.isClosed() {
		client.closeWithError(errors.New("superseded by existing quic stream client"))
		return existing, nil
	}
	p.streams[key] = client
	return client, nil
}

func (p *quicClientPool) getConn(ctx context.Context, addr string) (*quic.Conn, error) {
	addr = quicAddr(addr)

	p.mu.Lock()
	if conn := p.conns[addr]; conn != nil {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	opCtx, cancel := p.dialContext(ctx)
	defer cancel()

	conn, err := quic.DialAddr(opCtx, addr, quicClientTLSConfig(), quicConfig())
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.conns[addr]; existing != nil {
		_ = conn.CloseWithError(0, "superseded by existing quic connection")
		return existing, nil
	}
	p.conns[addr] = conn
	return conn, nil
}

// closeConn removes the connection and every stream client pooled on it,
// then fails those clients' in-flight requests with cause.
func (p *quicClientPool) closeConn(addr string, conn *quic.Conn, cause error) {
	addr = quicAddr(addr)
	streamPrefix := addr + "\x00"
	var streams []*streamClient
	p.mu.Lock()
	if p.conns[addr] == conn {
		delete(p.conns, addr)
		for key, client := range p.streams {
			if strings.HasPrefix(key, streamPrefix) {
				delete(p.streams, key)
				streams = append(streams, client)
			}
		}
	}
	p.mu.Unlock()
	for _, stream := range streams {
		stream.closeWithError(cause)
	}
	_ = conn.CloseWithError(0, cause.Error())
}

func (p *quicClientPool) closeStream(addr, lane string, client *streamClient, cause error) {
	addr = quicAddr(addr)
	lane = normalizeQUICLane(lane)

	var removed bool
	p.mu.Lock()
	for key, existing := range p.streams {
		if existing == client && strings.HasPrefix(key, addr+"\x00"+lane+"\x00") {
			delete(p.streams, key)
			removed = true
			break
		}
	}
	p.mu.Unlock()
	if removed {
		client.closeWithError(cause)
	}
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

func quicStreamPoolKey(addr, lane string, shard int) string {
	if shard < 0 {
		shard = 0
	}
	return quicAddr(addr) + "\x00" + normalizeQUICLane(lane) + "\x00" + strconv.Itoa(shard)
}

func normalizeQUICLane(lane string) string {
	lane = strings.TrimSpace(strings.ToLower(lane))
	switch lane {
	case "produce", "consume", "ack", "control":
		return lane
	default:
		return "control"
	}
}

func quicLaneWidth(lane string) int {
	switch normalizeQUICLane(lane) {
	case "produce":
		return quicProduceLanes
	case "consume":
		return quicConsumeLanes
	case "ack":
		return quicAckLanes
	default:
		return quicControlLanes
	}
}

// quicAddr normalizes an address that may have been copied from an HTTP
// peer URL down to the bare host:port QUIC dials.
func quicAddr(addr string) string {
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
