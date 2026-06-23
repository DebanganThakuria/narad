package replication

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

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type quicClientPool struct {
	timeout time.Duration
	metrics stageObserver

	nextLane atomic.Uint64

	mu      sync.Mutex
	conns   map[string]*quic.Conn
	streams map[string]*streamClient
}

type QUICFrameClient struct {
	pool *quicClientPool
}

const (
	quicDialTimeout = time.Second

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

func newQUICClientPool(timeout time.Duration, observers ...stageObserver) *quicClientPool {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &quicClientPool{
		timeout: timeout,
		metrics: observer,
		conns:   make(map[string]*quic.Conn),
		streams: make(map[string]*streamClient),
	}
}

func NewQUICFrameClient(timeout time.Duration, observers ...stageObserver) *QUICFrameClient {
	return &QUICFrameClient{pool: newQUICClientPool(timeout, observers...)}
}

func (c *QUICFrameClient) Request(ctx context.Context, addr string, frameType replicationwire.StreamFrameType, payload []byte) (replicationwire.StreamFrame, error) {
	if c == nil || c.pool == nil {
		return replicationwire.StreamFrame{}, errors.New("quic frame client is nil")
	}
	return c.pool.request(ctx, addr, quicLaneForFrame(frameType), frameType, payload)
}

func (c *QUICFrameClient) RequestOnLane(ctx context.Context, addr, lane string, frameType replicationwire.StreamFrameType, payload []byte) (replicationwire.StreamFrame, error) {
	if c == nil || c.pool == nil {
		return replicationwire.StreamFrame{}, errors.New("quic frame client is nil")
	}
	return c.pool.request(ctx, addr, lane, frameType, payload)
}

func (p *quicClientPool) request(ctx context.Context, addr, lane string, frameType replicationwire.StreamFrameType, payload []byte) (replicationwire.StreamFrame, error) {
	client, err := p.getStream(ctx, addr, lane)
	if err != nil {
		return replicationwire.StreamFrame{}, err
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
	shard := int((p.nextLane.Add(1) - 1) % uint64(quicLaneWidth(lane)))
	key := quicStreamPoolKey(addr, lane, shard)

	p.mu.Lock()
	if client := p.streams[key]; client != nil && !client.isClosed() {
		p.mu.Unlock()
		return client, nil
	}
	p.mu.Unlock()

	operation := lane
	stageStart := time.Now()
	conn, err := p.getConn(opCtx, addr)
	if err != nil {
		p.observe(operation, "open_stream", "error", time.Since(stageStart))
		return nil, err
	}
	stream, err := conn.OpenStreamSync(opCtx)
	if err != nil {
		p.closeConn(addr, conn, err)
		p.observe(operation, "open_stream", "error", time.Since(stageStart))
		return nil, err
	}
	p.observe(operation, "open_stream", "ok", time.Since(stageStart))

	client := &streamClient{
		conn:      stream,
		reader:    bufio.NewReader(stream),
		timeout:   p.timeout,
		metrics:   p.metrics,
		component: "quic_frame",
		pending:   make(map[uint64]chan streamResult),
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

func quicLaneForFrame(frameType replicationwire.StreamFrameType) string {
	switch frameType {
	case replicationwire.StreamFrameNodeRequest:
		return "control"
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

func (p *quicClientPool) dialContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= quicDialTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, quicDialTimeout)
}

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

func (p *quicClientPool) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || p.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.timeout)
}

func quicAddr(addr string) string {
	addr = strings.TrimSpace(strings.TrimRight(addr, "/"))
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	return addr
}

func (p *quicClientPool) observe(operation, stage, outcome string, duration time.Duration) {
	if p.metrics == nil {
		return
	}
	p.metrics.ObserveHotPathStage("quic_frame", operation, stage, outcome, duration)
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
