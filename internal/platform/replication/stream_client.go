package replication

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

const defaultStreamTimeout = 5 * time.Second

type streamConn interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

type streamClientPool struct {
	selfID  string
	timeout time.Duration

	mu      sync.Mutex
	clients map[string]*streamClient
}

func newStreamClientPool(selfID string, timeout time.Duration) *streamClientPool {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	return &streamClientPool{
		selfID:  selfID,
		timeout: timeout,
		clients: make(map[string]*streamClient),
	}
}

func (p *streamClientPool) get(ctx context.Context, addr string, shard int) (*streamClient, error) {
	key := streamClientPoolKey(addr, shard)
	p.mu.Lock()
	if client := p.clients[key]; client != nil && !client.isClosed() {
		p.mu.Unlock()
		return client, nil
	}
	p.mu.Unlock()

	client, err := dialStreamClient(ctx, streamEndpoint(addr), p.selfID, p.timeout)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.clients[key]; existing != nil && !existing.isClosed() {
		client.closeWithError(errors.New("superseded by existing stream client"))
		return existing, nil
	}
	p.clients[key] = client
	return client, nil
}

func streamClientPoolKey(addr string, shard int) string {
	if shard < 0 {
		shard = 0
	}
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	return addr + "\x00" + strconv.Itoa(shard)
}

type streamClient struct {
	conn      streamConn
	reader    *bufio.Reader
	timeout   time.Duration
	metrics   stageObserver
	component string

	writerMu sync.Mutex
	nextID   atomic.Uint64
	closed   atomic.Bool
	closeMu  sync.Mutex
	closeErr error

	pendingMu sync.Mutex
	pending   map[uint64]chan streamResult
}

type streamResult struct {
	nextOffset int64
	results    []replicationwire.StreamAppendResult
	frame      replicationwire.StreamFrame
	err        error
}

func dialStreamClient(ctx context.Context, endpoint, leaderID string, timeout time.Duration) (*streamClient, error) {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse replication stream endpoint: %w", err)
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "http"
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("replication stream endpoint missing host: %q", endpoint)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported replication stream scheme %q", parsed.Scheme)
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", parsed.Host)
	if err != nil {
		return nil, fmt.Errorf("dial replication stream: %w", err)
	}
	if err := setHandshakeDeadline(ctx, conn, timeout); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if parsed.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: tlsServerName(parsed.Host)})
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("handshake replication stream TLS: %w", err)
		}
		conn = tlsConn
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("build replication stream upgrade request: %w", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", replicationwire.StreamUpgradeProtocol)
	req.Header.Set(replicationwire.HeaderLeaderID, leaderID)
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write replication stream upgrade request: %w", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read replication stream upgrade response: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("replication stream upgrade failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), replicationwire.StreamUpgradeProtocol) {
		_ = conn.Close()
		return nil, fmt.Errorf("replication stream upgrade protocol mismatch: %q", resp.Header.Get("Upgrade"))
	}
	_ = conn.SetDeadline(time.Time{})

	client := &streamClient{
		conn:    conn,
		reader:  reader,
		timeout: timeout,
		pending: make(map[uint64]chan streamResult),
	}
	go client.readLoop()
	return client, nil
}

func setHandshakeDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(timeout)
	}
	return conn.SetDeadline(deadline)
}

func tlsServerName(host string) string {
	name, _, err := net.SplitHostPort(host)
	if err == nil {
		return name
	}
	return host
}

func (c *streamClient) appendBatch(ctx context.Context, topic string, partition int, records []Record) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = record.Payload
	}
	payload, err := replicationwire.EncodeStreamAppendBatch(topic, partition, records[0].Offset, payloads)
	if err != nil {
		return 0, err
	}

	requestID := c.nextID.Add(1)
	resultCh := make(chan streamResult, 1)
	c.addPending(requestID, resultCh)
	stageStart := time.Now()
	if err := c.writeFrame(ctx, replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameAppendBatch,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		c.removePending(requestID)
		c.observe("append_batch", "write_frame", "error", time.Since(stageStart))
		return 0, err
	}
	c.observe("append_batch", "write_frame", "ok", time.Since(stageStart))

	stageStart = time.Now()
	select {
	case result := <-resultCh:
		c.observe("append_batch", "wait_reply", observeOutcome(result.err), time.Since(stageStart))
		return result.nextOffset, result.err
	case <-ctx.Done():
		c.removePending(requestID)
		c.closeWithError(ctx.Err())
		c.observe("append_batch", "wait_reply", "error", time.Since(stageStart))
		return 0, ctx.Err()
	}
}

func (c *streamClient) appendMulti(ctx context.Context, groups []replicationwire.StreamAppendGroup) ([]replicationwire.StreamAppendResult, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	payload, err := replicationwire.EncodeStreamAppendMulti(groups)
	if err != nil {
		return nil, err
	}

	requestID := c.nextID.Add(1)
	resultCh := make(chan streamResult, 1)
	c.addPending(requestID, resultCh)
	stageStart := time.Now()
	if err := c.writeFrame(ctx, replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameAppendMulti,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		c.removePending(requestID)
		c.observe("append_multi", "write_frame", "error", time.Since(stageStart))
		return nil, err
	}
	c.observe("append_multi", "write_frame", "ok", time.Since(stageStart))

	stageStart = time.Now()
	select {
	case result := <-resultCh:
		c.observe("append_multi", "wait_reply", observeOutcome(result.err), time.Since(stageStart))
		return result.results, result.err
	case <-ctx.Done():
		c.removePending(requestID)
		c.closeWithError(ctx.Err())
		c.observe("append_multi", "wait_reply", "error", time.Since(stageStart))
		return nil, ctx.Err()
	}
}

func (c *streamClient) requestFrame(ctx context.Context, frameType replicationwire.StreamFrameType, payload []byte) (replicationwire.StreamFrame, error) {
	operation := streamFrameOperation(frameType)
	requestID := c.nextID.Add(1)
	resultCh := make(chan streamResult, 1)
	c.addPending(requestID, resultCh)
	stageStart := time.Now()
	if err := c.writeFrame(ctx, replicationwire.StreamFrame{
		Type:      frameType,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		c.removePending(requestID)
		c.observe(operation, "write_frame", "error", time.Since(stageStart))
		return replicationwire.StreamFrame{}, err
	}
	c.observe(operation, "write_frame", "ok", time.Since(stageStart))

	stageStart = time.Now()
	select {
	case result := <-resultCh:
		c.observe(operation, "wait_reply", observeOutcome(result.err), time.Since(stageStart))
		return result.frame, result.err
	case <-ctx.Done():
		c.removePending(requestID)
		c.closeWithError(ctx.Err())
		c.observe(operation, "wait_reply", "error", time.Since(stageStart))
		return replicationwire.StreamFrame{}, ctx.Err()
	}
}

func (c *streamClient) observe(operation, stage, outcome string, duration time.Duration) {
	if c.metrics == nil {
		return
	}
	component := c.component
	if component == "" {
		component = "stream_client"
	}
	c.metrics.ObserveHotPathStage(component, operation, stage, outcome, duration)
}

func streamFrameOperation(frameType replicationwire.StreamFrameType) string {
	switch frameType {
	case replicationwire.StreamFrameAppendBatch:
		return "append_batch"
	case replicationwire.StreamFrameAppendMulti:
		return "append_multi"
	case replicationwire.StreamFrameReplicaRead:
		return "replica_read"
	case replicationwire.StreamFrameNodeRequest:
		return "node_request"
	case replicationwire.StreamFramePing:
		return "ping"
	default:
		return "unknown"
	}
}

func (c *streamClient) addPending(requestID uint64, ch chan streamResult) {
	c.pendingMu.Lock()
	c.pending[requestID] = ch
	c.pendingMu.Unlock()
}

func (c *streamClient) removePending(requestID uint64) {
	c.pendingMu.Lock()
	delete(c.pending, requestID)
	c.pendingMu.Unlock()
}

func (c *streamClient) complete(requestID uint64, result streamResult) {
	c.pendingMu.Lock()
	ch := c.pending[requestID]
	delete(c.pending, requestID)
	c.pendingMu.Unlock()
	if ch == nil {
		return
	}
	ch <- result
}

func (c *streamClient) writeFrame(ctx context.Context, frame replicationwire.StreamFrame) error {
	if c.isClosed() {
		return c.err()
	}
	c.writerMu.Lock()
	defer c.writerMu.Unlock()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.timeout)
	}
	_ = c.conn.SetWriteDeadline(deadline)
	err := replicationwire.WriteStreamFrame(c.conn, frame)
	_ = c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}

func (c *streamClient) readLoop() {
	for {
		frame, err := replicationwire.ReadStreamFrame(c.reader, replicationwire.MaxStreamFramePayloadBytes)
		if err != nil {
			c.closeWithError(err)
			return
		}
		switch frame.Type {
		case replicationwire.StreamFrameAck:
			next, err := replicationwire.DecodeStreamAck(frame.Payload)
			c.complete(frame.RequestID, streamResult{nextOffset: next, err: err})
		case replicationwire.StreamFrameMultiAck:
			results, err := replicationwire.DecodeStreamAppendResults(frame.Payload)
			c.complete(frame.RequestID, streamResult{results: results, err: err})
		case replicationwire.StreamFrameNodeReply, replicationwire.StreamFrameReplicaData:
			c.complete(frame.RequestID, streamResult{frame: frame})
		case replicationwire.StreamFrameError:
			streamErr, err := replicationwire.DecodeStreamError(frame.Payload)
			if err != nil {
				c.complete(frame.RequestID, streamResult{err: err})
				continue
			}
			if streamErr.ReplicaNextOffset >= 0 {
				c.complete(frame.RequestID, streamResult{err: &OffsetMismatchError{
					RequestedOffset:   -1,
					ReplicaNextOffset: streamErr.ReplicaNextOffset,
				}})
				continue
			}
			c.complete(frame.RequestID, streamResult{err: errors.New(streamErr.Message)})
		case replicationwire.StreamFramePong:
		default:
			c.closeWithError(fmt.Errorf("unsupported replication stream frame type %d", frame.Type))
			return
		}
	}
}

func (c *streamClient) isClosed() bool {
	return c.closed.Load()
}

func (c *streamClient) err() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closeErr == nil {
		return errors.New("replication stream closed")
	}
	return c.closeErr
}

func (c *streamClient) closeWithError(err error) {
	if err == nil {
		err = errors.New("replication stream closed")
	}
	c.closeMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeMu.Unlock()
	if c.closed.Swap(true) {
		return
	}
	_ = c.conn.Close()

	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[uint64]chan streamResult)
	c.pendingMu.Unlock()
	for _, ch := range pending {
		ch <- streamResult{err: err}
	}
}
