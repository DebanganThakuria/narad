package replication

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type streamLogStore interface {
	Get(topicName string, idx int) (*storage.Log, error)
}

type StreamLogStore interface {
	Get(topicName string, idx int) (*storage.Log, error)
}

type StreamFrameHandler interface {
	HandleStreamFrame(frame replicationwire.StreamFrame, respond func(replicationwire.StreamFrame)) bool
}

type streamFollowerLog interface {
	NextOffset() int64
	Read(int64) ([]byte, error)
	AppendBatch([][]byte) (int64, int64, error)
	AdvanceHighWatermark(int64) error
}

type streamReplicaReadLog interface {
	Read(int64) ([]byte, error)
	HighWatermark() int64
}

func ServeStream(w http.ResponseWriter, r *http.Request, logs streamLogStore, logger *slog.Logger) {
	if !isReplicationStreamUpgrade(r) {
		http.Error(w, "replication stream upgrade required", http.StatusUpgradeRequired)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "replication stream unsupported", http.StatusInternalServerError)
		return
	}

	conn, rw, err := hijacker.Hijack()
	if err != nil {
		if logger != nil {
			logger.Error("replication stream hijack", "err", err)
		}
		return
	}
	if _, err := rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + replicationwire.StreamUpgradeProtocol + "\r\n\r\n"); err != nil {
		_ = conn.Close()
		if logger != nil {
			logger.Error("replication stream handshake", "err", err)
		}
		return
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		if logger != nil {
			logger.Error("replication stream handshake flush", "err", err)
		}
		return
	}

	go serveStreamConn(conn, rw.Reader, logs, logger)
}

func isReplicationStreamUpgrade(r *http.Request) bool {
	if !strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), replicationwire.StreamUpgradeProtocol) {
		return false
	}
	for part := range strings.SplitSeq(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(part), "upgrade") {
			return true
		}
	}
	return false
}

type streamServerConn struct {
	conn    streamConn
	reader  io.Reader
	logs    streamLogStore
	logger  *slog.Logger
	handler StreamFrameHandler
	append  *replicaAppendCoordinator
	writeMu sync.Mutex
}

func ServeStreamConn(conn streamConn, reader io.Reader, logs StreamLogStore, logger *slog.Logger, handlers ...StreamFrameHandler) {
	serveStreamConn(conn, reader, logs, logger, handlers...)
}

func serveStreamConn(conn streamConn, reader io.Reader, logs streamLogStore, logger *slog.Logger, handlers ...StreamFrameHandler) {
	serveStreamConnWithCoordinator(conn, reader, logs, logger, newReplicaAppendCoordinator(logs, logger), handlers...)
}

func serveStreamConnWithCoordinator(conn streamConn, reader io.Reader, logs streamLogStore, logger *slog.Logger, appendCoordinator *replicaAppendCoordinator, handlers ...StreamFrameHandler) {
	if reader == nil {
		reader = conn
	}
	if appendCoordinator == nil {
		appendCoordinator = newReplicaAppendCoordinator(logs, logger)
	}
	c := &streamServerConn{
		conn:    conn,
		reader:  reader,
		logs:    logs,
		logger:  logger,
		handler: firstStreamFrameHandler(handlers),
		append:  appendCoordinator,
	}
	c.serve()
}

func firstStreamFrameHandler(handlers []StreamFrameHandler) StreamFrameHandler {
	for _, handler := range handlers {
		if handler != nil {
			return handler
		}
	}
	return nil
}

func (c *streamServerConn) serve() {
	defer c.conn.Close()
	for {
		frame, err := replicationwire.ReadStreamFrame(c.reader, replicationwire.MaxStreamFramePayloadBytes)
		if err != nil {
			if c.logger != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				c.logger.Debug("replication stream read", "err", err)
			}
			return
		}

		c.handleFrame(frame, true)
	}
}

func (c *streamServerConn) serveOne() {
	defer c.conn.Close()
	frame, err := replicationwire.ReadStreamFrame(c.reader, replicationwire.MaxStreamFramePayloadBytes)
	if err != nil {
		if c.logger != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("replication stream read", "err", err)
		}
		return
	}
	c.handleFrame(frame, false)
}

func (c *streamServerConn) handleFrame(frame replicationwire.StreamFrame, async bool) {
	switch frame.Type {
	case replicationwire.StreamFrameAppendBatch:
		if async {
			go c.handleAppendBatch(frame.RequestID, frame.Payload)
			return
		}
		c.handleAppendBatch(frame.RequestID, frame.Payload)
	case replicationwire.StreamFrameAppendMulti:
		if async {
			go c.handleAppendMulti(frame.RequestID, frame.Payload)
			return
		}
		c.handleAppendMulti(frame.RequestID, frame.Payload)
	case replicationwire.StreamFramePing:
		c.writeFrame(replicationwire.StreamFrame{
			Type:      replicationwire.StreamFramePong,
			RequestID: frame.RequestID,
		})
	case replicationwire.StreamFrameReplicaRead:
		if async {
			go c.handleReplicaRead(frame.RequestID, frame.Payload)
			return
		}
		c.handleReplicaRead(frame.RequestID, frame.Payload)
	default:
		if c.handler != nil && c.handler.HandleStreamFrame(frame, c.writeFrame) {
			return
		}
		c.writeError(frame.RequestID, -1, fmt.Sprintf("unsupported stream frame type %d", frame.Type))
	}
}

func (c *streamServerConn) handleReplicaRead(requestID uint64, payload []byte) {
	req, err := replicationwire.DecodeStreamReplicaRead(payload)
	if err != nil {
		c.writeError(requestID, -1, "invalid replica read: "+err.Error())
		return
	}
	if req.Topic == "" || req.Partition < 0 || req.Offset < 0 {
		c.writeError(requestID, -1, "invalid replica read")
		return
	}
	log, err := c.logs.Get(req.Topic, req.Partition)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("replication stream read open log", "topic", req.Topic, "partition", req.Partition, "err", err)
		}
		c.writeError(requestID, -1, "replica read failed")
		return
	}
	found := true
	var body []byte
	if req.CommittedOnly && req.Offset >= log.HighWatermark() {
		found = false
	} else {
		body, err = log.Read(req.Offset)
		if err != nil {
			if errors.Is(err, storage.ErrOffsetNotFound) {
				found = false
			} else {
				if c.logger != nil {
					c.logger.Error("replication stream read", "topic", req.Topic, "partition", req.Partition, "offset", req.Offset, "err", err)
				}
				c.writeError(requestID, -1, "replica read failed")
				return
			}
		}
	}
	data, err := replicationwire.EncodeStreamReplicaData(found, body)
	if err != nil {
		c.writeError(requestID, -1, err.Error())
		return
	}
	c.writeFrame(replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameReplicaData,
		RequestID: requestID,
		Payload:   data,
	})
}

func (c *streamServerConn) handleAppendBatch(requestID uint64, payload []byte) {
	req, err := replicationwire.DecodeStreamAppendBatch(payload)
	if err != nil {
		c.writeError(requestID, -1, "invalid append batch: "+err.Error())
		return
	}
	if err := validateStreamAppendBatch(req); err != nil {
		c.writeError(requestID, -1, err.Error())
		return
	}

	next, err := c.append.append(req)
	if err != nil {
		var mismatch *OffsetMismatchError
		if errors.As(err, &mismatch) {
			c.writeError(requestID, mismatch.ReplicaNextOffset, "replicate offset mismatch")
			return
		}
		if c.logger != nil {
			c.logger.Error("replication stream append", "topic", req.Topic, "partition", req.Partition, "err", err)
		}
		c.writeError(requestID, -1, "replicate failed")
		return
	}

	ack, err := replicationwire.EncodeStreamAck(next)
	if err != nil {
		c.writeError(requestID, -1, err.Error())
		return
	}
	c.writeFrame(replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameAck,
		RequestID: requestID,
		Payload:   ack,
	})
}

func (c *streamServerConn) handleAppendMulti(requestID uint64, payload []byte) {
	req, err := replicationwire.DecodeStreamAppendMulti(payload)
	if err != nil {
		c.writeError(requestID, -1, "invalid append multi: "+err.Error())
		return
	}

	results := make([]replicationwire.StreamAppendResult, len(req.Groups))
	for i, group := range req.Groups {
		results[i] = c.appendMultiGroup(group)
	}

	ack, err := replicationwire.EncodeStreamAppendResults(results)
	if err != nil {
		c.writeError(requestID, -1, err.Error())
		return
	}
	c.writeFrame(replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameMultiAck,
		RequestID: requestID,
		Payload:   ack,
	})
}

func (c *streamServerConn) appendMultiGroup(group replicationwire.StreamAppendGroup) replicationwire.StreamAppendResult {
	return c.append.appendGroup(group)
}

func (c *streamServerConn) writeError(requestID uint64, replicaNextOffset int64, message string) {
	payload, err := replicationwire.EncodeStreamError(replicaNextOffset, message)
	if err != nil {
		return
	}
	c.writeFrame(replicationwire.StreamFrame{
		Type:      replicationwire.StreamFrameError,
		RequestID: requestID,
		Payload:   payload,
	})
}

func (c *streamServerConn) writeFrame(frame replicationwire.StreamFrame) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := replicationwire.WriteStreamFrame(c.conn, frame); err != nil && c.logger != nil {
		c.logger.Debug("replication stream write", "err", err)
	}
}

func validateStreamAppendBatch(req replicationwire.StreamAppendBatch) error {
	if req.Topic == "" {
		return errors.New("topic required")
	}
	if req.Partition < 0 {
		return errors.New("partition must be >= 0")
	}
	if req.BaseOffset < 0 {
		return errors.New("offset must be >= 0")
	}
	if len(req.Payloads) == 0 {
		return errors.New("batch must contain at least one payload")
	}
	return nil
}

func appendReplicaBatch(log streamFollowerLog, req replicationwire.StreamAppendBatch) (int64, error) {
	next := log.NextOffset()
	endOffset := req.BaseOffset + int64(len(req.Payloads))
	if next < req.BaseOffset {
		return next, &OffsetMismatchError{RequestedOffset: req.BaseOffset, ReplicaNextOffset: next}
	}

	appendStart := next
	if next > req.BaseOffset {
		verifiedUntil, accepted := acceptDuplicateStreamBatchPrefix(log, req, min(next, endOffset))
		if !accepted {
			return next, &OffsetMismatchError{RequestedOffset: req.BaseOffset, ReplicaNextOffset: next}
		}
		appendStart = verifiedUntil
	}
	if appendStart < endOffset {
		startIdx := int(appendStart - req.BaseOffset)
		first, last, err := log.AppendBatch(req.Payloads[startIdx:])
		if err != nil {
			return next, err
		}
		if first != appendStart || last != endOffset-1 {
			return first, &OffsetMismatchError{RequestedOffset: appendStart, ReplicaNextOffset: first}
		}
	}
	if err := log.AdvanceHighWatermark(endOffset); err != nil {
		return next, err
	}
	return endOffset, nil
}

func acceptDuplicateStreamBatchPrefix(log streamFollowerLog, req replicationwire.StreamAppendBatch, exclusiveEnd int64) (int64, bool) {
	for offset := req.BaseOffset; offset < exclusiveEnd; offset++ {
		existing, err := log.Read(offset)
		idx := int(offset - req.BaseOffset)
		if err != nil || idx < 0 || idx >= len(req.Payloads) || !bytes.Equal(existing, req.Payloads[idx]) {
			return offset, false
		}
	}
	return exclusiveEnd, true
}
