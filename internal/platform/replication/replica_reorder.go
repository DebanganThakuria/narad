package replication

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

const (
	maxStagedReplicaBatchesPerPartition = 4096
	maxStagedReplicaBytesPerPartition   = 64 << 20
)

type replicaAppendCoordinator struct {
	logs   streamLogStore
	logger *slog.Logger

	mu         sync.Mutex
	partitions map[replicaAppendKey]*replicaPartitionReorder
}

type replicaAppendKey struct {
	topic     string
	partition int
}

type replicaPartitionReorder struct {
	topic     string
	partition int
	logger    *slog.Logger

	mu     sync.Mutex
	log    streamFollowerLog
	staged map[int64]stagedReplicaBatch
	bytes  int
	signal chan struct{}
}

type stagedReplicaBatch struct {
	req   replicationwire.StreamAppendBatch
	bytes int
}

func newReplicaAppendCoordinator(logs streamLogStore, logger *slog.Logger) *replicaAppendCoordinator {
	return &replicaAppendCoordinator{
		logs:       logs,
		logger:     logger,
		partitions: make(map[replicaAppendKey]*replicaPartitionReorder),
	}
}

func (c *replicaAppendCoordinator) appendGroup(group replicationwire.StreamAppendGroup) replicationwire.StreamAppendResult {
	req := replicationwire.StreamAppendBatch{
		Topic:      group.Topic,
		Partition:  group.Partition,
		BaseOffset: group.BaseOffset,
		Payloads:   group.Payloads,
	}
	next, err := c.append(req)
	if err != nil {
		var mismatch *OffsetMismatchError
		if errors.As(err, &mismatch) {
			return replicationwire.StreamAppendResult{
				NextOffset:        -1,
				ReplicaNextOffset: mismatch.ReplicaNextOffset,
				Message:           "replicate offset mismatch",
			}
		}
		return replicationwire.StreamAppendResult{NextOffset: -1, ReplicaNextOffset: -1, Message: err.Error()}
	}
	return replicationwire.StreamAppendResult{NextOffset: next, ReplicaNextOffset: -1}
}

func (c *replicaAppendCoordinator) append(req replicationwire.StreamAppendBatch) (int64, error) {
	if err := validateStreamAppendBatch(req); err != nil {
		return 0, err
	}

	log, err := c.logs.Get(req.Topic, req.Partition)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("replication stream open log", "topic", req.Topic, "partition", req.Partition, "err", err)
		}
		return 0, fmt.Errorf("replicate failed")
	}

	return c.partition(req.Topic, req.Partition).append(log, req)
}

func (c *replicaAppendCoordinator) partition(topic string, partition int) *replicaPartitionReorder {
	key := replicaAppendKey{topic: topic, partition: partition}

	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.partitions[key]; state != nil {
		return state
	}
	state := &replicaPartitionReorder{
		topic:     topic,
		partition: partition,
		logger:    c.logger,
		staged:    make(map[int64]stagedReplicaBatch),
		signal:    make(chan struct{}, 1),
	}
	go state.runDrainer()
	c.partitions[key] = state
	return state
}

func (p *replicaPartitionReorder) append(log streamFollowerLog, req replicationwire.StreamAppendBatch) (int64, error) {
	expectedNext := req.BaseOffset + int64(len(req.Payloads))

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.log == nil {
		p.log = log
	}

	next := log.NextOffset()
	if expectedNext <= next {
		if _, accepted := acceptDuplicateStreamBatchPrefix(log, req, expectedNext); !accepted {
			return 0, &OffsetMismatchError{RequestedOffset: req.BaseOffset, ReplicaNextOffset: next}
		}
		return expectedNext, nil
	}
	if req.BaseOffset < next {
		verifiedUntil, accepted := acceptDuplicateStreamBatchPrefix(log, req, next)
		if !accepted {
			return 0, &OffsetMismatchError{RequestedOffset: req.BaseOffset, ReplicaNextOffset: next}
		}
		startIdx := int(verifiedUntil - req.BaseOffset)
		req.BaseOffset = verifiedUntil
		req.Payloads = req.Payloads[startIdx:]
	}

	next, err := p.stageLocked(next, req)
	if err != nil {
		return 0, err
	}
	p.signalDrain()
	return expectedNext, nil
}

func (p *replicaPartitionReorder) stageLocked(replicaNext int64, req replicationwire.StreamAppendBatch) (int64, error) {
	expectedNext := req.BaseOffset + int64(len(req.Payloads))
	size := payloadBytes(req.Payloads)
	if existing, ok := p.staged[req.BaseOffset]; ok {
		if sameStreamAppendBatch(existing.req, req) {
			return expectedNext, nil
		}
		return 0, fmt.Errorf("conflicting staged replica batch at offset %d", req.BaseOffset)
	}
	if len(p.staged) >= maxStagedReplicaBatchesPerPartition || p.bytes+size > maxStagedReplicaBytesPerPartition {
		return 0, &OffsetMismatchError{
			RequestedOffset:   req.BaseOffset,
			ReplicaNextOffset: replicaNext,
		}
	}
	p.staged[req.BaseOffset] = stagedReplicaBatch{
		req:   cloneStreamAppendBatch(req),
		bytes: size,
	}
	p.bytes += size
	return expectedNext, nil
}

func (p *replicaPartitionReorder) runDrainer() {
	for range p.signal {
		p.drain()
	}
}

func (p *replicaPartitionReorder) signalDrain() {
	select {
	case p.signal <- struct{}{}:
	default:
	}
}

func (p *replicaPartitionReorder) drain() {
	for {
		p.mu.Lock()
		log := p.log
		if log == nil {
			p.mu.Unlock()
			return
		}
		next := log.NextOffset()
		base, batch, ok := p.nextReadyLocked(next)
		if !ok {
			p.mu.Unlock()
			return
		}
		delete(p.staged, base)
		p.bytes -= batch.bytes
		p.mu.Unlock()

		if _, err := appendReplicaBatch(log, batch.req); err != nil {
			p.restoreBatch(base, batch)
			if p.logger != nil {
				p.logger.Error("replication reorder drain", "topic", p.topic, "partition", p.partition, "base_offset", base, "err", err)
			}
			return
		}
	}
}

func (p *replicaPartitionReorder) restoreBatch(base int64, batch stagedReplicaBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.staged[base]; ok {
		return
	}
	p.staged[base] = batch
	p.bytes += batch.bytes
}

func (p *replicaPartitionReorder) nextReadyLocked(next int64) (int64, stagedReplicaBatch, bool) {
	var (
		bestBase int64
		best     stagedReplicaBatch
		found    bool
	)
	for base, batch := range p.staged {
		end := base + int64(len(batch.req.Payloads))
		if end <= next {
			delete(p.staged, base)
			p.bytes -= batch.bytes
			continue
		}
		if base > next {
			continue
		}
		if !found || base < bestBase {
			bestBase = base
			best = batch
			found = true
		}
	}
	return bestBase, best, found
}

func cloneStreamAppendBatch(req replicationwire.StreamAppendBatch) replicationwire.StreamAppendBatch {
	payloads := make([][]byte, len(req.Payloads))
	for i, payload := range req.Payloads {
		payloads[i] = append([]byte(nil), payload...)
	}
	req.Payloads = payloads
	return req
}

func sameStreamAppendBatch(a, b replicationwire.StreamAppendBatch) bool {
	if a.Topic != b.Topic || a.Partition != b.Partition || a.BaseOffset != b.BaseOffset || len(a.Payloads) != len(b.Payloads) {
		return false
	}
	for i := range a.Payloads {
		if !bytes.Equal(a.Payloads[i], b.Payloads[i]) {
			return false
		}
	}
	return true
}

func payloadBytes(payloads [][]byte) int {
	var total int
	for _, payload := range payloads {
		total += len(payload)
	}
	return total
}
