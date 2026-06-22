package replication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type StreamingCluster struct {
	selfID  string
	store   memberLister
	quic    *quicClientPool
	metrics stageObserver
	timeout time.Duration

	appendBatchersMu sync.Mutex
	appendBatchers   map[string]*streamAppendBatcher
}

func NewStreamingCluster(selfID string, store memberLister, client *http.Client, observers ...stageObserver) *StreamingCluster {
	timeout := defaultStreamTimeout
	if client != nil && client.Timeout > 0 {
		timeout = client.Timeout
	}
	return NewStreamingClusterWithTimeout(selfID, store, timeout, observers...)
}

func NewStreamingClusterWithTimeout(selfID string, store memberLister, timeout time.Duration, observers ...stageObserver) *StreamingCluster {
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &StreamingCluster{
		selfID:  selfID,
		store:   store,
		quic:    newQUICClientPool(timeout, observer),
		metrics: observer,
		timeout: timeout,

		appendBatchers: make(map[string]*streamAppendBatcher),
	}
}

func (c *StreamingCluster) Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error {
	return c.ReplicateBatch(ctx, topic, partition, []Record{{Offset: offset, Payload: payload}})
}

func (c *StreamingCluster) ReplicateBatch(ctx context.Context, topic string, partition int, records []Record) error {
	if len(records) == 0 {
		return nil
	}

	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		c.observe("replicate_batch", "total", totalOutcome, time.Since(totalStart))
	}()

	for i := 1; i < len(records); i++ {
		if records[i].Offset != records[i-1].Offset+1 {
			totalOutcome = "error"
			return fmt.Errorf("replicate batch offsets must be contiguous: %d after %d", records[i].Offset, records[i-1].Offset)
		}
	}

	follower, ok, err := c.replicationFollower("stream_replicate_batch", topic, partition)
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if !ok {
		totalOutcome = "no_follower"
		return nil
	}

	opCtx, cancel := c.operationContext(ctx)
	defer cancel()

	stageStart := time.Now()
	next, err := c.appendToFollower(opCtx, follower.Addr, topic, partition, records)
	c.observe("replicate_batch", "quic_append", observeOutcome(err), time.Since(stageStart))
	if err == nil {
		expectedNext := records[0].Offset + int64(len(records))
		if next != expectedNext {
			totalOutcome = "offset_mismatch"
			return &OffsetMismatchError{RequestedOffset: records[0].Offset, ReplicaNextOffset: next}
		}
		return nil
	}
	if ctx.Err() != nil {
		totalOutcome = "error"
		return ctx.Err()
	}
	var mismatch *OffsetMismatchError
	if errors.As(err, &mismatch) {
		totalOutcome = "offset_mismatch"
		if mismatch.RequestedOffset < 0 {
			mismatch.RequestedOffset = records[0].Offset
		}
		return mismatch
	}

	totalOutcome = "error"
	return err
}

func (c *StreamingCluster) appendToFollower(ctx context.Context, addr, topic string, partition int, records []Record) (int64, error) {
	return c.appendBatcher(addr).append(ctx, streamAppendBatchJob{
		records: records,
		group:   streamAppendGroupForRecords(topic, partition, records),
	})
}

func (c *StreamingCluster) CatchUp(ctx context.Context, topic string, partition int, log LeaderLog, opts CatchUpOptions) error {
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		c.observe("catch_up", "total", totalOutcome, time.Since(totalStart))
	}()

	ownerNext := log.NextOffset()
	if ownerNext <= log.HighWatermark() && opts.FollowerNextOffset == nil {
		totalOutcome = "already_caught_up"
		return nil
	}

	follower, ok, err := c.replicationFollower("stream_catch_up", topic, partition)
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if !ok {
		if ownerNext > log.HighWatermark() {
			if err := log.AdvanceHighWatermark(ownerNext); err != nil {
				totalOutcome = "error"
				return err
			}
		}
		totalOutcome = "no_follower"
		return nil
	}

	opCtx, cancel := c.operationContext(ctx)
	defer cancel()

	stageStart := time.Now()
	start, err := c.followerNextOffset(opCtx, follower.Addr, topic, partition, ownerNext, opts)
	c.observe("catch_up", "follower_next_offset", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if start > ownerNext {
		totalOutcome = "follower_ahead"
		return fmt.Errorf("follower %s is ahead for %s/%d: follower_next=%d owner_next=%d", follower.ID, topic, partition, start, ownerNext)
	}

	for offset := start; offset < ownerNext; {
		stageStart = time.Now()
		payload, err := log.Read(offset)
		c.observe("catch_up", "read_owner_record", observeOutcome(err), time.Since(stageStart))
		if err != nil {
			totalOutcome = "error"
			return fmt.Errorf("read owner record %d: %w", offset, err)
		}

		stageStart = time.Now()
		_, err = c.appendToFollower(opCtx, follower.Addr, topic, partition, []Record{{Offset: offset, Payload: payload}})
		c.observe("catch_up", "quic_append", observeOutcome(err), time.Since(stageStart))
		if err != nil {
			var mismatch *OffsetMismatchError
			if errors.As(err, &mismatch) && mismatch.ReplicaNextOffset > offset {
				offset = mismatch.ReplicaNextOffset
				continue
			}
			totalOutcome = "error"
			return err
		}
		offset++
		stageStart = time.Now()
		if err := log.AdvanceHighWatermark(offset); err != nil {
			c.observe("catch_up", "advance_high_watermark", "error", time.Since(stageStart))
			totalOutcome = "error"
			return fmt.Errorf("advance repaired high watermark: %w", err)
		}
		c.observe("catch_up", "advance_high_watermark", "ok", time.Since(stageStart))
	}
	if ownerNext > log.HighWatermark() {
		stageStart := time.Now()
		if err := log.AdvanceHighWatermark(ownerNext); err != nil {
			c.observe("catch_up", "advance_high_watermark", "error", time.Since(stageStart))
			totalOutcome = "error"
			return fmt.Errorf("advance repaired high watermark: %w", err)
		}
		c.observe("catch_up", "advance_high_watermark", "ok", time.Since(stageStart))
	}
	return nil
}

func (c *StreamingCluster) followerNextOffset(ctx context.Context, addr, topic string, partition int, ownerNext int64, opts CatchUpOptions) (int64, error) {
	if opts.FollowerNextOffset != nil {
		if *opts.FollowerNextOffset < 0 {
			return 0, fmt.Errorf("invalid follower next offset %d", *opts.FollowerNextOffset)
		}
		return *opts.FollowerNextOffset, nil
	}
	return c.findReplicaNextOffset(ctx, addr, topic, partition, ownerNext)
}

func (c *StreamingCluster) findReplicaNextOffset(ctx context.Context, addr, topic string, partition int, upperBound int64) (int64, error) {
	if upperBound <= 0 {
		return 0, nil
	}
	low, high := int64(0), upperBound
	for low < high {
		mid := low + (high-low)/2
		_, found, err := c.quic.readReplica(ctx, addr, topic, partition, mid, false)
		if err != nil {
			return 0, err
		}
		if found {
			low = mid + 1
			continue
		}
		high = mid
	}
	return low, nil
}

func (c *StreamingCluster) replicationFollower(operation, topic string, partition int) (metastore.Member, bool, error) {
	stageStart := time.Now()
	assignment, err := c.store.GetAssignment(topic, partition)
	c.observe(operation, "assignment_lookup", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return metastore.Member{}, false, fmt.Errorf("lookup assignment: %w", err)
	}
	if assignment.FollowerID == "" || assignment.FollowerID == c.selfID {
		return metastore.Member{}, false, nil
	}

	stageStart = time.Now()
	follower, err := c.store.GetMember(assignment.FollowerID)
	c.observe(operation, "member_lookup", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return metastore.Member{}, false, fmt.Errorf("lookup follower: %w", err)
	}
	if follower.Status == metastore.MemberDead {
		return metastore.Member{}, false, fmt.Errorf("follower %s is dead", follower.ID)
	}
	return follower, true, nil
}

func (c *StreamingCluster) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || c.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

func (c *StreamingCluster) observe(operation, stage, outcome string, duration time.Duration) {
	if c.metrics == nil {
		return
	}
	c.metrics.ObserveHotPathStage("replication_stream", operation, stage, outcome, duration)
}

var (
	_ Replicator        = (*StreamingCluster)(nil)
	_ BatchReplicator   = (*StreamingCluster)(nil)
	_ CatchUpReplicator = (*StreamingCluster)(nil)
)
