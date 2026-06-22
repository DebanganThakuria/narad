package replication

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type memberLister interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
}

type Cluster struct {
	selfID  string
	store   memberLister
	client  *http.Client
	metrics stageObserver
}

type stageObserver interface {
	ObserveHotPathStage(component, operation, stage, outcome string, duration time.Duration)
}

type OffsetMismatchError struct {
	RequestedOffset   int64
	ReplicaNextOffset int64
}

func (e *OffsetMismatchError) Error() string {
	return fmt.Sprintf("replicate offset mismatch: replica_next_offset=%d requested_offset=%d", e.ReplicaNextOffset, e.RequestedOffset)
}

func NewCluster(selfID string, store memberLister, client *http.Client, observers ...stageObserver) Cluster {
	if client == nil {
		client = http.DefaultClient
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return Cluster{selfID: selfID, store: store, client: client, metrics: observer}
}

func (c Cluster) Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error {
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		c.observe("replicate", "total", totalOutcome, time.Since(totalStart))
	}()

	follower, ok, err := c.replicationFollower("replicate", topic, partition)
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if !ok {
		totalOutcome = "no_follower"
		return nil
	}

	status, header, err := c.pushRawReplicaRecord(ctx, "replicate", follower.Addr, topic, partition, offset, payload)
	if err != nil {
		totalOutcome = "error"
		return err
	}

	if status == http.StatusConflict {
		if replicaNext, ok := replicaNextOffset(header); ok {
			totalOutcome = "offset_mismatch"
			return &OffsetMismatchError{RequestedOffset: offset, ReplicaNextOffset: replicaNext}
		}
	}
	if status != http.StatusNoContent {
		totalOutcome = "bad_status"
		return fmt.Errorf("replicate request failed with status %d", status)
	}
	return nil
}

func (c Cluster) ReplicateBatch(ctx context.Context, topic string, partition int, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) == 1 {
		record := records[0]
		return c.Replicate(ctx, topic, partition, record.Offset, record.Payload)
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

	follower, ok, err := c.replicationFollower("replicate_batch", topic, partition)
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if !ok {
		totalOutcome = "no_follower"
		return nil
	}

	status, header, err := c.pushRawReplicaBatch(ctx, follower.Addr, topic, partition, records)
	if err != nil {
		totalOutcome = "error"
		return err
	}

	if status == http.StatusConflict {
		if replicaNext, ok := replicaNextOffset(header); ok {
			totalOutcome = "offset_mismatch"
			return &OffsetMismatchError{RequestedOffset: records[0].Offset, ReplicaNextOffset: replicaNext}
		}
	}
	if status != http.StatusNoContent {
		totalOutcome = "bad_status"
		return fmt.Errorf("replicate batch request failed with status %d", status)
	}
	return nil
}

func (c Cluster) CatchUp(ctx context.Context, topic string, partition int, log LeaderLog, opts CatchUpOptions) error {
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

	follower, ok, err := c.replicationFollower("catch_up", topic, partition)
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

	stageStart := time.Now()
	start, err := c.followerNextOffset(ctx, follower.Addr, topic, partition, ownerNext, opts)
	c.observe("catch_up", "follower_next_offset", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return err
	}
	if start > ownerNext {
		totalOutcome = "follower_ahead"
		return fmt.Errorf("follower %s is ahead for %s/%d: follower_next=%d owner_next=%d", follower.ID, topic, partition, start, ownerNext)
	}

	for offset := start; offset < ownerNext; offset++ {
		stageStart = time.Now()
		payload, err := log.Read(offset)
		c.observe("catch_up", "read_owner_record", observeOutcome(err), time.Since(stageStart))
		if err != nil {
			totalOutcome = "error"
			return fmt.Errorf("read owner record %d: %w", offset, err)
		}
		status, header, err := c.pushRawReplicaRecord(ctx, "catch_up", follower.Addr, topic, partition, offset, payload)
		if err != nil {
			totalOutcome = "error"
			return err
		}
		if status == http.StatusConflict {
			if replicaNext, ok := replicaNextOffset(header); ok && replicaNext > offset {
				offset = replicaNext - 1
				continue
			}
		}
		if status != http.StatusNoContent {
			totalOutcome = "bad_status"
			return fmt.Errorf("replicate catch-up failed with status %d", status)
		}
		stageStart = time.Now()
		if err := log.AdvanceHighWatermark(offset + 1); err != nil {
			c.observe("catch_up", "advance_high_watermark", "error", time.Since(stageStart))
			totalOutcome = "error"
			return fmt.Errorf("advance repaired high watermark: %w", err)
		}
		c.observe("catch_up", "advance_high_watermark", "ok", time.Since(stageStart))
	}
	if ownerNext > log.HighWatermark() {
		stageStart = time.Now()
		if err := log.AdvanceHighWatermark(ownerNext); err != nil {
			c.observe("catch_up", "advance_high_watermark", "error", time.Since(stageStart))
			totalOutcome = "error"
			return fmt.Errorf("advance repaired high watermark: %w", err)
		}
		c.observe("catch_up", "advance_high_watermark", "ok", time.Since(stageStart))
	}
	return nil
}

func (c Cluster) replicationFollower(operation, topic string, partition int) (metastore.Member, bool, error) {
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

func (c Cluster) followerNextOffset(ctx context.Context, addr, topic string, partition int, ownerNext int64, opts CatchUpOptions) (int64, error) {
	if opts.FollowerNextOffset != nil {
		if *opts.FollowerNextOffset < 0 {
			return 0, fmt.Errorf("invalid follower next offset %d", *opts.FollowerNextOffset)
		}
		return *opts.FollowerNextOffset, nil
	}
	return findReplicaNextOffset(ctx, c.client, addr, topic, partition, ownerNext)
}

func (c Cluster) pushRawReplicaRecord(ctx context.Context, operation, addr, topic string, partition int, offset int64, payload []byte) (int, http.Header, error) {
	stageStart := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, replicateEndpoint(addr), bytes.NewReader(payload))
	c.observe(operation, "build_request", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("build replicate request: %w", err)
	}
	req.Header.Set("Content-Type", replicationwire.RawContentType)
	req.Header.Set(replicationwire.HeaderTopic, topic)
	req.Header.Set(replicationwire.HeaderPartition, strconv.Itoa(partition))
	req.Header.Set(replicationwire.HeaderOffset, strconv.FormatInt(offset, 10))
	req.Header.Set(replicationwire.HeaderLeaderID, c.selfID)

	stageStart = time.Now()
	resp, err := c.client.Do(req)
	c.observe(operation, "send_request", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("send replicate request: %w", err)
	}
	defer resp.Body.Close()
	stageStart = time.Now()
	_, err = io.Copy(io.Discard, resp.Body)
	c.observe(operation, "drain_response", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("drain replicate response: %w", err)
	}

	return resp.StatusCode, resp.Header.Clone(), nil
}

func (c Cluster) pushRawReplicaBatch(ctx context.Context, addr, topic string, partition int, records []Record) (int, http.Header, error) {
	stageStart := time.Now()
	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = record.Payload
	}
	body, err := replicationwire.EncodeBatchPayload(payloads)
	if err != nil {
		c.observe("replicate_batch", "build_request", "error", time.Since(stageStart))
		return 0, nil, fmt.Errorf("encode replicate batch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, replicateEndpoint(addr), bytes.NewReader(body))
	c.observe("replicate_batch", "build_request", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("build replicate batch request: %w", err)
	}
	req.Header.Set("Content-Type", replicationwire.BatchContentType)
	req.Header.Set(replicationwire.HeaderTopic, topic)
	req.Header.Set(replicationwire.HeaderPartition, strconv.Itoa(partition))
	req.Header.Set(replicationwire.HeaderOffset, strconv.FormatInt(records[0].Offset, 10))
	req.Header.Set(replicationwire.HeaderLeaderID, c.selfID)
	req.Header.Set(replicationwire.HeaderRecordCount, strconv.Itoa(len(records)))

	stageStart = time.Now()
	resp, err := c.client.Do(req)
	c.observe("replicate_batch", "send_request", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("send replicate batch request: %w", err)
	}
	defer resp.Body.Close()
	stageStart = time.Now()
	_, err = io.Copy(io.Discard, resp.Body)
	c.observe("replicate_batch", "drain_response", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		return 0, nil, fmt.Errorf("drain replicate batch response: %w", err)
	}

	return resp.StatusCode, resp.Header.Clone(), nil
}

func (c Cluster) observe(operation, stage, outcome string, duration time.Duration) {
	if c.metrics == nil {
		return
	}
	c.metrics.ObserveHotPathStage("replication", operation, stage, outcome, duration)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func replicaNextOffset(header http.Header) (int64, bool) {
	raw := header.Get(replicationwire.HeaderReplicaNextOffset)
	if raw == "" {
		return 0, false
	}
	next, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || next < 0 {
		return 0, false
	}
	return next, true
}

var (
	_ Replicator        = Cluster{}
	_ BatchReplicator   = Cluster{}
	_ CatchUpReplicator = Cluster{}
)
