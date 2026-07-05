package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// commitBuckets commits each bucket concurrently with bounded fan-out and
// marks every record of a successful bucket done. Failed buckets are
// returned so the caller can decide between retrying them on their original
// partition and rerouting them. The done map and the returned failures are
// produced only after all commits finish, so the parallel phase has no
// shared writes.
func (d *ProduceDispatcher) commitBuckets(ctx context.Context, buckets map[produceDispatchTarget][]ingress.ProduceRecord, done map[uint64]bool) (map[produceDispatchTarget][]ingress.ProduceRecord, error) {
	if len(buckets) == 0 {
		return nil, nil
	}
	type job struct {
		target produceDispatchTarget
		recs   []ingress.ProduceRecord
	}
	jobs := make([]job, 0, len(buckets))
	for target, recs := range buckets {
		jobs = append(jobs, job{target: target, recs: recs})
	}

	results := make([]error, len(jobs))
	sem := make(chan struct{}, max(d.commitConcurrency, 1))
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = d.commitBatch(ctx, jobs[i].target, jobs[i].recs)
		}(i)
	}
	wg.Wait()

	var failed map[produceDispatchTarget][]ingress.ProduceRecord
	var firstErr error
	for i, j := range jobs {
		if results[i] == nil {
			for _, r := range j.recs {
				done[r.WAL.Seq] = true
			}
			continue
		}
		if failed == nil {
			failed = make(map[produceDispatchTarget][]ingress.ProduceRecord)
		}
		failed[j.target] = j.recs
		if firstErr == nil {
			firstErr = results[i]
		}
	}
	return failed, firstErr
}

// commitBatch dispatches a batch. If the commit fails AND the topic is
// genuinely gone from this node's metastore replica, the records are
// DISCARDED (returns nil so the caller advances the WAL checkpoint past
// them) — a topic deleted while it still had undispatched WAL records is
// the motivating case; without this the dispatch window would block on records
// that can never commit.
//
// The discard decision keys off this node's own replica, never the commit
// error itself. That is the safe signal: a record only reached this WAL
// because AcceptProduce saw the topic in this replica, and Raft replicas
// only move forward — so if the topic is now absent here, a delete was
// truly applied (it cannot be create-replication lag). Any other failure
// (transient network, a lagging remote owner returning 404 for a live
// topic, owner moved, malformed record) is returned so the caller retries
// rather than silently dropping data.
func (d *ProduceDispatcher) commitBatch(ctx context.Context, target produceDispatchTarget, records []ingress.ProduceRecord) error {
	err := d.dispatchRecordBatch(ctx, target, records)
	if err == nil || ctx.Err() != nil {
		return err
	}
	if d.topicDeletedLocally(target.topic) {
		d.logger.Warn("discarding undispatched produce records for deleted topic",
			"topic", target.topic, "partition", target.partition,
			"records", len(records), "err", err)
		return nil
	}
	return err
}

// topicDeletedLocally reports whether the topic is absent from this node's
// local metastore replica.
func (d *ProduceDispatcher) topicDeletedLocally(topicName string) bool {
	if d.store == nil {
		return false
	}
	_, err := d.store.GetTopic(context.Background(), topicName)
	return errors.Is(err, errs.ErrNotFound)
}

func (d *ProduceDispatcher) dispatchRecordBatch(ctx context.Context, target produceDispatchTarget, records []ingress.ProduceRecord) error {
	if len(records) == 0 {
		return nil
	}
	if target.local {
		return d.commitLocal(ctx, records)
	}
	return d.commitRemote(ctx, target.addr, records)
}

func (d *ProduceDispatcher) commitLocal(ctx context.Context, records []ingress.ProduceRecord) error {
	if d.committer == nil {
		return errors.New("produce dispatcher committer is nil")
	}
	if batcher, ok := d.committer.(produceBatchCommitter); ok {
		_, err := batcher.CommitAcceptedProduceBatch(ctx, records)
		return err
	}
	for _, record := range records {
		if _, err := d.committer.CommitAcceptedProduce(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (d *ProduceDispatcher) commitRemote(ctx context.Context, addr string, records []ingress.ProduceRecord) error {
	if d.peer == nil {
		return errors.New("produce dispatcher peer client is nil")
	}
	req := nodewire.CommitProduceBatchRequest{Records: make([]nodewire.CommitProduceRequest, 0, len(records))}
	for _, record := range records {
		req.Records = append(req.Records, nodewire.CommitProduceRequest{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	// Explicit deadline: without one the transport's short default reply
	// timeout applies, and a slow-but-successful remote commit would be
	// re-committed as duplicates (see produceCommitRPCTimeout).
	rpcCtx, cancel := context.WithTimeout(ctx, produceCommitRPCTimeout)
	defer cancel()
	res, err := d.peer.CommitProduceBatch(rpcCtx, addr, req)
	if err != nil {
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("commit produce batch returned status %d", res.Status)
	}
	return nil
}
