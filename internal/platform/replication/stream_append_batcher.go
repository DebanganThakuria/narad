package replication

import (
	"context"
	"fmt"
	"time"

	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

const (
	streamAppendBatchMaxGroups  = 256
	streamAppendBatchMaxRecords = 2048
	streamAppendBatchMaxBytes   = 2 << 20
	streamAppendBatchLinger     = 2 * time.Millisecond
)

type streamAppendBatcher struct {
	cluster *StreamingCluster
	addr    string

	wakeup chan struct{}
	queue  chan streamAppendBatchJob
}

type streamAppendBatchJob struct {
	ctx     context.Context
	records []Record
	group   replicationwire.StreamAppendGroup
	done    chan streamAppendBatchResult
}

type streamAppendBatchResult struct {
	next int64
	err  error
}

func (c *StreamingCluster) appendBatcher(addr string) *streamAppendBatcher {
	c.appendBatchersMu.Lock()
	defer c.appendBatchersMu.Unlock()

	if batcher := c.appendBatchers[addr]; batcher != nil {
		return batcher
	}
	batcher := &streamAppendBatcher{
		cluster: c,
		addr:    addr,
		wakeup:  make(chan struct{}, 1),
		queue:   make(chan streamAppendBatchJob, 4096),
	}
	go batcher.run()
	c.appendBatchers[addr] = batcher
	return batcher
}

func (b *streamAppendBatcher) append(ctx context.Context, job streamAppendBatchJob) (int64, error) {
	if len(job.records) == 0 {
		return 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	job.ctx = ctx
	job.done = make(chan streamAppendBatchResult, 1)

	select {
	case b.queue <- job:
		b.signal()
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	select {
	case result := <-job.done:
		return result.next, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *streamAppendBatcher) signal() {
	select {
	case b.wakeup <- struct{}{}:
	default:
	}
}

func (b *streamAppendBatcher) run() {
	pending := make([]streamAppendBatchJob, 0, streamAppendBatchMaxGroups)
	for {
		<-b.wakeup
		for {
			pending = b.drainQueue(pending)
			batch, rest := b.takeBatch(pending)
			pending = rest
			if len(batch) == 0 {
				break
			}
			b.send(batch)
		}
	}
}

func (b *streamAppendBatcher) drainQueue(pending []streamAppendBatchJob) []streamAppendBatchJob {
	timer := time.NewTimer(streamAppendBatchLinger)
	defer stopStreamAppendBatchTimer(timer)

	for {
		if streamAppendBatchReady(pending) {
			return pending
		}
		select {
		case job := <-b.queue:
			pending = append(pending, job)
		case <-timer.C:
			return pending
		default:
			select {
			case job := <-b.queue:
				pending = append(pending, job)
			case <-timer.C:
				return pending
			}
		}
	}
}

func streamAppendBatchReady(jobs []streamAppendBatchJob) bool {
	if len(jobs) >= streamAppendBatchMaxGroups {
		return true
	}
	var records, bytes int
	for _, job := range jobs {
		records += len(job.group.Payloads)
		for _, payload := range job.group.Payloads {
			bytes += len(payload)
		}
		if records >= streamAppendBatchMaxRecords || bytes >= streamAppendBatchMaxBytes {
			return true
		}
	}
	return false
}

func (b *streamAppendBatcher) takeBatch(jobs []streamAppendBatchJob) ([]streamAppendBatchJob, []streamAppendBatchJob) {
	batch := make([]streamAppendBatchJob, 0, min(len(jobs), streamAppendBatchMaxGroups))
	n := 0
	records := 0
	bytes := 0
	for n < len(jobs) && len(batch) < streamAppendBatchMaxGroups {
		job := jobs[n]
		n++
		if err := job.ctx.Err(); err != nil {
			job.complete(0, err)
			continue
		}

		jobRecords := len(job.group.Payloads)
		jobBytes := streamAppendPayloadBytes(job.group.Payloads)
		if len(batch) > 0 && (records+jobRecords > streamAppendBatchMaxRecords || bytes+jobBytes > streamAppendBatchMaxBytes) {
			n--
			break
		}
		batch = append(batch, job)
		records += jobRecords
		bytes += jobBytes
	}
	return batch, jobs[n:]
}

func (b *streamAppendBatcher) send(batch []streamAppendBatchJob) {
	groups := make([]replicationwire.StreamAppendGroup, len(batch))
	for i, job := range batch {
		groups[i] = job.group
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.cluster.timeout)
	defer cancel()

	start := time.Now()
	results, err := b.cluster.quic.appendMulti(ctx, b.addr, groups)
	b.cluster.observe("replicate_batch", "append_multi_batch", observeOutcome(err), time.Since(start))
	if err != nil {
		for _, job := range batch {
			job.complete(0, err)
		}
		return
	}
	if len(results) != len(batch) {
		err := fmt.Errorf("replication stream returned %d append results for %d groups", len(results), len(batch))
		for _, job := range batch {
			job.complete(0, err)
		}
		return
	}
	for i, job := range batch {
		next, err := streamResultForRecords(job.records, results[i])
		job.complete(next, err)
	}
}

func (j streamAppendBatchJob) complete(next int64, err error) {
	select {
	case j.done <- streamAppendBatchResult{next: next, err: err}:
	default:
	}
}

func streamAppendPayloadBytes(payloads [][]byte) int {
	var total int
	for _, payload := range payloads {
		total += len(payload)
	}
	return total
}

func stopStreamAppendBatchTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
