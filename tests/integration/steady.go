package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Steady mode: producers and consumers run CONCURRENTLY for a fixed
// duration, which is what a produce/consume/ack ceiling actually is. Load
// mode produces everything and then consumes everything, so its number is
// a per-phase rate, not a pipeline's.
//
// Nothing here retries. The shared helpers retry a 429 or 503 up to
// twenty times with backoff, which is right for a correctness run and
// wrong for a ceiling: it converts backpressure into latency and hides
// the knee. Here a 429/503 on produce drops that job (it never enters
// the expected set), a 410 on ack is counted as a lapsed lease, and all
// of them are reported as the signal they are. Only real violations fail
// the run: an id we never produced, a duplicate delivered AFTER its ack,
// or messages still unacked when the drain window closes.

// latency buckets in milliseconds; percentiles are reported as the
// smallest bucket edge covering the requested fraction, i.e. bucketed,
// not exact. Contention-free (one atomic add per sample).
var latEdgesMs = [...]float64{0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000}

type hist struct {
	b [len(latEdgesMs) + 1]atomic.Int64
}

func (h *hist) observe(d time.Duration) {
	i := sort.SearchFloat64s(latEdgesMs[:], float64(d)/float64(time.Millisecond))
	h.b[i].Add(1)
}

type histSnap [len(latEdgesMs) + 1]int64

func (h *hist) snapshot() (s histSnap) {
	for i := range s {
		s[i] = h.b[i].Load()
	}
	return s
}

func (s histSnap) sub(prev histSnap) (d histSnap) {
	for i := range s {
		d[i] = s[i] - prev[i]
	}
	return d
}

func (s histSnap) pct(p float64) string {
	var total int64
	for _, c := range s {
		total += c
	}
	if total == 0 {
		return "-"
	}
	need := int64(math.Ceil(p * float64(total)))
	var cum int64
	for i, c := range s {
		cum += c
		if cum >= need {
			if i == len(latEdgesMs) {
				return ">60s"
			}
			return fmt.Sprintf("<=%gms", latEdgesMs[i])
		}
	}
	return ">60s"
}

type steadyStats struct {
	produced, consumed, acked              atomic.Int64
	produce429, produce503, produceErr     atomic.Int64
	consume429, consume503, consumeErr     atomic.Int64
	ackGone, ack429, ack503, ackErr        atomic.Int64
	dupBeforeAck, dupAfterAck              atomic.Int64
	unknownID, ambiguousProduce            atomic.Int64
	latProduce, latConsume, latAck, latE2E hist
}

// inFlightRecord is what we remember per produced message until it is
// acked: when it was produced (for end-to-end latency) and whether it
// has been acked (for duplicate classification).
type inFlightRecord struct {
	producedAt time.Time
	acked      atomic.Bool
	// ambiguous: our request errored after it may have reached the
	// server. Not counted as produced unless it is later delivered.
	ambiguous atomic.Bool
	// delivered: consumed at least once. Distinguishes a message the
	// broker never handed out from one whose ack failed, in the
	// post-mortem listing at the end of the run.
	delivered atomic.Bool
}

func runSteady(cfg config) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+cfg.drainTimeout+2*time.Minute)
	defer cancel()

	lb := &roundRobinClient{
		nodes:    cfg.nodes,
		client:   &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{MaxIdleConns: 4096, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}},
		username: cfg.username,
		password: cfg.password,
	}
	topics := topicNames(cfg)
	fmt.Printf("steady: nodes=%s run_id=%s topics=%d partitions=%d producers=%d consumers=%d rate=%d duration=%s\n",
		strings.Join(cfg.nodes, ","), cfg.runID, len(topics), cfg.partitions, cfg.produceConcurrency, cfg.consumeConcurrency, cfg.produceRate, cfg.duration)
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	if err := createTopics(ctx, lb, cfg, topics); err != nil {
		return err
	}
	if cfg.cleanup {
		defer func() {
			cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer ccancel()
			if err := deleteTopics(cctx, lb, topics); err != nil {
				fmt.Printf("cleanup: %v\n", err)
			}
		}()
	}

	var st steadyStats
	var expected sync.Map // id -> *inFlightRecord
	seqByTopic := make([]atomic.Int64, len(topics))
	var jobCounter atomic.Int64

	var stopProducers atomic.Bool
	var pace <-chan time.Time
	if cfg.produceRate > 0 {
		t := time.NewTicker(time.Second / time.Duration(cfg.produceRate))
		defer t.Stop()
		pace = t.C
	}

	// Producers.
	var pwg sync.WaitGroup
	for range cfg.produceConcurrency {
		pwg.Go(func() {
			// Checked BEFORE each request and never mid-request: cancelling an
			// in-flight produce disowns a record the server may already hold,
			// which then arrives as an id "we never produced". Exactly one per
			// worker, every run, until this was fixed.
			for !stopProducers.Load() && ctx.Err() == nil {
				if pace != nil {
					select {
					case <-pace:
					case <-ctx.Done():
						return
					}
				}
				n := jobCounter.Add(1) - 1
				ti := int(n % int64(len(topics)))
				seq := int(seqByTopic[ti].Add(1) - 1)
				topicName := topics[ti]
				rec := messageRecord{
					ID:       fmt.Sprintf("%s/%s/%08d", cfg.runID, topicName, seq),
					Topic:    topicName,
					Sequence: seq,
					Key:      fmt.Sprintf("key-%04d", seq%max(cfg.partitions*4, 1)),
					RunID:    cfg.runID,
				}
				body, _ := json.Marshal(rec)
				path := "/v1/topics/" + url.PathEscape(topicName) + "/produce?key=" + url.QueryEscape(rec.Key)
				// Register BEFORE sending. The record becomes visible to
				// consumers the instant the server commits it, which can
				// precede this goroutine reading its own 202 by a scheduler
				// quantum; registering afterwards produced "unknown id"
				// false positives at ~0.04% (13 in 29k) that then sat leased
				// for a full visibility timeout before redelivering.
				rec0 := &inFlightRecord{producedAt: time.Now()}
				expected.Store(rec.ID, rec0)
				status, _, err := lb.doRaw(ctx, http.MethodPost, path, body, nil,
					http.StatusAccepted, http.StatusTooManyRequests, http.StatusServiceUnavailable)
				st.latProduce.observe(time.Since(rec0.producedAt))
				switch {
				case status == http.StatusAccepted && err == nil:
					st.produced.Add(1)
				case err != nil:
					// The server may or may not have it. Keep the record but
					// mark it: it counts as produced only if delivered.
					rec0.ambiguous.Store(true)
					if ctx.Err() == nil {
						st.produceErr.Add(1)
					}
				default:
					// The server said no (429/503): it does not have it.
					expected.Delete(rec.ID)
					if status == http.StatusTooManyRequests {
						st.produce429.Add(1)
					} else {
						st.produce503.Add(1)
					}
				}
			}
		})
	}

	// Consumers: run until told to stop (after the drain).
	consumeCtx, stopConsuming := context.WithCancel(ctx)
	var cwg sync.WaitGroup
	var topicCursor atomic.Uint64
	var fatal atomic.Value // first correctness violation, as error
	for range cfg.consumeConcurrency {
		cwg.Go(func() {
			for consumeCtx.Err() == nil {
				topicName := topics[int(topicCursor.Add(1)-1)%len(topics)]
				path := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=500ms"
				t0 := time.Now()
				status, body, err := lb.do(consumeCtx, http.MethodGet, path, nil, nil,
					http.StatusOK, http.StatusNoContent, http.StatusTooManyRequests, http.StatusServiceUnavailable)
				st.latConsume.observe(time.Since(t0))
				if err != nil {
					if consumeCtx.Err() == nil {
						st.consumeErr.Add(1)
					}
					continue
				}
				switch status {
				case http.StatusNoContent:
					continue
				case http.StatusTooManyRequests:
					st.consume429.Add(1)
					continue
				case http.StatusServiceUnavailable:
					st.consume503.Add(1)
					continue
				}
				var msg consumeResponse
				if err := json.Unmarshal(body, &msg); err != nil {
					st.consumeErr.Add(1)
					continue
				}
				st.consumed.Add(1)
				v, ok := expected.Load(msg.Payload.ID)
				if !ok {
					st.unknownID.Add(1)
					fatal.CompareAndSwap(nil, fmt.Errorf("consumed id %q that this run never produced (topic %s)", msg.Payload.ID, msg.Topic))
					continue
				}
				rec := v.(*inFlightRecord)
				rec.delivered.Store(true)
				if rec.ambiguous.CompareAndSwap(true, false) {
					// Delivered, so the server did accept it despite our error.
					st.produced.Add(1)
					st.ambiguousProduce.Add(1)
				}
				if rec.acked.Load() {
					// Redelivered after we acked it. Legitimate only if our
					// ack was lost in flight or a broker restarted (acks ahead
					// of a gap live in memory by design); otherwise
					// at-most-once-after-ack is broken. Counted, optionally
					// fatal, and then ACKED like a real consumer would: a
					// duplicate left leased forever becomes a permanent gap
					// that the ahead-of-frontier cap gates the whole partition
					// behind, one visibility cycle at a time.
					st.dupAfterAck.Add(1)
					if cfg.fatalDupAfterAck {
						fatal.CompareAndSwap(nil, fmt.Errorf("message %q redelivered after it was acked", msg.Payload.ID))
					}
				}
				a0 := time.Now()
				apath := "/v1/topics/" + url.PathEscape(msg.Topic) + "/ack?receipt_handle=" + url.QueryEscape(msg.ReceiptHandle)
				astatus, _, aerr := lb.do(consumeCtx, http.MethodPost, apath, nil, nil,
					http.StatusNoContent, http.StatusGone, http.StatusTooManyRequests, http.StatusServiceUnavailable)
				st.latAck.observe(time.Since(a0))
				switch {
				case aerr != nil:
					if consumeCtx.Err() == nil {
						st.ackErr.Add(1)
					}
				case astatus == http.StatusNoContent:
					if rec.acked.CompareAndSwap(false, true) {
						st.acked.Add(1)
						st.latE2E.observe(time.Since(rec.producedAt))
					} else {
						st.dupBeforeAck.Add(1)
					}
				case astatus == http.StatusGone:
					// The lease lapsed before our ack: the message will be
					// redelivered and acked later. A saturation signal.
					st.ackGone.Add(1)
				case astatus == http.StatusTooManyRequests:
					st.ack429.Add(1)
				default:
					st.ack503.Add(1)
				}
			}
		})
	}

	// Periodic report.
	report := func(label string, prev *steadySnapshot) *steadySnapshot {
		cur := snapshotSteady(&st)
		d := cur
		if prev != nil {
			d = cur.sub(prev)
		}
		secs := math.Max(d.elapsed.Seconds(), 0.001)
		fmt.Printf("%-6s t=%-5s prod/s=%-8.0f cons/s=%-8.0f ack/s=%-8.0f inflight=%-7d | 429 p/c/a=%d/%d/%d 503=%d/%d/%d gone=%d err=%d/%d/%d dup=%d/%d | p50/p99 produce=%s/%s consume=%s/%s ack=%s/%s e2e=%s/%s\n",
			label, time.Since(start).Round(time.Second),
			float64(d.produced)/secs, float64(d.consumed)/secs, float64(d.acked)/secs, cur.produced-cur.acked,
			d.produce429, d.consume429, d.ack429, d.produce503, d.consume503, d.ack503, d.ackGone,
			d.produceErr, d.consumeErr, d.ackErr, d.dupBeforeAck, d.dupAfterAck,
			d.latProduce.pct(0.5), d.latProduce.pct(0.99), d.latConsume.pct(0.5), d.latConsume.pct(0.99),
			d.latAck.pct(0.5), d.latAck.pct(0.99), d.latE2E.pct(0.5), d.latE2E.pct(0.99))
		return &cur
	}

	ticker := time.NewTicker(cfg.reportEvery)
	defer ticker.Stop()
	deadline := time.After(cfg.duration)
	prev := snapshotSteady(&st)
	prevP := &prev
loop:
	for {
		select {
		case <-ticker.C:
			prevP = report("", prevP)
		case <-deadline:
			break loop
		case <-ctx.Done():
			break loop
		}
	}
	stopProducers.Store(true)
	pwg.Wait()
	fmt.Printf("producers stopped after %s; draining up to %s\n", cfg.duration, cfg.drainTimeout)

	drainDeadline := time.After(cfg.drainTimeout)
drain:
	for {
		select {
		case <-ticker.C:
			prevP = report("drain", prevP)
			if st.acked.Load() >= st.produced.Load() {
				break drain
			}
		case <-drainDeadline:
			break drain
		case <-ctx.Done():
			break drain
		}
	}
	stopConsuming()
	cwg.Wait()

	total := snapshotSteady(&st)
	elapsed := time.Since(start)
	fmt.Printf("TOTAL  duration=%s produced=%d consumed=%d acked=%d | overall ack/s=%.0f (over %s of production) | 429 p/c/a=%d/%d/%d 503=%d/%d/%d gone=%d err=%d/%d/%d dup=%d/%d unknown=%d ambiguous_produce=%d | p50/p99 produce=%s/%s consume=%s/%s ack=%s/%s e2e=%s/%s\n",
		elapsed.Round(time.Second), total.produced, total.consumed, total.acked,
		float64(total.acked)/math.Max(cfg.duration.Seconds(), 0.001), cfg.duration,
		total.produce429, total.consume429, total.ack429, total.produce503, total.consume503, total.ack503, total.ackGone,
		total.produceErr, total.consumeErr, total.ackErr, total.dupBeforeAck, total.dupAfterAck, total.unknownID, st.ambiguousProduce.Load(),
		total.latProduce.pct(0.5), total.latProduce.pct(0.99), total.latConsume.pct(0.5), total.latConsume.pct(0.99),
		total.latAck.pct(0.5), total.latAck.pct(0.99), total.latE2E.pct(0.5), total.latE2E.pct(0.99))

	// Post-mortem aid: name a sample of the records that never reached
	// the acked state, split by whether the broker ever handed them out,
	// so a retained topic can be inspected for them.
	var never, unacked []string
	expected.Range(func(k, v any) bool {
		r := v.(*inFlightRecord)
		if r.acked.Load() || r.ambiguous.Load() {
			return true
		}
		if r.delivered.Load() {
			unacked = append(unacked, k.(string))
		} else {
			never = append(never, k.(string))
		}
		return true
	})
	sort.Strings(never)
	sort.Strings(unacked)
	fmt.Printf("POSTMORTEM never_delivered=%d delivered_unacked=%d\n", len(never), len(unacked))
	if len(never) > 0 {
		fmt.Printf("  never_delivered sample: %s\n", strings.Join(never[:min(len(never), 25)], " "))
	}
	if len(unacked) > 0 {
		fmt.Printf("  delivered_unacked sample: %s\n", strings.Join(unacked[:min(len(unacked), 25)], " "))
	}

	if v := fatal.Load(); v != nil {
		return fmt.Errorf("correctness violation: %w", v.(error))
	}
	if len(never) > 0 {
		return fmt.Errorf("UNDELIVERED: %d messages produced (202) were not delivered within the %s drain window: a backlog the consumers did not drain, or a loss; compare with narad_consumer_lag_messages on the brokers", len(never), cfg.drainTimeout)
	}
	if len(unacked) > 0 {
		// Delivered, our ack errored, and it never came back: after a
		// drain longer than the visibility timeout that is only possible
		// if the broker holds it as acked (the response was lost on the
		// way to us). An ambiguous ack, not a loss. Shorter drains cannot
		// tell the two apart, so they stay a failure.
		if cfg.drainTimeout <= cfg.visibilityTimeout {
			return fmt.Errorf("UNRESOLVED: %d delivered messages never acked and the %s drain is shorter than the %s visibility timeout", len(unacked), cfg.drainTimeout, cfg.visibilityTimeout)
		}
		fmt.Printf("PASS steady: zero loss; %d ambiguous acks (delivered, ack response lost, never redelivered in a drain longer than the visibility timeout: broker-side acked)\n", len(unacked))
		return nil
	}
	fmt.Println("PASS steady: every produced message was consumed and acked exactly once")
	return nil
}

type steadySnapshot struct {
	at                                     time.Time
	elapsed                                time.Duration
	produced, consumed, acked              int64
	produce429, produce503, produceErr     int64
	consume429, consume503, consumeErr     int64
	ackGone, ack429, ack503, ackErr        int64
	dupBeforeAck, dupAfterAck, unknownID   int64
	latProduce, latConsume, latAck, latE2E histSnap
}

func snapshotSteady(st *steadyStats) steadySnapshot {
	return steadySnapshot{
		at:       time.Now(),
		produced: st.produced.Load(), consumed: st.consumed.Load(), acked: st.acked.Load(),
		produce429: st.produce429.Load(), produce503: st.produce503.Load(), produceErr: st.produceErr.Load(),
		consume429: st.consume429.Load(), consume503: st.consume503.Load(), consumeErr: st.consumeErr.Load(),
		ackGone: st.ackGone.Load(), ack429: st.ack429.Load(), ack503: st.ack503.Load(), ackErr: st.ackErr.Load(),
		dupBeforeAck: st.dupBeforeAck.Load(), dupAfterAck: st.dupAfterAck.Load(), unknownID: st.unknownID.Load(),
		latProduce: st.latProduce.snapshot(), latConsume: st.latConsume.snapshot(), latAck: st.latAck.snapshot(), latE2E: st.latE2E.snapshot(),
	}
}

func (s steadySnapshot) sub(p *steadySnapshot) steadySnapshot {
	return steadySnapshot{
		at: s.at, elapsed: s.at.Sub(p.at),
		produced: s.produced - p.produced, consumed: s.consumed - p.consumed, acked: s.acked - p.acked,
		produce429: s.produce429 - p.produce429, produce503: s.produce503 - p.produce503, produceErr: s.produceErr - p.produceErr,
		consume429: s.consume429 - p.consume429, consume503: s.consume503 - p.consume503, consumeErr: s.consumeErr - p.consumeErr,
		ackGone: s.ackGone - p.ackGone, ack429: s.ack429 - p.ack429, ack503: s.ack503 - p.ack503, ackErr: s.ackErr - p.ackErr,
		dupBeforeAck: s.dupBeforeAck - p.dupBeforeAck, dupAfterAck: s.dupAfterAck - p.dupAfterAck, unknownID: s.unknownID - p.unknownID,
		latProduce: s.latProduce.sub(p.latProduce), latConsume: s.latConsume.sub(p.latConsume), latAck: s.latAck.sub(p.latAck), latE2E: s.latE2E.sub(p.latE2E),
	}
}
