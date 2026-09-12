package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Soak mode: a workload shaped like a company's production traffic
// rather than a uniform benchmark.
//
// A benchmark produces and consumes one topic shape as fast as it can,
// which is the one traffic pattern no real deployment has. Nothing in it
// ever sits idle long enough for retention to matter, no consumer is
// slow enough to ack out of order, and no topic is quiet enough to test
// the cold paths. The features this driver is meant to keep honest live
// exactly there.
//
// So the profiles below model a payments company: a couple of firehose
// topics with fast handlers, an outbound-webhook topic whose handlers
// call someone else's flaky API, a low-volume refund topic with slow
// handlers, a periodic settlement batch with fat payloads, an audit
// topic nobody reads promptly, and a config topic that is idle for
// minutes at a time. Which in turn exercises:
//
//   - out-of-order acks, hence the persisted acked-ahead set: many
//     consumers per partition with uneven handler latency
//   - redelivery and the visibility timeout: handlers that nack, and a
//     small fraction that never ack at all
//   - retention on an idle topic, hence the cold-partition walk: a topic
//     with an hour of retention and a message every few minutes
//   - retention expiry under write pressure: an audit topic with a short
//     retention and a consumer that falls behind on purpose
//   - the cross-node consume path: every request goes through the load
//     balancer, so consumers routinely land on a node that owns nothing
//     they want
//
// Verification is bounded so this can run for days: per-message state is
// dropped once acked, aged out on a sweep, and duplicate detection keeps
// a fixed-size window of recently acked sequences.

// soakEpoch identifies this process, so a restart cannot confuse its
// predecessor's messages with its own.
var soakEpoch = time.Now().UnixNano()

const (
	// soakLostAfter is how long a produced message may go undelivered
	// before it is counted as lost. It has to clear the slowest
	// profile's visibility timeout several times over, or a message
	// parked behind one stuck lease would be reported as loss.
	soakLostAfter = 30 * time.Minute
	// soakPendingCap bounds the per-topic outstanding table. Reaching it
	// means consumers are far behind, which the overflow counter says
	// out loud rather than the process dying of memory.
	soakPendingCap = 400_000
	// soakAckedWindow is how many recently acked sequences each topic
	// remembers for redelivery-after-ack detection.
	soakAckedWindow = 100_000
)

// soakProfile is one topic's traffic shape.
type soakProfile struct {
	name         string
	partitions   int
	retention    time.Duration
	visibility   time.Duration
	rate         float64 // messages per second, per driver pod
	payloadBytes int
	keys         int
	consumers    int
	consumeWait  time.Duration
	// minWork/maxWork bracket how long a handler pretends to work.
	minWork, maxWork time.Duration
	// nackRate returns messages to the queue (a handler that failed and
	// wants a retry); poisonRate never acks at all (a handler that hung
	// or crashed), which is what strands a lease at the frontier.
	nackRate, poisonRate float64
	// burstEvery, when set, replaces the steady rate with one batch of
	// rate*burstEvery messages on that interval: a nightly or hourly job.
	burstEvery time.Duration
	// idleBetweenPolls makes the consumer sleep between drains, so the
	// topic accumulates a backlog between them the way a periodic batch
	// consumer does. The drain that follows is continuous, so the group
	// still keeps up on average.
	idleBetweenPolls time.Duration
	// backlogByDesign marks a profile whose backlog is the point. Aged
	// entries there are reported separately instead of as the loss
	// signal, because a consumer that is behind on purpose can lose a
	// race with retention without anything being wrong.
	backlogByDesign bool
}

// soakProfiles is the modelled company. Rates are per driver pod, so N
// pods multiply them; the totals here come to roughly 1000 messages a
// second with one pod.
//
// Retentions are hours, not days, for two reasons. A soak that runs for
// months has to be a good neighbour on a shared volume: at these rates
// the steady state is a few gigabytes across the cluster, and every hour
// added to a firehose topic costs about half a gigabyte more. And a
// short retention exercises expiry constantly, which is the point, where
// a long one would mostly measure how fast a disk fills.
var soakProfiles = []soakProfile{
	{
		// The firehose: every authorization, handled by a fast service.
		name: "soak-payments-authorized", partitions: 12,
		retention: 2 * time.Hour, visibility: 30 * time.Second,
		rate: 400, payloadBytes: 420, keys: 5000,
		consumers: 8, consumeWait: 2 * time.Second,
		minWork: time.Millisecond, maxWork: 6 * time.Millisecond,
	},
	{
		name: "soak-payments-captured", partitions: 12,
		retention: 2 * time.Hour, visibility: 30 * time.Second,
		rate: 250, payloadBytes: 420, keys: 5000,
		consumers: 6, consumeWait: 2 * time.Second,
		minWork: time.Millisecond, maxWork: 8 * time.Millisecond,
	},
	{
		// Outbound webhooks: someone else's HTTP endpoint, so handlers
		// are slow, uneven, and sometimes give up. This is the profile
		// that fills the acked-ahead set and strands leases.
		name: "soak-webhooks-outbound", partitions: 6,
		retention: time.Hour, visibility: 60 * time.Second,
		rate: 200, payloadBytes: 1100, keys: 2000,
		// 16 handlers per pod against a 180ms average: capacity has to
		// clear the rate or the backlog only ever grows.
		consumers: 16, consumeWait: 2 * time.Second,
		minWork: 40 * time.Millisecond, maxWork: 320 * time.Millisecond,
		nackRate: 0.03, poisonRate: 0.002,
	},
	{
		// Refunds: low volume, slow handlers talking to a bank.
		name: "soak-refunds-initiated", partitions: 3,
		retention: 4 * time.Hour, visibility: 120 * time.Second,
		rate: 40, payloadBytes: 640, keys: 800,
		consumers: 8, consumeWait: 5 * time.Second,
		minWork: 200 * time.Millisecond, maxWork: 600 * time.Millisecond,
		nackRate: 0.01,
	},
	{
		// Settlement batches: idle, then a pile of fat records at once.
		name: "soak-settlements-batch", partitions: 3,
		retention: 6 * time.Hour, visibility: 300 * time.Second,
		rate: 2, payloadBytes: 8200, keys: 200,
		consumers: 2, consumeWait: 5 * time.Second,
		minWork: 100 * time.Millisecond, maxWork: 900 * time.Millisecond,
		burstEvery: 5 * time.Minute,
	},
	{
		// Audit: written constantly, read lazily, kept an hour. The
		// backlog and the retention sweep meet here.
		name: "soak-audit-events", partitions: 6,
		retention: time.Hour, visibility: 30 * time.Second,
		rate: 100, payloadBytes: 300, keys: 3000,
		consumers: 4, consumeWait: 2 * time.Second,
		minWork: time.Millisecond, maxWork: 4 * time.Millisecond,
		idleBetweenPolls: 20 * time.Second, backlogByDesign: true,
	},
	{
		// Config changes: minutes of silence. Its partitions go cold, so
		// only the cold-retention walk can ever expire them.
		name: "soak-config-changes", partitions: 3,
		retention: time.Hour, visibility: 30 * time.Second,
		rate: 1.0 / 180.0, payloadBytes: 220, keys: 50,
		consumers: 1, consumeWait: 10 * time.Second,
		minWork: time.Millisecond, maxWork: 5 * time.Millisecond,
	},
}

// soakRecord is the payload. Pod and seq identify a message across the
// pods sharing these topics; at carries the produce time so a consumer
// can measure end to end without a shared clock beyond the cluster's.
type soakRecord struct {
	Pod string `json:"pod"`
	// Epoch is the producing process's start time. Sequences restart at
	// one when a soak process does, and its predecessor's messages are
	// still in the backlog: without the epoch a restarted process sees
	// its own sequence numbers coming back and reports them as
	// redelivery after ack, which is a lie about the broker.
	Epoch  int64  `json:"epoch"`
	Seq    uint64 `json:"seq"`
	At     int64  `json:"at"`
	Filler string `json:"filler,omitempty"`
}

type soakPending struct {
	producedAt  time.Time
	deliveredAt time.Time
}

// soakTracker is one topic's bounded verification state.
type soakTracker struct {
	mu      sync.Mutex
	pending map[uint64]*soakPending
	// ackedRing remembers the last soakAckedWindow acked sequences, so a
	// redelivery after ack is caught without keeping every sequence ever
	// produced.
	ackedRing []uint64
	ackedSet  map[uint64]struct{}
	ackedAt   int
}

func newSoakTracker() *soakTracker {
	return &soakTracker{
		pending:   make(map[uint64]*soakPending),
		ackedRing: make([]uint64, soakAckedWindow),
		ackedSet:  make(map[uint64]struct{}, soakAckedWindow),
	}
}

func (t *soakTracker) produced(seq uint64, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) >= soakPendingCap {
		return false
	}
	t.pending[seq] = &soakPending{producedAt: now}
	return true
}

// delivered records a delivery and reports whether this sequence was
// already acked (a redelivery after ack) and the produce-to-delivery
// latency when it is known.
func (t *soakTracker) delivered(seq uint64, now time.Time) (dupAfterAck bool, e2e time.Duration, known bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.ackedSet[seq]; ok {
		return true, 0, true
	}
	p, ok := t.pending[seq]
	if !ok {
		return false, 0, false
	}
	if p.deliveredAt.IsZero() {
		p.deliveredAt = now
		return false, now.Sub(p.producedAt), true
	}
	return false, 0, true
}

func (t *soakTracker) acked(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, seq)
	if old := t.ackedRing[t.ackedAt]; old != 0 {
		delete(t.ackedSet, old)
	}
	t.ackedRing[t.ackedAt] = seq
	t.ackedSet[seq] = struct{}{}
	t.ackedAt = (t.ackedAt + 1) % len(t.ackedRing)
}

// sweep ages out entries older than soakLostAfter and reports how many
// were never delivered at all (the loss signal) against how many were
// delivered but never acked (a stranded lease, which the poison fraction
// produces on purpose).
func (t *soakTracker) sweep(now time.Time, retention time.Duration) (neverDelivered, deliveredUnacked, reclaimed, outstanding int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for seq, p := range t.pending {
		age := now.Sub(p.producedAt)
		if age < soakLostAfter && age < retention {
			continue
		}
		switch {
		case !p.deliveredAt.IsZero():
			deliveredUnacked++
		case age >= retention:
			// Retention reclaimed it before any consumer got to it. That
			// is the topic's configured behaviour, not loss.
			reclaimed++
		default:
			neverDelivered++
		}
		delete(t.pending, seq)
	}
	return neverDelivered, deliveredUnacked, reclaimed, len(t.pending)
}

// soakMetrics is what a watcher scrapes. Everything is labelled by topic
// so one pod's series say which profile is in trouble.
type soakMetrics struct {
	produced, consumed, acked, nacked, poisoned *prometheus.CounterVec
	produceErrors, consumeErrors, ackErrors     *prometheus.CounterVec
	dupAfterAck, unknownSeq, priorEpoch         *prometheus.CounterVec
	neverDelivered, deliveredUnacked, overflow  *prometheus.CounterVec
	retentionReclaimed, backlogAged             *prometheus.CounterVec
	outstanding                                 *prometheus.GaugeVec
	e2e                                         *prometheus.HistogramVec
	produceLatency, consumeLatency, ackLatency  *prometheus.HistogramVec
	startedAt                                   prometheus.Gauge
}

func newSoakMetrics(reg *prometheus.Registry) *soakMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "narad_soak", Name: name, Help: help}, labels)
		reg.MustRegister(c)
		return c
	}
	histogram := func(name, help string, buckets []float64) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "narad_soak", Name: name, Help: help, Buckets: buckets,
		}, []string{"topic"})
		reg.MustRegister(h)
		return h
	}
	rpcBuckets := []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	e2eBuckets := []float64{.005, .01, .05, .1, .5, 1, 5, 15, 30, 60, 300, 900, 1800}

	m := &soakMetrics{
		produced:           counter("produced_total", "messages accepted with 202", "topic"),
		consumed:           counter("consumed_total", "messages returned by a consume", "topic"),
		acked:              counter("acked_total", "messages acked with 204", "topic"),
		nacked:             counter("nacked_total", "messages deliberately returned to the queue", "topic"),
		poisoned:           counter("poisoned_total", "messages deliberately left unacked, stranding a lease", "topic"),
		produceErrors:      counter("produce_errors_total", "failed produces by outcome", "topic", "outcome"),
		consumeErrors:      counter("consume_errors_total", "failed consumes by outcome", "topic", "outcome"),
		ackErrors:          counter("ack_errors_total", "failed acks by outcome", "topic", "outcome"),
		dupAfterAck:        counter("dup_after_ack_total", "messages redelivered after this pod acked them: an exactly-once violation outside a broker restart", "topic"),
		unknownSeq:         counter("unknown_sequence_total", "deliveries whose sequence this process produced but no longer remembers, because the acked window rolled past it", "topic"),
		priorEpoch:         counter("prior_epoch_total", "deliveries this pod produced before its current process started: acked, not verified", "topic"),
		neverDelivered:     counter("never_delivered_total", "messages produced and never delivered before the loss deadline: the loss signal", "topic"),
		deliveredUnacked:   counter("delivered_unacked_total", "messages delivered but never acked before the loss deadline: stranded leases, which the poison fraction creates on purpose", "topic"),
		overflow:           counter("tracking_overflow_total", "messages produced without tracking because the outstanding table was full", "topic"),
		retentionReclaimed: counter("retention_reclaimed_total", "messages that reached the topic's retention without being delivered: expected on a topic whose consumers are behind on purpose", "topic"),
		backlogAged:        counter("backlog_aged_total", "aged undelivered messages on a profile whose backlog is deliberate, kept out of the loss signal", "topic"),
		e2e:                histogram("end_to_end_seconds", "produce to first delivery", e2eBuckets),
		produceLatency:     histogram("produce_seconds", "produce request latency", rpcBuckets),
		consumeLatency:     histogram("consume_seconds", "consume request latency, including the long poll", rpcBuckets),
		ackLatency:         histogram("ack_seconds", "ack request latency", rpcBuckets),
	}
	m.outstanding = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "narad_soak", Name: "outstanding", Help: "messages produced by this pod and not yet acked",
	}, []string{"topic"})
	m.startedAt = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "narad_soak", Name: "started_at_seconds", Help: "unix time this soak process started",
	})
	reg.MustRegister(m.outstanding, m.startedAt)
	m.startedAt.Set(float64(time.Now().Unix()))
	return m
}

// runSoak is the mode entry point. It runs until the context ends, which
// for a soak means until the pod is stopped.
func runSoak(cfg config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cfg.timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, cfg.timeout)
		defer stop()
	}

	pod := os.Getenv("POD_NAME")
	if pod == "" {
		pod, _ = os.Hostname()
	}
	if pod == "" {
		pod = cfg.runID
	}
	lb := &roundRobinClient{
		nodes: cfg.nodes,
		client: &http.Client{
			// Longer than the longest consume wait, so a long poll is
			// never cut off by the client.
			Timeout:   60 * time.Second,
			Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128, IdleConnTimeout: 90 * time.Second},
		},
		username: cfg.username,
		password: cfg.password,
	}

	reg := prometheus.NewRegistry()
	metrics := newSoakMetrics(reg)
	if cfg.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		srv := &http.Server{Addr: cfg.metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("soak: metrics listener: %v\n", err)
			}
		}()
		defer func() { _ = srv.Close() }()
		fmt.Printf("soak: metrics on %s/metrics\n", cfg.metricsAddr)
	}

	// The pod ordinal is stable across restarts, so a restarted process
	// picks its own topics back up.
	suffix := pod
	if i := strings.LastIndex(pod, "-"); i >= 0 && i+1 < len(pod) {
		suffix = pod[i+1:]
	}
	profiles := soakSelected(cfg, suffix)
	if err := soakEnsureTopics(ctx, lb, profiles); err != nil {
		return err
	}
	if err := soakWaitTopicsVisible(ctx, lb, profiles); err != nil {
		return err
	}
	trackers := make(map[string]*soakTracker, len(profiles))
	for _, p := range profiles {
		trackers[p.name] = newSoakTracker()
	}

	fmt.Printf("soak: pod=%s profiles=%d target=%.1f msg/s rate-scale=%.2f\n",
		pod, len(profiles), soakTotalRate(profiles), cfg.rateScale)
	for _, p := range profiles {
		fmt.Printf("  %-26s %2d partitions  %7.2f msg/s  %5d B  %2d consumers  work %v-%v  nack %.1f%% poison %.2f%%  retention %v\n",
			p.name, p.partitions, p.rate*cfg.rateScale, p.payloadBytes, p.consumers,
			p.minWork, p.maxWork, p.nackRate*100, p.poisonRate*100, p.retention)
	}

	var wg sync.WaitGroup
	for _, p := range profiles {
		tracker := trackers[p.name]
		wg.Add(1)
		go func(p soakProfile) {
			defer wg.Done()
			soakProduce(ctx, lb, p, cfg, pod, tracker, metrics)
		}(p)
		for i := range p.consumers {
			wg.Add(1)
			go func(p soakProfile, worker int) {
				defer wg.Done()
				soakConsume(ctx, lb, p, pod, worker, tracker, metrics)
			}(p, i)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		soakReport(ctx, cfg, profiles, trackers, metrics)
	}()

	wg.Wait()
	fmt.Println("soak: stopped")
	return nil
}

// soakSelected applies any name filter and scopes the topics to this
// process.
//
// Each process gets its own copy of every profile, which models several
// services running the same shapes rather than one service whose workers
// are spread over several pods. The distinction matters for verification
// and not much else: narad's queue semantics mean workers sharing a
// topic are one consumer group, so a message one process produced can be
// acked by another, and then no process can tell whether it was acked or
// lost. Within a process the group is still many workers on one topic,
// which is what makes acks land out of order.
func soakSelected(cfg config, suffix string) []soakProfile {
	var out []soakProfile
	wanted := map[string]bool{}
	for _, n := range strings.Split(cfg.soakProfiles, ",") {
		if n = strings.TrimSpace(n); n != "" {
			wanted[n] = true
		}
	}
	for _, p := range soakProfiles {
		if len(wanted) > 0 && !wanted[p.name] {
			continue
		}
		if suffix != "" {
			p.name += "-" + suffix
		}
		out = append(out, p)
	}
	return out
}

func soakTotalRate(profiles []soakProfile) float64 {
	total := 0.0
	for _, p := range profiles {
		total += p.rate
	}
	return total
}

// soakEnsureTopics creates what is missing and leaves what exists alone:
// the topics outlive any one run of this driver, as a company's do.
func soakEnsureTopics(ctx context.Context, lb *roundRobinClient, profiles []soakProfile) error {
	for _, p := range profiles {
		record := topicRecord{
			Name:                      p.name,
			Partitions:                p.partitions,
			RetentionMs:               int64(p.retention / time.Millisecond),
			VisibilityTimeoutMs:       int64(p.visibility / time.Millisecond),
			MaxInFlightPerPartition:   driverTopicInFlight,
			MaxAckedAheadPerPartition: driverTopicAckedAhead,
		}
		status, body, err := lb.do(ctx, http.MethodPost, "/v1/topics", record, nil,
			http.StatusOK, http.StatusCreated, http.StatusConflict)
		if err != nil {
			return fmt.Errorf("create %s: %w", p.name, err)
		}
		if status == http.StatusConflict {
			fmt.Printf("soak: topic %s already exists, reusing it\n", p.name)
			continue
		}
		_ = body
		fmt.Printf("soak: created topic %s\n", p.name)
	}
	return nil
}

// soakWaitTopicsVisible blocks until every node answers for every topic.
// A produce that beats the assignment view around the cluster is
// answered 404, which is a real answer to a real race and not worth
// counting as a soak error every time a process restarts.
func soakWaitTopicsVisible(ctx context.Context, lb *roundRobinClient, profiles []soakProfile) error {
	deadline := time.Now().Add(2 * time.Minute)
	for _, p := range profiles {
		for {
			ok := true
			// One pass per node, since each answers from its own view.
			for range len(lb.nodes) {
				status, _, err := lb.do(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(p.name), nil, nil,
					http.StatusOK, http.StatusNotFound)
				if err != nil || status != http.StatusOK {
					ok = false
					break
				}
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("topic %s was not visible on every node within the deadline", p.name)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	fmt.Printf("soak: all %d topics visible on every node\n", len(profiles))
	return nil
}

// soakProduce paces one profile: a steady ticker, or one batch per
// burstEvery for the batch-shaped profiles.
func soakProduce(ctx context.Context, lb *roundRobinClient, p soakProfile, cfg config, pod string, tracker *soakTracker, m *soakMetrics) {
	rate := p.rate * cfg.rateScale
	if rate <= 0 {
		return
	}
	filler := strings.Repeat("x", max(0, p.payloadBytes-120))
	var seq atomic.Uint64

	send := func() {
		n := seq.Add(1)
		key := fmt.Sprintf("%s-%06d", pod, n%uint64(max(p.keys, 1)))
		record := soakRecord{Pod: pod, Epoch: soakEpoch, Seq: n, At: time.Now().UnixNano(), Filler: filler}
		body, err := json.Marshal(record)
		if err != nil {
			m.produceErrors.WithLabelValues(p.name, "encode").Inc()
			return
		}
		path := "/v1/topics/" + url.PathEscape(p.name) + "/produce?key=" + url.QueryEscape(key)
		started := time.Now()
		status, _, err := lb.doRaw(ctx, http.MethodPost, path, body, nil, http.StatusAccepted)
		m.produceLatency.WithLabelValues(p.name).Observe(time.Since(started).Seconds())
		if err != nil {
			m.produceErrors.WithLabelValues(p.name, soakOutcome(status, err)).Inc()
			return
		}
		if !tracker.produced(n, started) {
			m.overflow.WithLabelValues(p.name).Inc()
		}
		m.produced.WithLabelValues(p.name).Inc()
	}

	// A pool so one slow request cannot drag the pace down, sized to
	// cover the profile's rate at a pessimistic request latency.
	// A steady profile needs enough workers to cover its rate at a
	// pessimistic latency; a burst profile needs enough to place the
	// whole batch before the next one, or most of it is dropped.
	workers := max(2, min(64, int(rate/4)+2))
	if p.burstEvery > 0 {
		workers = max(workers, min(64, int(rate*p.burstEvery.Seconds()/8)+2))
	}
	jobs := make(chan struct{}, workers*4)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				send()
			}
		}()
	}
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	offer := func(n int) {
		for range n {
			select {
			case jobs <- struct{}{}:
			case <-ctx.Done():
				return
			default:
				// The pool is saturated: the cluster is slower than the
				// profile's rate, which the produce latency histogram
				// and this counter both say.
				m.produceErrors.WithLabelValues(p.name, "pacer_behind").Inc()
			}
		}
	}

	if p.burstEvery > 0 {
		batch := max(1, int(rate*p.burstEvery.Seconds()))
		ticker := time.NewTicker(p.burstEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				offer(batch)
			}
		}
	}

	interval := time.Duration(float64(time.Second) / rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	perTick := max(1, int(rate*interval.Seconds()+0.5))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			offer(perTick)
		}
	}
}

// soakConsume is one handler: poll, pretend to work for as long as the
// profile says, then ack, nack, or leave it stranded.
func soakConsume(ctx context.Context, lb *roundRobinClient, p soakProfile, pod string, worker int, tracker *soakTracker, m *soakMetrics) {
	wait := p.consumeWait
	if wait <= 0 {
		wait = 2 * time.Second
	}
	path := "/v1/topics/" + url.PathEscape(p.name) + "/consume?wait=" + wait.String()
	// drainBudget is how many messages a periodic consumer takes before
	// it naps again; it has to exceed what accumulates during the nap.
	const drainBudget = 512
	drainedSinceNap := drainBudget
	for {
		if ctx.Err() != nil {
			return
		}
		// A periodic consumer sleeps, then drains what piled up. Napping
		// between individual messages instead would cap the group at one
		// message per nap, which no real batch consumer does and which
		// only ever grows the backlog.
		if p.idleBetweenPolls > 0 && drainedSinceNap >= drainBudget {
			drainedSinceNap = 0
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.idleBetweenPolls):
			}
		}
		started := time.Now()
		status, body, err := lb.do(ctx, http.MethodGet, path, nil, nil,
			http.StatusOK, http.StatusNoContent)
		m.consumeLatency.WithLabelValues(p.name).Observe(time.Since(started).Seconds())
		if err != nil {
			m.consumeErrors.WithLabelValues(p.name, soakOutcome(status, err)).Inc()
			soakBackoff(ctx, 250*time.Millisecond)
			continue
		}
		if status == http.StatusNoContent {
			// Nothing left: the drain is done, so nap.
			drainedSinceNap = drainBudget
			continue
		}
		drainedSinceNap++
		var msg struct {
			ReceiptHandle string     `json:"receipt_handle"`
			Payload       soakRecord `json:"payload"`
		}
		if err := json.Unmarshal(body, &msg); err != nil {
			m.consumeErrors.WithLabelValues(p.name, "decode").Inc()
			continue
		}
		m.consumed.WithLabelValues(p.name).Inc()

		// Only this process's own messages can be verified; anything from
		// a sibling pod or an earlier incarnation is still acked, just
		// not measured.
		mine := msg.Payload.Pod == pod && msg.Payload.Epoch == soakEpoch
		if msg.Payload.Pod == pod && msg.Payload.Epoch != soakEpoch {
			m.priorEpoch.WithLabelValues(p.name).Inc()
		}
		if mine {
			dup, e2e, known := tracker.delivered(msg.Payload.Seq, time.Now())
			switch {
			case dup:
				m.dupAfterAck.WithLabelValues(p.name).Inc()
			case !known:
				// Produced by an earlier incarnation of this pod, before
				// a restart wiped the table.
				m.unknownSeq.WithLabelValues(p.name).Inc()
			case e2e > 0:
				m.e2e.WithLabelValues(p.name).Observe(e2e.Seconds())
			}
		}

		// The handler's own latency, which is what makes acks land out
		// of order across a partition.
		if p.maxWork > 0 {
			work := p.minWork
			if p.maxWork > p.minWork {
				work += time.Duration(rand.Int64N(int64(p.maxWork - p.minWork)))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(work):
			}
		}

		switch {
		case p.poisonRate > 0 && rand.Float64() < p.poisonRate:
			// A handler that hung: the lease stays out until it lapses.
			m.poisoned.WithLabelValues(p.name).Inc()
			continue
		case p.nackRate > 0 && rand.Float64() < p.nackRate:
			if soakNack(ctx, lb, p.name, msg.ReceiptHandle) {
				m.nacked.WithLabelValues(p.name).Inc()
			} else {
				m.ackErrors.WithLabelValues(p.name, "nack").Inc()
			}
			continue
		}

		ackStart := time.Now()
		ackPath := "/v1/topics/" + url.PathEscape(p.name) + "/ack?receipt_handle=" + url.QueryEscape(msg.ReceiptHandle)
		ackStatus, _, ackErr := lb.do(ctx, http.MethodPost, ackPath, nil, nil, http.StatusNoContent)
		m.ackLatency.WithLabelValues(p.name).Observe(time.Since(ackStart).Seconds())
		if ackErr != nil {
			m.ackErrors.WithLabelValues(p.name, soakOutcome(ackStatus, ackErr)).Inc()
			continue
		}
		m.acked.WithLabelValues(p.name).Inc()
		if mine {
			tracker.acked(msg.Payload.Seq)
		}
	}
}

// soakNack returns a message to the queue at once: the ack route with
// extend=0 releases the reservation instead of settling it.
func soakNack(ctx context.Context, lb *roundRobinClient, topicName, handle string) bool {
	path := "/v1/topics/" + url.PathEscape(topicName) + "/ack?receipt_handle=" + url.QueryEscape(handle) + "&extend=0"
	_, _, err := lb.do(ctx, http.MethodPost, path, nil, nil, http.StatusNoContent)
	return err == nil
}

func soakBackoff(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// soakOutcome labels a failure by HTTP status when there is one, so the
// error counters separate "the cluster said no" from "the request never
// arrived".
func soakOutcome(status int, err error) string {
	if status > 0 {
		return fmt.Sprintf("http_%d", status)
	}
	if ctxErr := err; ctxErr != nil && strings.Contains(ctxErr.Error(), "context") {
		return "cancelled"
	}
	return "transport"
}

// soakReport sweeps the trackers and prints a line per topic, so the pod
// log alone answers "is the soak healthy" without a metrics scrape.
func soakReport(ctx context.Context, cfg config, profiles []soakProfile, trackers map[string]*soakTracker, m *soakMetrics) {
	every := cfg.reportEvery
	if every <= 0 {
		every = time.Minute
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			var lost, stranded int
			byName := make(map[string]soakProfile, len(profiles))
			names := make([]string, 0, len(profiles))
			for _, p := range profiles {
				names = append(names, p.name)
				byName[p.name] = p
			}
			sort.Strings(names)
			var b strings.Builder
			fmt.Fprintf(&b, "soak t=%s\n", now.Sub(started).Round(time.Second))
			for _, name := range names {
				p := byName[name]
				neverDelivered, deliveredUnacked, reclaimed, outstanding := trackers[name].sweep(now, p.retention)
				if p.backlogByDesign {
					// The backlog is the point here, so an aged entry is
					// not evidence about the broker either way.
					m.backlogAged.WithLabelValues(name).Add(float64(neverDelivered))
					neverDelivered = 0
				}
				lost += neverDelivered
				stranded += deliveredUnacked
				if neverDelivered > 0 {
					m.neverDelivered.WithLabelValues(name).Add(float64(neverDelivered))
				}
				if deliveredUnacked > 0 {
					m.deliveredUnacked.WithLabelValues(name).Add(float64(deliveredUnacked))
				}
				if reclaimed > 0 {
					m.retentionReclaimed.WithLabelValues(name).Add(float64(reclaimed))
				}
				m.outstanding.WithLabelValues(name).Set(float64(outstanding))
				fmt.Fprintf(&b, "  %-26s outstanding=%-7d aged_never_delivered=%-4d aged_delivered_unacked=%-4d retention_reclaimed=%d\n",
					name, outstanding, neverDelivered, deliveredUnacked, reclaimed)
			}
			if lost > 0 {
				fmt.Fprintf(&b, "  ALERT %d messages passed the %v loss deadline without ever being delivered\n", lost, soakLostAfter)
			}
			fmt.Print(b.String())
		}
	}
}
