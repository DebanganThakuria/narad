package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// pollInterval is the cadence at which the poller refreshes
// inventory and lag gauges. Hardcoded — Prometheus scrapes are
// usually 15-30s, so a 5s tick keeps gauges fresh without doing
// significantly more work than the scraper consumes.
const pollInterval = 5 * time.Second

// Poller is the goroutine that updates Narad's gauge-style metrics
// (lag, inventory, on-disk sizes). Counters and histograms are
// updated inline at the relevant call sites; only gauges need
// periodic refresh because their value is "current state", not a
// running tally.
type Poller struct {
	metrics *Metrics
	broker  SnapshotProvider
	logger  *slog.Logger

	// previousTopics tracks what topics existed at the last tick so
	// we can prune their gauge series after DeleteTopic. Without
	// this, deleted topics would leak series in /metrics until the
	// process restarts.
	previousTopics map[string]struct{}
}

// NewPoller wires the poller. Run must be called for it to do any
// work; the constructor itself does no I/O.
func NewPoller(m *Metrics, br SnapshotProvider, log *slog.Logger) *Poller {
	return &Poller{
		metrics:        m,
		broker:         br,
		logger:         log,
		previousTopics: make(map[string]struct{}),
	}
}

// Run blocks until ctx is cancelled. It does an immediate first tick
// so /metrics returns useful values before the first 5-second
// interval elapses.
func (p *Poller) Run(ctx context.Context) {
	if p.metrics == nil || p.broker == nil {
		return
	}

	p.tick(ctx)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(ctx context.Context) {
	snaps, err := p.broker.Snapshot(ctx)
	if err != nil {
		p.logger.Warn("metrics: snapshot failed", "err", err)
		p.metrics.IncError("metrics", "snapshot")
		return
	}

	currentTopics := make(map[string]struct{}, len(snaps))
	var partitionsTotal int
	nowUnix := time.Now().Unix()

	for _, ts := range snaps {
		currentTopics[ts.Topic] = struct{}{}
		partitionsTotal += len(ts.Partitions)

		var topicBytes int64
		for _, ps := range ts.Partitions {
			partLabel := strconv.Itoa(ps.Partition)
			labels := prometheus.Labels{"topic": ts.Topic, "partition": partLabel}

			topicBytes += ps.SizeBytes
			p.metrics.PartitionSizeBytes.With(labels).Set(float64(ps.SizeBytes))
			p.metrics.Segments.With(labels).Set(float64(ps.SegmentCount))

			lag := max(ps.LogEndOffset-ps.CommittedOffset, 0)
			p.metrics.ConsumerLagMessages.With(labels).Set(float64(lag))
			p.metrics.ConsumerDroppedMessages.With(labels).Set(float64(ps.Dropped))

			var ageSeconds float64
			if ps.OldestUnconsumedAt > 0 {
				ageSeconds = float64(nowUnix - ps.OldestUnconsumedAt)
				if ageSeconds < 0 {
					ageSeconds = 0
				}
			}
			p.metrics.OldestUnconsumedAgeSeconds.With(labels).Set(ageSeconds)
		}

		p.metrics.TopicBytes.With(prometheus.Labels{"topic": ts.Topic}).Set(float64(topicBytes))
	}

	p.metrics.TopicsTotal.Set(float64(len(snaps)))
	p.metrics.PartitionsTotal.Set(float64(partitionsTotal))

	// Prune series for topics that disappeared since last tick.
	for topic := range p.previousTopics {
		if _, still := currentTopics[topic]; still {
			continue
		}
		p.metrics.TopicBytes.DeletePartialMatch(prometheus.Labels{"topic": topic})
		p.metrics.PartitionSizeBytes.DeletePartialMatch(prometheus.Labels{"topic": topic})
		p.metrics.Segments.DeletePartialMatch(prometheus.Labels{"topic": topic})
		p.metrics.ConsumerLagMessages.DeletePartialMatch(prometheus.Labels{"topic": topic})
		p.metrics.ConsumerDroppedMessages.DeletePartialMatch(prometheus.Labels{"topic": topic})
		p.metrics.OldestUnconsumedAgeSeconds.DeletePartialMatch(prometheus.Labels{"topic": topic})
	}
	p.previousTopics = currentTopics
}
