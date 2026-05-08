// Package metrics owns Narad's Prometheus metrics surface.
//
// All collectors are owned by a single Metrics struct constructed via
// New(reg). Hot-path callers (broker, storage adapter, HTTP middleware,
// poller) read metrics off this struct directly — there is no global
// state and no init-time registration.
//
// Cardinality budget is enforced by the label set: {topic},
// {topic, partition}, and {route, method, status}. No per-client-id
// or per-request labels.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace is prepended to every metric name.
const Namespace = "narad"

// Metrics holds every collector emitted by Narad. Pass an instance
// down to producers/consumers/storage; pass nil to disable (callers
// that hold *Metrics check for nil before observing).
type Metrics struct {
	// HTTP
	HTTPRequestsTotal      *prometheus.CounterVec   // route, method, status
	HTTPRequestDuration    *prometheus.HistogramVec // route, method
	HTTPBytesIn            *prometheus.CounterVec   // route
	HTTPBytesOut           *prometheus.CounterVec   // route
	HTTPRequestsInFlight   prometheus.Gauge         // total (route unknown at entry)

	// Broker throughput
	MessagesProducedTotal *prometheus.CounterVec // topic, partition
	MessagesConsumedTotal *prometheus.CounterVec // topic, partition
	BytesProducedTotal    *prometheus.CounterVec // topic, partition
	BytesConsumedTotal    *prometheus.CounterVec // topic, partition
	ProduceRejectionsTotal *prometheus.CounterVec // topic, reason

	// Long-poll
	ConsumeWaitSeconds *prometheus.HistogramVec // topic, outcome
	ConsumeEmptyTotal  *prometheus.CounterVec   // topic

	// Inventory / lag (gauges; updated by poller)
	TopicsTotal               prometheus.Gauge
	PartitionsTotal           prometheus.Gauge
	TopicBytes                *prometheus.GaugeVec // topic
	PartitionSizeBytes        *prometheus.GaugeVec // topic, partition
	Segments                  *prometheus.GaugeVec // topic, partition
	ConsumerLagMessages       *prometheus.GaugeVec // topic, partition
	ConsumerDroppedMessages   *prometheus.GaugeVec // topic, partition
	OldestUnconsumedAgeSeconds *prometheus.GaugeVec // topic, partition

	// Storage lifecycle
	FlushDurationSeconds      *prometheus.HistogramVec // topic, partition
	FlushBytesTotal           *prometheus.CounterVec   // topic, partition
	FsyncDurationSeconds      *prometheus.HistogramVec // topic, partition
	SegmentsRolledTotal       *prometheus.CounterVec   // topic, partition
	RetentionDeletionsTotal   *prometheus.CounterVec   // topic, partition, reason
	RetentionBytesDeleted     *prometheus.CounterVec   // topic, partition, reason
	RetentionMessagesDeleted  *prometheus.CounterVec   // topic, partition, reason
	RetentionRunSeconds       *prometheus.HistogramVec // topic, partition
	SegmentsScannedAtBoot     *prometheus.CounterVec   // topic, partition

	// Errors
	ErrorsTotal *prometheus.CounterVec // component, kind

	// Boot
	BootDurationSeconds prometheus.Gauge
}

// New constructs the metrics struct and registers every collector with
// reg. Panics on duplicate registration — collectors share a process,
// so duplicates are a programming error.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests by route, method, and status code.",
		}, []string{"route", "method", "status"}),

		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request duration in seconds, by route and method.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"route", "method"}),

		HTTPBytesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "request_bytes_in_total",
			Help:      "Total request bytes received per route (from Content-Length).",
		}, []string{"route"}),

		HTTPBytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "response_bytes_out_total",
			Help:      "Total response bytes written per route.",
		}, []string{"route"}),

		HTTPRequestsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Subsystem: "http",
			Name:      "requests_in_flight",
			Help:      "Number of HTTP requests currently being served. Not labeled by route — the matched route is only known after dispatch, so we track the total only.",
		}),

		MessagesProducedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "messages_produced_total",
			Help:      "Messages successfully appended to a partition log.",
		}, []string{"topic", "partition"}),

		MessagesConsumedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "messages_consumed_total",
			Help:      "Messages returned by Consume (queue or replay).",
		}, []string{"topic", "partition"}),

		BytesProducedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "bytes_produced_total",
			Help:      "Payload bytes appended to a partition log.",
		}, []string{"topic", "partition"}),

		BytesConsumedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "bytes_consumed_total",
			Help:      "Payload bytes returned by Consume.",
		}, []string{"topic", "partition"}),

		ProduceRejectionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "produce_rejections_total",
			Help:      "Produce calls rejected before append (schema, policy, etc.).",
		}, []string{"topic", "reason"}),

		ConsumeWaitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "consume_wait_seconds",
			Help:      "Time consumers spent in long-poll wait, by outcome (hit, timeout, cancelled).",
			// Long-poll waits cluster around 0 (immediate hit) or near MaxConsumeWait.
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60},
		}, []string{"topic", "outcome"}),

		ConsumeEmptyTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "consume_empty_total",
			Help:      "Consume calls that returned no message after waiting (timeout or no-wait empty).",
		}, []string{"topic"}),

		TopicsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "topics_total",
			Help:      "Number of topics currently registered.",
		}),

		PartitionsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "partitions_total",
			Help:      "Total partitions across all topics.",
		}),

		TopicBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "topic_bytes",
			Help:      "On-disk bytes used by a topic, summed across partitions.",
		}, []string{"topic"}),

		PartitionSizeBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "partition_size_bytes",
			Help:      "On-disk bytes used by a single partition.",
		}, []string{"topic", "partition"}),

		Segments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "segments",
			Help:      "Number of segment files for a partition.",
		}, []string{"topic", "partition"}),

		ConsumerLagMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "consumer_lag_messages",
			Help:      "Unconsumed messages: log_end_offset - committed_offset.",
		}, []string{"topic", "partition"}),

		ConsumerDroppedMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "consumer_dropped_messages",
			Help:      "Unacknowledged messages already deleted by retention: max(0, log_start - committed).",
		}, []string{"topic", "partition"}),

		OldestUnconsumedAgeSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "oldest_unconsumed_message_age_seconds",
			Help:      "Wall-clock age of the message at committed_offset (head of unconsumed queue). 0 when caught up.",
		}, []string{"topic", "partition"}),

		FlushDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "flush_duration_seconds",
			Help:      "Time spent draining the in-memory buffer to a segment file.",
			Buckets:   storageDurationBuckets,
		}, []string{"topic", "partition"}),

		FlushBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "flush_bytes_total",
			Help:      "Total bytes written by flusher to segment files.",
		}, []string{"topic", "partition"}),

		FsyncDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "fsync_duration_seconds",
			Help:      "Time spent in file.Sync().",
			Buckets:   storageDurationBuckets,
		}, []string{"topic", "partition"}),

		SegmentsRolledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "segments_rolled_total",
			Help:      "Active segment closed and a new one created.",
		}, []string{"topic", "partition"}),

		RetentionDeletionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "retention_deletions_total",
			Help:      "Segment files removed by retention, by reason (age, bytes).",
		}, []string{"topic", "partition", "reason"}),

		RetentionBytesDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "retention_bytes_deleted_total",
			Help:      "Bytes reclaimed by retention, by reason.",
		}, []string{"topic", "partition", "reason"}),

		RetentionMessagesDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "retention_messages_deleted_total",
			Help:      "Messages discarded by retention, by reason.",
		}, []string{"topic", "partition", "reason"}),

		RetentionRunSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "retention_run_duration_seconds",
			Help:      "Wall time of one retention sweep.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"topic", "partition"}),

		SegmentsScannedAtBoot: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Subsystem: "storage",
			Name:      "segments_scanned_at_boot_total",
			Help:      "Segment files scanned during partition log recovery (cumulative across restarts).",
		}, []string{"topic", "partition"}),

		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "errors_total",
			Help:      "Cross-cutting error counter, by component and kind.",
		}, []string{"component", "kind"}),

		BootDurationSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "boot_duration_seconds",
			Help:      "Wall time from process start to API listener up. Set once at startup.",
		}),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal, m.HTTPRequestDuration, m.HTTPBytesIn, m.HTTPBytesOut, m.HTTPRequestsInFlight,
		m.MessagesProducedTotal, m.MessagesConsumedTotal,
		m.BytesProducedTotal, m.BytesConsumedTotal,
		m.ProduceRejectionsTotal,
		m.ConsumeWaitSeconds, m.ConsumeEmptyTotal,
		m.TopicsTotal, m.PartitionsTotal,
		m.TopicBytes, m.PartitionSizeBytes, m.Segments,
		m.ConsumerLagMessages, m.ConsumerDroppedMessages, m.OldestUnconsumedAgeSeconds,
		m.FlushDurationSeconds, m.FlushBytesTotal, m.FsyncDurationSeconds,
		m.SegmentsRolledTotal,
		m.RetentionDeletionsTotal, m.RetentionBytesDeleted, m.RetentionMessagesDeleted, m.RetentionRunSeconds,
		m.SegmentsScannedAtBoot,
		m.ErrorsTotal,
		m.BootDurationSeconds,
	)

	return m
}

// storageDurationBuckets is tuned for sub-second IO (flush, fsync,
// retention sweeps that don't touch the disk). Anything above 1s is
// already pathological for these paths.
var storageDurationBuckets = []float64{
	100e-6, 500e-6, // 100µs, 500µs
	0.001, 0.005, 0.01, 0.05, // 1ms, 5ms, 10ms, 50ms
	0.1, 0.5, 1, // 100ms, 500ms, 1s
}

// IncError is a small helper so callers don't need to remember the
// label order or guard against m == nil at every site.
func (m *Metrics) IncError(component, kind string) {
	if m == nil {
		return
	}
	m.ErrorsTotal.WithLabelValues(component, kind).Inc()
}
