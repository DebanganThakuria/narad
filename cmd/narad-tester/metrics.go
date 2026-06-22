package main

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type testerMetrics struct {
	registry *prometheus.Registry

	messagesProduced       prometheus.Counter
	messagesConsumed       *prometheus.CounterVec
	messagesAcked          prometheus.Counter
	messagesDuplicate      prometheus.Counter
	messagesUnknown        prometheus.Counter
	consumedMarkersEvicted prometheus.Counter
	produceThrottled       prometheus.Counter
	targetRate             prometheus.Gauge
	producerQueueDepth     prometheus.Gauge
	producerQueueCapacity  prometheus.Gauge

	produceErrors *prometheus.CounterVec
	consumeErrors *prometheus.CounterVec
	ackErrors     *prometheus.CounterVec
	ledgerErrors  *prometheus.CounterVec

	outstandingMessages     prometheus.Gauge
	pendingMessages         prometheus.Gauge
	missingMessages         prometheus.Gauge
	oldestOutstandingAge    prometheus.Gauge
	ledgerRecords           *prometheus.GaugeVec
	produceLatency          prometheus.Histogram
	consumeRequestLatency   prometheus.Histogram
	ackLatency              prometheus.Histogram
	dispatchBlockLatency    prometheus.Histogram
	produceToConsumeLatency prometheus.Histogram
	nodeRequests            *prometheus.CounterVec
	nodeRequestDuration     *prometheus.HistogramVec
}

func newTesterMetrics() *testerMetrics {
	registry := prometheus.NewRegistry()
	m := &testerMetrics{
		registry: registry,
		messagesProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "messages_produced_total",
			Help:      "Messages successfully produced by the tester.",
		}),
		messagesConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "messages_consumed_total",
			Help:      "Messages consumed by the tester, classified against in-memory tester state.",
		}, []string{"result"}),
		messagesAcked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "messages_acked_total",
			Help:      "Messages successfully acked by the tester.",
		}),
		messagesDuplicate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "messages_duplicate_total",
			Help:      "Consumed messages already seen by the exact per-run sequence ledger.",
		}),
		messagesUnknown: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "messages_unknown_total",
			Help:      "Consumed messages not present in outstanding state or the consumed sequence ledger.",
		}),
		consumedMarkersEvicted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "consumed_markers_evicted_total",
			Help:      "Deprecated: always zero because duplicate detection uses an exact sequence ledger.",
		}),
		produceThrottled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "produce_throttled_total",
			Help:      "Produce jobs skipped because the tester outstanding-message cap was reached.",
		}),
		targetRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "target_messages_per_second",
			Help:      "Current tester target aggregate produce rate.",
		}),
		producerQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "producer_queue_depth",
			Help:      "Current number of queued produce jobs waiting for tester workers.",
		}),
		producerQueueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "producer_queue_capacity",
			Help:      "Capacity of the tester produce job queue.",
		}),
		produceErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "produce_errors_total",
			Help:      "Produce errors by reason and HTTP status.",
		}, []string{"reason", "status"}),
		consumeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "consume_errors_total",
			Help:      "Consume errors by reason and HTTP status.",
		}, []string{"reason", "status"}),
		ackErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "ack_errors_total",
			Help:      "Ack errors by reason and HTTP status.",
		}, []string{"reason", "status"}),
		ledgerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "ledger_errors_total",
			Help:      "Tester state errors by operation.",
		}, []string{"operation"}),
		outstandingMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "outstanding_messages",
			Help:      "Produced messages not yet consumed according to in-memory tester state.",
		}),
		pendingMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "pending_messages",
			Help:      "Deprecated: always zero because the tester records only successful produces.",
		}),
		missingMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "messages_missing",
			Help:      "Produced outstanding messages older than the configured missing-after duration.",
		}),
		oldestOutstandingAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "oldest_outstanding_message_age_seconds",
			Help:      "Age of the oldest produced message not yet consumed according to in-memory tester state.",
		}),
		ledgerRecords: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "narad_tester",
			Name:      "ledger_records",
			Help:      "Records tracked by the tester in-memory ledger.",
		}, []string{"bucket"}),
		produceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "produce_latency_seconds",
			Help:      "Tester produce request latency.",
			Buckets:   prometheus.DefBuckets,
		}),
		consumeRequestLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "consume_request_latency_seconds",
			Help:      "Tester consume request latency, including long-poll wait.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
		ackLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "ack_latency_seconds",
			Help:      "Tester ack request latency.",
			Buckets:   prometheus.DefBuckets,
		}),
		dispatchBlockLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "producer_dispatch_block_seconds",
			Help:      "Time the tester dispatcher spent waiting to enqueue a produce job.",
			Buckets:   []float64{0.000001, 0.00001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}),
		produceToConsumeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "produce_to_consume_latency_seconds",
			Help:      "End-to-end latency from successful produce timestamp to first valid consume.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
		}),
		nodeRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "narad_tester",
			Name:      "node_requests_total",
			Help:      "HTTP requests issued by tester by Narad node, method, endpoint, and status.",
		}, []string{"node", "method", "endpoint", "status"}),
		nodeRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "narad_tester",
			Name:      "node_request_duration_seconds",
			Help:      "HTTP request duration by Narad node, method, and endpoint.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"node", "method", "endpoint"}),
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.messagesProduced,
		m.messagesConsumed,
		m.messagesAcked,
		m.messagesDuplicate,
		m.messagesUnknown,
		m.consumedMarkersEvicted,
		m.produceThrottled,
		m.targetRate,
		m.producerQueueDepth,
		m.producerQueueCapacity,
		m.produceErrors,
		m.consumeErrors,
		m.ackErrors,
		m.ledgerErrors,
		m.outstandingMessages,
		m.pendingMessages,
		m.missingMessages,
		m.oldestOutstandingAge,
		m.ledgerRecords,
		m.produceLatency,
		m.consumeRequestLatency,
		m.ackLatency,
		m.dispatchBlockLatency,
		m.produceToConsumeLatency,
		m.nodeRequests,
		m.nodeRequestDuration,
	)
	return m
}

func (m *testerMetrics) observeNodeRequest(node, method, endpoint, status string, duration time.Duration) {
	m.nodeRequests.WithLabelValues(node, method, endpoint, status).Inc()
	m.nodeRequestDuration.WithLabelValues(node, method, endpoint).Observe(duration.Seconds())
}

func (m *testerMetrics) observeLedgerStats(stats ledgerStats) {
	m.outstandingMessages.Set(float64(stats.ProducedOutstanding))
	m.pendingMessages.Set(float64(stats.Pending))
	m.missingMessages.Set(float64(stats.Missing))
	m.oldestOutstandingAge.Set(stats.OldestProducedAge)
	m.ledgerRecords.WithLabelValues("outstanding").Set(float64(stats.OutstandingRecords))
	m.ledgerRecords.WithLabelValues("consumed_sequences").Set(float64(stats.ConsumedSequences))
}

func (m *testerMetrics) serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	select {
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case <-ctx.Done():
		return nil
	}
}
