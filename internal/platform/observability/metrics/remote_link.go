package metrics

import "github.com/prometheus/client_golang/prometheus"

// RemoteLinkMetrics are the families of remote children: per-link
// state, lag and headroom, and the replicator's per-remote data-path
// counters.
type RemoteLinkMetrics struct {
	// State is 1 for a cursor's current link state. parent, child,
	// partition, state.
	State *prometheus.GaugeVec
	// LagSeconds is the age of the oldest record not yet on the
	// remote. parent, child, partition.
	LagSeconds *prometheus.GaugeVec
	// RetentionHeadroomSeconds is the parent's retention minus the
	// lag. parent, child, partition.
	RetentionHeadroomSeconds *prometheus.GaugeVec
	// LastSuccessTimestampSeconds is when a chunk last got a 202.
	// parent, child.
	LastSuccessTimestampSeconds *prometheus.GaugeVec
	// CheckFailuresTotal counts runtime target checks that errored.
	// parent, child.
	CheckFailuresTotal *prometheus.CounterVec
	// SkippedRecordsTotal counts records an admin skipped. parent, child.
	SkippedRecordsTotal *prometheus.CounterVec
	// RereadsTotal counts slabs a remote child read again because the
	// held budget could not keep a waiting lane's records. parent, child.
	RereadsTotal *prometheus.CounterVec
	// ErrorsTotal counts failed chunks by class. remote, class.
	ErrorsTotal *prometheus.CounterVec
	// ResentRecordsTotal counts records resent after an ambiguous
	// failure. remote.
	ResentRecordsTotal *prometheus.CounterVec
	// GateBackoffSeconds is the remote gate's current backoff. remote.
	GateBackoffSeconds *prometheus.GaugeVec
	// HeldBytes is the node's held-record budget in use.
	HeldBytes prometheus.Gauge
	// InflightWaitSeconds is the wait for a max_in_flight slot. remote.
	InflightWaitSeconds *prometheus.HistogramVec
	// ChunkBytesLimit is a remote's adaptive chunk byte cap. remote.
	ChunkBytesLimit *prometheus.GaugeVec
	// WireBytesTotal and BodyBytesTotal are egress bytes after and
	// before compression. remote.
	WireBytesTotal *prometheus.CounterVec
	BodyBytesTotal *prometheus.CounterVec
	// BatchBodyBudgetRejectionsTotal counts batch produce bodies the
	// node-wide body budget turned away with 503.
	BatchBodyBudgetRejectionsTotal prometheus.Counter
}

func newRemoteLinkMetrics() *RemoteLinkMetrics {
	link := []string{"parent", "child"}
	linkPartition := []string{"parent", "child", "partition"}
	remote := []string{"remote"}
	return &RemoteLinkMetrics{
		State: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_state",
			Help: "1 for a remote child cursor's current link state.",
		}, []string{"parent", "child", "partition", "state"}),
		LagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_lag_seconds",
			Help: "Age of the oldest parent record not yet accepted by the remote: the link's live recovery point.",
		}, linkPartition),
		RetentionHeadroomSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_retention_headroom_seconds",
			Help: "Parent retention minus the remote lag: time left before drop-behind starts.",
		}, linkPartition),
		LastSuccessTimestampSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_last_success_timestamp_seconds",
			Help: "Unix time of the last chunk the remote accepted.",
		}, link),
		CheckFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_check_failures_total",
			Help: "Runtime target checks of a remote child that errored.",
		}, link),
		SkippedRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_skipped_records_total",
			Help: "Parent records an admin skipped on a remote child. Each is one record not replicated.",
		}, link),
		RereadsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "fanout", Name: "remote_rereads_total",
			Help: "Slabs a remote child read again because remotes.max_held_bytes could not keep a waiting lane's records. Records the target already accepted are not sent again.",
		}, link),
		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "errors_total",
			Help: "Failed chunks sent to a remote, by class.",
		}, []string{"remote", "class"}),
		ResentRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "resent_records_total",
			Help: "Records resent to a remote after an ambiguous failure: the duplicate volume.",
		}, remote),
		GateBackoffSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "gate_backoff_seconds",
			Help: "Current backoff of a remote's gate on this node; 0 when healthy.",
		}, remote),
		HeldBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "held_bytes",
			Help: "Bytes of records held across a failure, against remotes.max_held_bytes.",
		}),
		InflightWaitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "inflight_wait_seconds",
			Help:    "Time a chunk waited for a max_in_flight slot.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 4, 10),
		}, remote),
		ChunkBytesLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "chunk_bytes_limit",
			Help: "A remote's adaptive chunk byte cap on this node.",
		}, remote),
		WireBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "wire_bytes_total",
			Help: "Request body bytes sent to a remote, after compression.",
		}, remote),
		BodyBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "body_bytes_total",
			Help: "Request body bytes sent to a remote, before compression.",
		}, remote),
		BatchBodyBudgetRejectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "http", Name: "batch_body_budget_rejections_total",
			Help: "Batch produce bodies over 1 MiB answered 503 because the node-wide body budget was full.",
		}),
	}
}

func (r *RemoteLinkMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		r.State, r.LagSeconds, r.RetentionHeadroomSeconds, r.LastSuccessTimestampSeconds,
		r.CheckFailuresTotal, r.SkippedRecordsTotal, r.RereadsTotal,
		r.ErrorsTotal, r.ResentRecordsTotal, r.GateBackoffSeconds, r.HeldBytes,
		r.InflightWaitSeconds, r.ChunkBytesLimit, r.WireBytesTotal, r.BodyBytesTotal,
		r.BatchBodyBudgetRejectionsTotal,
	}
}

// PruneLink drops every series of one remote child (the link was
// detached or its parent deleted).
func (r *RemoteLinkMetrics) PruneLink(parent, child string) {
	if r == nil {
		return
	}
	sel := prometheus.Labels{"parent": parent, "child": child}
	for _, c := range []interface{ DeletePartialMatch(prometheus.Labels) int }{
		r.State, r.LagSeconds, r.RetentionHeadroomSeconds,
		r.LastSuccessTimestampSeconds, r.CheckFailuresTotal, r.SkippedRecordsTotal,
		r.RereadsTotal,
	} {
		c.DeletePartialMatch(sel)
	}
}

// PruneRemote drops every per-remote series of a remote this node no
// longer sends to.
func (r *RemoteLinkMetrics) PruneRemote(remote string) {
	if r == nil {
		return
	}
	sel := prometheus.Labels{"remote": remote}
	for _, c := range []interface{ DeletePartialMatch(prometheus.Labels) int }{
		r.ErrorsTotal, r.ResentRecordsTotal, r.GateBackoffSeconds,
		r.InflightWaitSeconds, r.ChunkBytesLimit, r.WireBytesTotal, r.BodyBytesTotal,
	} {
		c.DeletePartialMatch(sel)
	}
}
