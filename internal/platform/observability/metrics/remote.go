package metrics

import "github.com/prometheus/client_golang/prometheus"

// RemoteMetrics are the families of the remotes registry and the
// outbound plane: requests to remotes, the credential cache, the seal
// accounting and the node's posture. Labels are remote names, states,
// reasons and "current" or "previous" for keys; never a URL, a
// username, a fingerprint, a key version or anything derived from a
// password or the cluster secret.
type RemoteMetrics struct {
	// RequestsTotal counts requests to a remote by status code ("error"
	// for a transport failure). remote, code.
	RequestsTotal *prometheus.CounterVec
	// RequestSeconds is the request round trip. remote.
	RequestSeconds *prometheus.HistogramVec
	// RTTSeconds is the last TCP connect time a check measured. remote.
	RTTSeconds *prometheus.GaugeVec
	// CredentialState is 1 for the cache's current state of a remote
	// (ready, stale, credential_unreadable, node_insecure). remote, state.
	CredentialState *prometheus.GaugeVec
	// CredentialDecryptsTotal moves once per credential version per
	// node; anything faster breaks the decrypt-once rule. remote.
	CredentialDecryptsTotal *prometheus.CounterVec
	// ResealOpensTotal counts opens a re-encrypt made on the leader,
	// outside the cache.
	ResealOpensTotal prometheus.Counter
	// CredentialAgeSeconds is the time since the password was set. remote.
	CredentialAgeSeconds *prometheus.GaugeVec
	// CredentialKeyCurrent is 1 when the remote's credential is sealed
	// under the current key. remote.
	CredentialKeyCurrent *prometheus.GaugeVec
	// KeySeals is the FSM's seal count per key (a lower bound). key.
	KeySeals *prometheus.GaugeVec
	// SealsTotal counts seals this node made.
	SealsTotal prometheus.Counter
	// KeyAgeSeconds is the age of a key since its first seal. key.
	KeyAgeSeconds *prometheus.GaugeVec
	// DestinationRefusedTotal counts refused dials and redirects.
	// remote, reason (address, allowlist, port, redirect).
	DestinationRefusedTotal *prometheus.CounterVec
	// AllowlistConfigured is 1 when remotes.allowed_hosts is set.
	AllowlistConfigured prometheus.Gauge
	// PlaintextRaft is 1 on a node that holds remotes while Raft runs
	// without TLS (Q23: warned about, not refused).
	PlaintextRaft prometheus.Gauge
}

func newRemoteMetrics() *RemoteMetrics {
	return &RemoteMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "requests_total",
			Help: "Requests this node sent to a remote cluster, by remote and status code (error for a transport failure).",
		}, []string{"remote", "code"}),
		RequestSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "request_seconds",
			Help:    "Round-trip time of requests to a remote cluster.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16), // 1ms .. ~33s
		}, []string{"remote"}),
		RTTSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "rtt_seconds",
			Help: "TCP connect time to a remote cluster, as the last check measured it.",
		}, []string{"remote"}),
		CredentialState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "credential_state",
			Help: "1 for the credential cache's current state of a remote on this node: ready, stale, credential_unreadable or node_insecure.",
		}, []string{"remote", "state"}),
		CredentialDecryptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "credential_decrypts_total",
			Help: "Credential decryptions by this node's cache. Moves once per credential version; faster movement without a remote write is a bug.",
		}, []string{"remote"}),
		ResealOpensTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "reseal_opens_total",
			Help: "Credential opens a re-encrypt made on this node as leader, outside the cache.",
		}),
		CredentialAgeSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "credential_age_seconds",
			Help: "Seconds since a remote's password was last set.",
		}, []string{"remote"}),
		CredentialKeyCurrent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "credential_key_current",
			Help: "1 when a remote's credential is sealed under the current key; 0 means a cluster secret rotation was not finished with a re-encrypt.",
		}, []string{"remote"}),
		KeySeals: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "key_seals",
			Help: "Seals the metastore counted under a key (current or previous); a lower bound. Warn at 2^29.",
		}, []string{"key"}),
		SealsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "seals_total",
			Help: "Credential seals this node made.",
		}),
		KeyAgeSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "key_age_seconds",
			Help: "Seconds since a key (current) first sealed a credential.",
		}, []string{"key"}),
		DestinationRefusedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "remote", Name: "destination_refused_total",
			Help: "Dials and redirects to a remote refused by the address guard, the port list or the host allowlist, by reason.",
		}, []string{"remote", "reason"}),
		AllowlistConfigured: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "remotes_allowlist_configured",
			Help: "1 when remotes.allowed_hosts is set on this node.",
		}),
		PlaintextRaft: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "remotes_plaintext_raft",
			Help: "1 when this node holds remotes and its Raft transport runs without TLS.",
		}),
	}
}

func (r *RemoteMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		r.RequestsTotal, r.RequestSeconds, r.RTTSeconds,
		r.CredentialState, r.CredentialDecryptsTotal, r.ResealOpensTotal,
		r.CredentialAgeSeconds, r.CredentialKeyCurrent,
		r.KeySeals, r.SealsTotal, r.KeyAgeSeconds,
		r.DestinationRefusedTotal, r.AllowlistConfigured, r.PlaintextRaft,
	}
}

// ForgetRemote drops every series labelled with the named remote, so a
// deleted remote leaves nothing behind in the exposition.
func (r *RemoteMetrics) ForgetRemote(name string) {
	if r == nil {
		return
	}
	match := prometheus.Labels{"remote": name}
	r.RequestsTotal.DeletePartialMatch(match)
	r.RequestSeconds.DeletePartialMatch(match)
	r.RTTSeconds.DeletePartialMatch(match)
	r.CredentialState.DeletePartialMatch(match)
	r.CredentialDecryptsTotal.DeletePartialMatch(match)
	r.CredentialAgeSeconds.DeletePartialMatch(match)
	r.CredentialKeyCurrent.DeletePartialMatch(match)
	r.DestinationRefusedTotal.DeletePartialMatch(match)
}
