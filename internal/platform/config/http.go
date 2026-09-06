package config

// HTTPConfig governs the public-facing API listener. Durations marshal to
// human-friendly strings ("10s", "500ms") rather than nanoseconds; see
// Duration.
type HTTPConfig struct {
	Addr           string   `json:"addr"`
	PprofAddr      string   `json:"pprof_addr,omitempty"`
	ReadTimeout    Duration `json:"read_timeout"`
	WriteTimeout   Duration `json:"write_timeout"`
	IdleTimeout    Duration `json:"idle_timeout"`
	ShutdownGrace  Duration `json:"shutdown_grace"`
	MaxConsumeWait Duration `json:"max_consume_wait"`

	// MaxHeaderBytes caps a request's header block (Go's default is
	// 1 MiB, far more than any legitimate client sends).
	// Env: NARAD_HTTP_MAX_HEADER_BYTES.
	MaxHeaderBytes int `json:"max_header_bytes"`

	// MaxConnections caps concurrently open client connections on the
	// API listener; connections beyond it wait in the accept queue. Every
	// open long-poll pins a connection and a goroutine, so this is the
	// per-node ceiling on that. 0 disables the cap.
	// Env: NARAD_HTTP_MAX_CONNECTIONS.
	MaxConnections int `json:"max_connections"`

	// MaxConsumeInFlightPerIdentity caps concurrent consume requests
	// (long-polls included) per authenticated user, or per client IP
	// when security is off; extra ones are answered 429. 0 disables it.
	// Env: NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY.
	MaxConsumeInFlightPerIdentity int `json:"max_consume_in_flight_per_identity"`

	// MetricsAddr, when set, serves /metrics on its own listener (like
	// pprof; the two may share an address) and NOT on the API port, so
	// the scrape target can stay cluster-internal without credentials.
	// When empty, /metrics is served on the API port and requires the
	// same Basic auth as the API unless MetricsUnauthenticated is set.
	// Env: NARAD_HTTP_METRICS_ADDR.
	MetricsAddr string `json:"metrics_addr,omitempty"`

	// MetricsUnauthenticated serves /metrics on the API port without
	// credentials (the pre-hardening behaviour). The exposition names
	// every topic with its lag, throughput and fan-out graph, so leave
	// this off unless the API port is not reachable by untrusted
	// callers. Ignored when MetricsAddr is set.
	// Env: NARAD_HTTP_METRICS_UNAUTHENTICATED.
	MetricsUnauthenticated bool `json:"metrics_unauthenticated"`
}
