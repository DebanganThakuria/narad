package config

// HTTPConfig governs the public-facing API listener. Durations marshal to
// human-friendly strings ("10s", "500ms") rather than nanoseconds — see
// Duration.
type HTTPConfig struct {
	Addr           string   `json:"addr"`
	PprofAddr      string   `json:"pprof_addr,omitempty"`
	ReadTimeout    Duration `json:"read_timeout"`
	WriteTimeout   Duration `json:"write_timeout"`
	IdleTimeout    Duration `json:"idle_timeout"`
	ShutdownGrace  Duration `json:"shutdown_grace"`
	MaxConsumeWait Duration `json:"max_consume_wait"`
}
