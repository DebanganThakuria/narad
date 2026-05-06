package config

import "time"

// Default returns a Config populated with sane local-development values.
//
// Port choices:
//
//   - 7942 — Narad public API
//   - 7943 — Narad cluster/internal traffic
//
// These are the canonical defaults; operators can override via config
// file, env vars, or CLI flags.
func Default() *Config {
	return &Config{
		HTTP: HTTPConfig{
			Addr:           ":7942",
			ReadTimeout:    Duration(10 * time.Second),
			WriteTimeout:   Duration(30 * time.Second),
			IdleTimeout:    Duration(60 * time.Second),
			ShutdownGrace:  Duration(10 * time.Second),
			MaxConsumeWait: Duration(30 * time.Second),
		},
		Cluster: ClusterConfig{
			Addr: ":7943",
		},
		Storage: StorageConfig{
			DataDir: "data",
			Fsync:   FsyncPerWrite,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		Worker: WorkerConfig{Enabled: false},
	}
}
