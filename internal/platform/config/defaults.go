package config

import "time"

// Default returns a Config populated with sane production values.
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
			MaxConsumeWait: Duration(10 * time.Second),
		},
		Cluster: ClusterConfig{
			Addr: ":7943",
		},
		Storage: StorageConfig{
			DataDir:                     "data",
			Fsync:                       FsyncBatched,
			Codec:                       "none",
			CompressionLevel:            "fastest",
			FlushBytes:                  1 << 20, // 1 MiB
			FlushRecords:                1000,
			FlushIntervalMs:             100,
			SyncIntervalMs:              1000,
			SyncBytes:                   8 << 20,
			HighWatermarkSyncIntervalMs: 5000,
			IngressWALSyncIntervalMs:    10,
			SegmentBytes:                64 << 20, // 64 MiB
			RetentionCheckIntervalMs:    60_000,   // 1 minute
		},
		Topic: TopicConfig{
			DefaultPartitions:                3,
			MaxPartitions:                    108,
			DefaultRetentionAgeMs:            7 * 24 * 60 * 60 * 1000, // 7 days
			DefaultVisibilityTimeoutMs:       30_000,                  // 30 seconds
			DefaultMaxInFlightPerPartition:   1024,
			DefaultMaxAckedAheadPerPartition: 1024,
		},
		Fanout: FanoutConfig{
			MaxBatchRecords: 4096,
			MaxBatchBytes:   4 << 20, // 4 MiB
			LingerMs:        25,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		// Secure by default: a fresh cluster seeds a root admin (random
		// password logged once unless NARAD_ADMIN_PASSWORD is set) and
		// every API call requires Basic auth. Local development can opt
		// out with security.enabled=false / NARAD_SECURITY_ENABLED=false.
		Security: SecurityConfig{
			Enabled: true,
		},
	}
}
