package config

// FanoutConfig tunes the parent→child fan-out cursor engine. Batch
// size trades latency for throughput: bigger batches mean fewer child
// fsyncs (higher ceiling) but records wait until a batch fills or the
// linger fires. Zero values use the engine defaults.
type FanoutConfig struct {
	// MaxBatchRecords caps records per fan-out batch (default 4096).
	MaxBatchRecords int `json:"max_batch_records"`
	// MaxBatchBytes caps payload bytes per fan-out batch (default 4 MiB).
	MaxBatchBytes int64 `json:"max_batch_bytes"`
	// LingerMs is how long a partially-filled batch waits for more
	// records before committing anyway, so a low-traffic child still
	// drains promptly (default 25).
	LingerMs int64 `json:"linger_ms"`
}
