package config

// StorageConfig governs the on-disk log layout and the in-memory
// buffering that fronts it.
type StorageConfig struct {
	DataDir string    `json:"data_dir"`
	Fsync   FsyncMode `json:"fsync"`

	// Codec selects per-frame compression: "zstd" or "none".
	Codec string `json:"codec"`

	// CompressionLevel: "fastest" | "default" | "better" | "best".
	// zstd decompression speed is independent of encoder level.
	CompressionLevel string `json:"compression_level"`

	// FlushBytes / FlushRecords trigger a flush when the buffer
	// crosses either bound. Zero/negative disables that bound.
	FlushBytes   int `json:"flush_bytes"`
	FlushRecords int `json:"flush_records"`

	// FlushIntervalMs is the maximum time a record may sit in the
	// buffer before being flushed.
	FlushIntervalMs int `json:"flush_interval_ms"`

	// SegmentBytes triggers a segment roll once the active segment's
	// on-disk size meets or exceeds this value.
	SegmentBytes int64 `json:"segment_bytes"`

	// RetentionCheckIntervalMs is the period between retention
	// reaper sweeps per partition.
	RetentionCheckIntervalMs int `json:"retention_check_interval_ms"`
}

// FsyncMode controls how aggressively the storage layer flushes
// writes to disk.
type FsyncMode string

const (
	FsyncPerWrite FsyncMode = "per_write"
	FsyncBatched  FsyncMode = "batched"
)
