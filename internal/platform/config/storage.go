package config

// StorageConfig governs the data directory and internal storage-engine
// defaults. Only DataDir is part of the stable operator surface; the
// remaining fields are intentionally configured by Narad defaults.
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

	// SyncIntervalMs is the maximum time flushed bytes may sit in the OS
	// page cache before the flusher calls file.Sync() in batched mode.
	SyncIntervalMs int `json:"sync_interval_ms"`

	// SyncBytes triggers file.Sync() once a partition has written at least
	// this many unsynced bytes in batched mode. Zero disables the byte bound.
	SyncBytes int64 `json:"sync_bytes"`

	// HighWatermarkSyncIntervalMs batches durable high-watermark metadata
	// rewrites. Close always forces one final persist.
	HighWatermarkSyncIntervalMs int `json:"high_watermark_sync_interval_ms"`

	// IngressWALSyncIntervalMs is the backstop cadence for the ingress WAL
	// sync loop. Appends wake the loop immediately (group commit), so this
	// only bounds how long buffered records can wait if a wakeup is missed.
	IngressWALSyncIntervalMs int `json:"ingress_wal_sync_interval_ms"`

	// SegmentBytes triggers a segment roll once the active segment's
	// on-disk size meets or exceeds this value.
	SegmentBytes int64 `json:"segment_bytes"`

	// RetentionCheckIntervalMs is the period between retention
	// reaper sweeps per partition.
	RetentionCheckIntervalMs int `json:"retention_check_interval_ms"`

	// IdleLogEvictionMs closes partition logs untouched by any produce,
	// consume, replay, or fan-out backlog read for this long; the next
	// access reopens them lazily. Frees the goroutines, file
	// descriptors, and buffers of used-then-abandoned topics. Zero
	// disables eviction.
	IdleLogEvictionMs int `json:"idle_log_eviction_ms"`
}

// FsyncMode controls how aggressively the storage layer flushes
// writes to disk.
type FsyncMode string

const (
	// FsyncPerWrite syncs after every append: maximal durability,
	// slowest throughput.
	FsyncPerWrite FsyncMode = "per_write"

	// FsyncBatched syncs on the SyncBytes / SyncIntervalMs cadence,
	// trading a bounded window of data loss for throughput.
	FsyncBatched FsyncMode = "batched"
)
