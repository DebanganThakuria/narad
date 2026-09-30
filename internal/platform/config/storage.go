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

	// HighWatermarkSyncIntervalMs is deprecated and has no effect: an
	// open log no longer persists its high-watermark while it runs (the
	// hwm file is emptied before the first advance and written exactly
	// at Close), so there is no deferred persist to batch. It is kept,
	// and any value accepted, so existing configs still load.
	HighWatermarkSyncIntervalMs int `json:"high_watermark_sync_interval_ms"`

	// ConsumerOffsetCommitIntervalMs is the durability interval of
	// acked consumer frontiers and acked-ahead sets (consumer.ahead,
	// consumer.offset). Every 100ms, or every interval when it is
	// shorter, the changed ones are written to the page cache, so a
	// process crash redelivers about 100ms of acks; every interval each
	// partition acked since is synced once (fdatasync on Linux, fsync
	// plus one F_FULLFSYNC per tick on macOS), so a power loss or kernel
	// crash redelivers at most about the interval plus 100ms. Both are
	// within the at-least-once contract; a graceful stop redelivers
	// none. 100 keeps the power-loss window of releases before the
	// two-cadence committer. It is its own setting rather than
	// FlushIntervalMs so tuning the storage flush does not change how
	// often offsets are synced.
	ConsumerOffsetCommitIntervalMs int `json:"consumer_offset_commit_interval_ms"`

	// IngressWALSyncIntervalMs is the backstop cadence for the ingress WAL
	// sync loop. Appends wake the loop immediately (group commit), so this
	// only bounds how long buffered records can wait if a wakeup is missed.
	IngressWALSyncIntervalMs int `json:"ingress_wal_sync_interval_ms"`

	// IngressWALPrealloc prepares ingress WAL segments ahead of use:
	// the next segment is created and zero-filled to full size off the
	// append path, so a group commit overwrites allocated blocks and its
	// sync is data-only instead of also committing the inode through the
	// file system journal (ext4, XFS). Off by default: it changes crash
	// recovery (a torn write inside a prepared segment is truncated
	// rather than refused, and a binary from before preparation refuses
	// such a segment), costs up to two segments of preallocated disk,
	// and its win is measured on ext4 only. See wal.SegmentPrealloc.
	IngressWALPrealloc bool `json:"ingress_wal_prealloc"`

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

	// ColdRetentionWalkMs is how often the node walks the partition
	// directories on disk and reaps expired segments of partitions whose
	// log is NOT open (idle-evicted, or never opened since a restart).
	// The shared reaper only sees open logs, so without this walk an
	// idle topic keeps its expired data until something touches it.
	// Zero disables the walk.
	ColdRetentionWalkMs int `json:"cold_retention_walk_ms"`
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
