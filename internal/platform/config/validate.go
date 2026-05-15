package config

import (
	"errors"
	"fmt"
	"strings"
)

// Validate enforces invariants the rest of the system relies on. Callers
// should fail fast on a Validate error — Narad refuses to start with bad
// config rather than coping at runtime.
func (c *Config) Validate() error {
	var errs []string

	if strings.TrimSpace(c.HTTP.Addr) == "" {
		errs = append(errs, "http.addr must not be empty")
	}
	if c.HTTP.ReadTimeout <= 0 {
		errs = append(errs, "http.read_timeout must be > 0")
	}
	if c.HTTP.WriteTimeout <= 0 {
		errs = append(errs, "http.write_timeout must be > 0")
	}
	if c.HTTP.IdleTimeout <= 0 {
		errs = append(errs, "http.idle_timeout must be > 0")
	}
	if c.HTTP.ShutdownGrace <= 0 {
		errs = append(errs, "http.shutdown_grace must be > 0")
	}
	if c.HTTP.MaxConsumeWait < 0 {
		errs = append(errs, "http.max_consume_wait must be >= 0")
	}

	if strings.TrimSpace(c.Cluster.Addr) == "" {
		errs = append(errs, "cluster.addr must not be empty")
	}
	if c.HTTP.Addr == c.Cluster.Addr {
		errs = append(errs, "http.addr and cluster.addr must differ")
	}

	if strings.TrimSpace(c.Storage.DataDir) == "" {
		errs = append(errs, "storage.data_dir must not be empty")
	}
	switch c.Storage.Fsync {
	case FsyncPerWrite, FsyncBatched:
	default:
		errs = append(errs, fmt.Sprintf("storage.fsync %q is not one of [per_write, batched]", c.Storage.Fsync))
	}
	switch strings.ToLower(c.Storage.Codec) {
	case "zstd", "none":
	default:
		errs = append(errs, fmt.Sprintf("storage.codec %q is not one of [zstd, none]", c.Storage.Codec))
	}
	switch strings.ToLower(c.Storage.CompressionLevel) {
	case "fastest", "default", "better", "best":
	default:
		errs = append(errs, fmt.Sprintf("storage.compression_level %q is not one of [fastest, default, better, best]", c.Storage.CompressionLevel))
	}
	if c.Storage.FlushBytes < 0 {
		errs = append(errs, "storage.flush_bytes must be >= 0")
	}
	if c.Storage.FlushRecords < 0 {
		errs = append(errs, "storage.flush_records must be >= 0")
	}
	if c.Storage.FlushIntervalMs <= 0 {
		errs = append(errs, "storage.flush_interval_ms must be > 0")
	}
	if c.Storage.FlushBytes == 0 && c.Storage.FlushRecords == 0 {
		errs = append(errs, "at least one of storage.flush_bytes or storage.flush_records must be > 0")
	}
	if c.Storage.SegmentBytes < 4096 {
		errs = append(errs, fmt.Sprintf("storage.segment_bytes (%d) must be >= 4096", c.Storage.SegmentBytes))
	}
	if c.Storage.RetentionCheckIntervalMs <= 0 {
		errs = append(errs, "storage.retention_check_interval_ms must be > 0")
	}

	if c.Topic.DefaultPartitions <= 0 {
		errs = append(errs, "topic.default_partitions must be > 0")
	}
	if c.Topic.MaxPartitions <= 0 {
		errs = append(errs, "topic.max_partitions must be > 0")
	}
	if c.Topic.DefaultPartitions > c.Topic.MaxPartitions {
		errs = append(errs, fmt.Sprintf("topic.default_partitions (%d) must not exceed topic.max_partitions (%d)",
			c.Topic.DefaultPartitions, c.Topic.MaxPartitions))
	}
	if c.Topic.DefaultReplicationFactor <= 0 {
		errs = append(errs, "topic.default_replication_factor must be > 0")
	}
	if c.Topic.DefaultRetentionAgeMs < 0 {
		errs = append(errs, "topic.default_retention_age_ms must be >= 0")
	}
	if c.Topic.DefaultVisibilityTimeoutMs < 0 {
		errs = append(errs, "topic.default_retention_bytes must be >= 0")
	}

	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("log.level %q is not one of [debug, info, warn, error]", c.Log.Level))
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Sprintf("log.format %q is not one of [json, text]", c.Log.Format))
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New("config: " + strings.Join(errs, "; "))
}
