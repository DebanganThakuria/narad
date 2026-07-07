package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

const clusterVoterCount = 3

// Validate enforces invariants the rest of the system relies on. Callers
// should fail fast on a Validate error — Narad refuses to start with bad
// config rather than coping at runtime.
func (c *Config) Validate() error {
	var errs []string
	errs = append(errs, httpValidationErrors(c.HTTP)...)
	errs = append(errs, clusterValidationErrors(c.HTTP, c.Cluster)...)
	errs = append(errs, storageValidationErrors(c.Storage)...)
	errs = append(errs, topicValidationErrors(c.Topic)...)
	errs = append(errs, fanoutValidationErrors(c.Fanout)...)
	errs = append(errs, logValidationErrors(c.Log)...)
	errs = append(errs, securityValidationErrors(c.Security, c.Cluster)...)
	if len(errs) == 0 {
		return nil
	}
	return errors.New("config: " + strings.Join(errs, "; "))
}

func httpValidationErrors(cfg HTTPConfig) []string {
	var errs []string
	if strings.TrimSpace(cfg.Addr) == "" {
		errs = append(errs, "http.addr must not be empty")
	}
	if cfg.ReadTimeout <= 0 {
		errs = append(errs, "http.read_timeout must be > 0")
	}
	if cfg.WriteTimeout <= 0 {
		errs = append(errs, "http.write_timeout must be > 0")
	}
	if cfg.IdleTimeout <= 0 {
		errs = append(errs, "http.idle_timeout must be > 0")
	}
	if cfg.ShutdownGrace <= 0 {
		errs = append(errs, "http.shutdown_grace must be > 0")
	}
	if cfg.MaxConsumeWait < 0 {
		errs = append(errs, "http.max_consume_wait must be >= 0")
	}
	// A long-poll that actually waits max_consume_wait must still fit
	// inside the server write deadline and the shutdown grace window,
	// or every waiting consume gets killed mid-response.
	if cfg.WriteTimeout > 0 && cfg.MaxConsumeWait >= cfg.WriteTimeout {
		errs = append(errs, fmt.Sprintf("http.max_consume_wait (%s) must be < http.write_timeout (%s)", cfg.MaxConsumeWait, cfg.WriteTimeout))
	}
	if cfg.ShutdownGrace > 0 && cfg.MaxConsumeWait > cfg.ShutdownGrace {
		errs = append(errs, fmt.Sprintf("http.max_consume_wait (%s) must be <= http.shutdown_grace (%s)", cfg.MaxConsumeWait, cfg.ShutdownGrace))
	}
	return errs
}

func clusterValidationErrors(httpCfg HTTPConfig, cfg ClusterConfig) []string {
	var errs []string
	selfAddr := strings.TrimSpace(cfg.Addr)
	selfID := strings.TrimSpace(cfg.NodeID)
	if selfAddr == "" {
		errs = append(errs, "cluster.addr must not be empty")
	}
	if httpCfg.Addr == cfg.Addr {
		errs = append(errs, "http.addr and cluster.addr must differ")
	}
	if len(cfg.Peers) == 0 {
		return errs
	}
	if selfID == "" {
		errs = append(errs, "cluster.node_id must not be empty when cluster.peers is set")
	}
	if len(cfg.Peers) != clusterVoterCount {
		errs = append(errs, fmt.Sprintf("cluster.peers must list exactly %d voters including self, got %d", clusterVoterCount, len(cfg.Peers)))
	}

	result := inspectPeers(cfg.Peers, selfID, selfAddr)
	errs = append(errs, result.errors...)
	if selfID != "" && !result.foundSelfID {
		errs = append(errs, fmt.Sprintf("cluster.peers must include local node id %q", selfID))
	}
	if !result.foundSelfAddr {
		errs = append(errs, fmt.Sprintf("cluster.peers must include local cluster address %q", selfAddr))
	}
	if selfID != "" && selfAddr != "" && !result.foundSelfVoter {
		errs = append(errs, fmt.Sprintf("cluster.peers must include local voter %q@%s", selfID, selfAddr))
	}
	return errs
}

// peerInspection is what one pass over cluster.peers reveals: per-peer
// validation errors plus whether the local node appears in the voter
// list by id, by address, and as a full id@addr voter entry.
type peerInspection struct {
	errors         []string
	foundSelfID    bool
	foundSelfAddr  bool
	foundSelfVoter bool
}

func inspectPeers(peers []ClusterPeer, selfID, selfAddr string) peerInspection {
	var result peerInspection
	seenIDs := make(map[string]struct{}, len(peers))
	seenAddrs := make(map[string]struct{}, len(peers))
	for i, peer := range peers {
		id := strings.TrimSpace(peer.ID)
		addr := strings.TrimSpace(peer.Addr)
		if id == "" {
			result.errors = append(result.errors, fmt.Sprintf("cluster.peers[%d].id must not be empty", i))
			continue
		}
		if addr == "" {
			result.errors = append(result.errors, fmt.Sprintf("cluster.peers[%d].addr must not be empty", i))
			continue
		}
		if _, ok := seenIDs[id]; ok {
			result.errors = append(result.errors, fmt.Sprintf("cluster peer id %q must be unique", id))
		} else {
			seenIDs[id] = struct{}{}
		}
		if _, ok := seenAddrs[addr]; ok {
			result.errors = append(result.errors, fmt.Sprintf("cluster peer addr %q must be unique", addr))
		} else {
			seenAddrs[addr] = struct{}{}
		}

		addrMatchesSelf := netaddr.ClusterAddrMatchesPeer(selfAddr, addr)
		if id == selfID {
			result.foundSelfID = true
		}
		if addrMatchesSelf {
			result.foundSelfAddr = true
		}
		if id == selfID && addrMatchesSelf {
			result.foundSelfVoter = true
		}
	}
	return result
}

func storageValidationErrors(cfg StorageConfig) []string {
	var errs []string
	if strings.TrimSpace(cfg.DataDir) == "" {
		errs = append(errs, "storage.data_dir must not be empty")
	}
	switch cfg.Fsync {
	case FsyncPerWrite, FsyncBatched:
	default:
		errs = append(errs, fmt.Sprintf("storage.fsync %q is not one of [per_write, batched]", cfg.Fsync))
	}
	switch strings.ToLower(cfg.Codec) {
	case "zstd", "none":
	default:
		errs = append(errs, fmt.Sprintf("storage.codec %q is not one of [zstd, none]", cfg.Codec))
	}
	switch strings.ToLower(cfg.CompressionLevel) {
	case "fastest", "default", "better", "best":
	default:
		errs = append(errs, fmt.Sprintf("storage.compression_level %q is not one of [fastest, default, better, best]", cfg.CompressionLevel))
	}
	if cfg.FlushBytes < 0 {
		errs = append(errs, "storage.flush_bytes must be >= 0")
	}
	if cfg.FlushRecords < 0 {
		errs = append(errs, "storage.flush_records must be >= 0")
	}
	if cfg.FlushIntervalMs <= 0 {
		errs = append(errs, "storage.flush_interval_ms must be > 0")
	}
	if cfg.FlushBytes == 0 && cfg.FlushRecords == 0 {
		errs = append(errs, "at least one of storage.flush_bytes or storage.flush_records must be > 0")
	}
	if cfg.SyncIntervalMs <= 0 {
		errs = append(errs, "storage.sync_interval_ms must be > 0")
	}
	if cfg.SyncBytes < 0 {
		errs = append(errs, "storage.sync_bytes must be >= 0")
	}
	if cfg.HighWatermarkSyncIntervalMs <= 0 {
		errs = append(errs, "storage.high_watermark_sync_interval_ms must be > 0")
	}
	if cfg.IngressWALSyncIntervalMs <= 0 {
		errs = append(errs, "storage.ingress_wal_sync_interval_ms must be > 0")
	}
	if cfg.SegmentBytes < 4096 {
		errs = append(errs, fmt.Sprintf("storage.segment_bytes (%d) must be >= 4096", cfg.SegmentBytes))
	}
	if cfg.RetentionCheckIntervalMs <= 0 {
		errs = append(errs, "storage.retention_check_interval_ms must be > 0")
	}
	return errs
}

func topicValidationErrors(cfg TopicConfig) []string {
	var errs []string
	if cfg.DefaultPartitions < 3 {
		errs = append(errs, "topic.default_partitions must be >= 3")
	}
	if cfg.MaxPartitions <= 0 {
		errs = append(errs, "topic.max_partitions must be > 0")
	}
	if cfg.DefaultPartitions > cfg.MaxPartitions {
		errs = append(errs, fmt.Sprintf("topic.default_partitions (%d) must not exceed topic.max_partitions (%d)", cfg.DefaultPartitions, cfg.MaxPartitions))
	}
	if cfg.DefaultRetentionAgeMs < 0 {
		errs = append(errs, "topic.default_retention_age_ms must be >= 0")
	}
	if cfg.DefaultRetentionAgeMs > 0 && cfg.DefaultRetentionAgeMs < topic.MinRetentionMs {
		errs = append(errs, fmt.Sprintf("topic.default_retention_age_ms (%d) must be >= %d (1 hour) or 0 (keep forever)",
			cfg.DefaultRetentionAgeMs, topic.MinRetentionMs))
	}
	if cfg.DefaultVisibilityTimeoutMs < 0 {
		errs = append(errs, "topic.default_visibility_timeout_ms must be >= 0")
	}
	if cfg.DefaultMaxInFlightPerPartition <= 0 {
		errs = append(errs, "topic.default_max_in_flight_per_partition must be > 0")
	}
	if cfg.DefaultMaxAckedAheadPerPartition <= 0 {
		errs = append(errs, "topic.default_max_acked_ahead_per_partition must be > 0")
	}
	return errs
}

func fanoutValidationErrors(cfg FanoutConfig) []string {
	var errs []string
	if cfg.MaxBatchRecords <= 0 {
		errs = append(errs, "fanout.max_batch_records must be > 0")
	}
	if cfg.MaxBatchBytes <= 0 {
		errs = append(errs, "fanout.max_batch_bytes must be > 0")
	}
	if cfg.LingerMs < 0 {
		errs = append(errs, "fanout.linger_ms must be >= 0")
	}
	return errs
}

func logValidationErrors(cfg LogConfig) []string {
	var errs []string
	switch strings.ToLower(cfg.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("log.level %q is not one of [debug, info, warn, error]", cfg.Level))
	}
	switch strings.ToLower(cfg.Format) {
	case "json", "text":
	default:
		errs = append(errs, fmt.Sprintf("log.format %q is not one of [json, text]", cfg.Format))
	}
	return errs
}

func securityValidationErrors(cfg SecurityConfig, cluster ClusterConfig) []string {
	var errs []string
	// A multi-node cluster with security on must set a cluster secret,
	// otherwise the node-to-node port would be the unauthenticated way
	// around RBAC. Single-node clusters (no peers) don't expose it.
	if cfg.Enabled && len(cluster.Peers) > 0 && strings.TrimSpace(cfg.ClusterSecret) == "" {
		errs = append(errs, "security.cluster_secret (NARAD_CLUSTER_SECRET) is required when security is enabled with cluster peers")
	}
	return errs
}
