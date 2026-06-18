package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

const clusterVoterCount = 3

// Validate enforces invariants the rest of the system relies on. Callers
// should fail fast on a Validate error — Narad refuses to start with bad
// config rather than coping at runtime.
func (c *Config) Validate() error {
	var errs []string
	appendHTTPValidationErrors(&errs, c.HTTP)
	appendClusterValidationErrors(&errs, c.HTTP, c.Cluster)
	appendStorageValidationErrors(&errs, c.Storage)
	appendTopicValidationErrors(&errs, c.Topic)
	appendLogValidationErrors(&errs, c.Log)
	if len(errs) == 0 {
		return nil
	}
	return errors.New("config: " + strings.Join(errs, "; "))
}

func appendHTTPValidationErrors(errs *[]string, cfg HTTPConfig) {
	if strings.TrimSpace(cfg.Addr) == "" {
		*errs = append(*errs, "http.addr must not be empty")
	}
	if cfg.ReadTimeout <= 0 {
		*errs = append(*errs, "http.read_timeout must be > 0")
	}
	if cfg.WriteTimeout <= 0 {
		*errs = append(*errs, "http.write_timeout must be > 0")
	}
	if cfg.IdleTimeout <= 0 {
		*errs = append(*errs, "http.idle_timeout must be > 0")
	}
	if cfg.ShutdownGrace <= 0 {
		*errs = append(*errs, "http.shutdown_grace must be > 0")
	}
	if cfg.MaxConsumeWait < 0 {
		*errs = append(*errs, "http.max_consume_wait must be >= 0")
	}
}

func appendClusterValidationErrors(errs *[]string, httpCfg HTTPConfig, cfg ClusterConfig) {
	selfAddr := strings.TrimSpace(cfg.Addr)
	selfID := strings.TrimSpace(cfg.NodeID)
	if selfAddr == "" {
		*errs = append(*errs, "cluster.addr must not be empty")
	}
	if httpCfg.Addr == cfg.Addr {
		*errs = append(*errs, "http.addr and cluster.addr must differ")
	}
	if len(cfg.Peers) == 0 {
		return
	}
	if selfID == "" {
		*errs = append(*errs, "cluster.node_id must not be empty when cluster.peers is set")
	}
	if len(cfg.Peers) != clusterVoterCount {
		*errs = append(*errs, fmt.Sprintf("cluster.peers must list exactly %d voters including self, got %d", clusterVoterCount, len(cfg.Peers)))
	}

	result := inspectClusterPeers(cfg.Peers, selfID, selfAddr)
	*errs = append(*errs, result.errors...)
	if selfID != "" && !result.foundSelfID {
		*errs = append(*errs, fmt.Sprintf("cluster.peers must include local node id %q", selfID))
	}
	if !result.foundSelfAddr {
		*errs = append(*errs, fmt.Sprintf("cluster.peers must include local cluster address %q", selfAddr))
	}
	if selfID != "" && selfAddr != "" && !result.foundSelfVoter {
		*errs = append(*errs, fmt.Sprintf("cluster.peers must include local voter %q@%s", selfID, selfAddr))
	}
}

func clusterAddrsMatch(localAddr, peerAddr string) bool {
	return netaddr.ClusterAddrMatchesPeer(localAddr, peerAddr)
}

func clusterVotersMatch(selfID, selfAddr, peerID, peerAddr string) bool {
	return peerID == selfID && clusterAddrsMatch(selfAddr, peerAddr)
}

type clusterPeerInspection struct {
	errors         []string
	foundSelfID    bool
	foundSelfAddr  bool
	foundSelfVoter bool
}

func inspectClusterPeers(peers []ClusterPeer, selfID, selfAddr string) clusterPeerInspection {
	result := clusterPeerInspection{}
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
		if id == selfID {
			result.foundSelfID = true
		}
		if clusterAddrsMatch(selfAddr, addr) {
			result.foundSelfAddr = true
		}
		if clusterVotersMatch(selfID, selfAddr, id, addr) {
			result.foundSelfVoter = true
		}
	}
	return result
}

func appendStorageValidationErrors(errs *[]string, cfg StorageConfig) {
	if strings.TrimSpace(cfg.DataDir) == "" {
		*errs = append(*errs, "storage.data_dir must not be empty")
	}
	switch cfg.Fsync {
	case FsyncPerWrite, FsyncBatched:
	default:
		*errs = append(*errs, fmt.Sprintf("storage.fsync %q is not one of [per_write, batched]", cfg.Fsync))
	}
	switch strings.ToLower(cfg.Codec) {
	case "zstd", "none":
	default:
		*errs = append(*errs, fmt.Sprintf("storage.codec %q is not one of [zstd, none]", cfg.Codec))
	}
	switch strings.ToLower(cfg.CompressionLevel) {
	case "fastest", "default", "better", "best":
	default:
		*errs = append(*errs, fmt.Sprintf("storage.compression_level %q is not one of [fastest, default, better, best]", cfg.CompressionLevel))
	}
	if cfg.FlushBytes < 0 {
		*errs = append(*errs, "storage.flush_bytes must be >= 0")
	}
	if cfg.FlushRecords < 0 {
		*errs = append(*errs, "storage.flush_records must be >= 0")
	}
	if cfg.FlushIntervalMs <= 0 {
		*errs = append(*errs, "storage.flush_interval_ms must be > 0")
	}
	if cfg.FlushBytes == 0 && cfg.FlushRecords == 0 {
		*errs = append(*errs, "at least one of storage.flush_bytes or storage.flush_records must be > 0")
	}
	if cfg.SegmentBytes < 4096 {
		*errs = append(*errs, fmt.Sprintf("storage.segment_bytes (%d) must be >= 4096", cfg.SegmentBytes))
	}
	if cfg.RetentionCheckIntervalMs <= 0 {
		*errs = append(*errs, "storage.retention_check_interval_ms must be > 0")
	}
}

func appendTopicValidationErrors(errs *[]string, cfg TopicConfig) {
	if cfg.DefaultPartitions < 3 {
		*errs = append(*errs, "topic.default_partitions must be >= 3")
	}
	if cfg.MaxPartitions <= 0 {
		*errs = append(*errs, "topic.max_partitions must be > 0")
	}
	if cfg.DefaultPartitions > cfg.MaxPartitions {
		*errs = append(*errs, fmt.Sprintf("topic.default_partitions (%d) must not exceed topic.max_partitions (%d)", cfg.DefaultPartitions, cfg.MaxPartitions))
	}
	if cfg.DefaultReplicationFactor < 2 {
		*errs = append(*errs, "topic.default_replication_factor must be >= 2")
	}
	if cfg.DefaultRetentionAgeMs < 0 {
		*errs = append(*errs, "topic.default_retention_age_ms must be >= 0")
	}
	if cfg.DefaultVisibilityTimeoutMs < 0 {
		*errs = append(*errs, "topic.default_visibility_timeout_ms must be >= 0")
	}
}

func appendLogValidationErrors(errs *[]string, cfg LogConfig) {
	switch strings.ToLower(cfg.Level) {
	case "debug", "info", "warn", "error":
	default:
		*errs = append(*errs, fmt.Sprintf("log.level %q is not one of [debug, info, warn, error]", cfg.Level))
	}
	switch strings.ToLower(cfg.Format) {
	case "json", "text":
	default:
		*errs = append(*errs, fmt.Sprintf("log.format %q is not one of [json, text]", cfg.Format))
	}
}
