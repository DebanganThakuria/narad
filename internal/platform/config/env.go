package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// applyEnv lets operators override individual fields without rewriting
// the config file. Each entry is intentionally explicit: there's no
// reflection magic — adding a new env override is a one-liner here.
func applyEnv(cfg *Config) error {
	if v, ok := os.LookupEnv("NARAD_HTTP_ADDR"); ok {
		cfg.HTTP.Addr = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_ADDR"); ok {
		cfg.Cluster.Addr = v
	}
	if v, ok := os.LookupEnv("NARAD_NODE_ID"); ok {
		cfg.Cluster.NodeID = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_PEERS"); ok {
		peers, err := parseClusterPeers(v)
		if err != nil {
			return fmt.Errorf("NARAD_CLUSTER_PEERS: %w", err)
		}
		cfg.Cluster.Peers = peers
	}
	if err := envDuration("NARAD_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_SHUTDOWN_GRACE", &cfg.HTTP.ShutdownGrace); err != nil {
		return err
	}
	if err := envDuration("NARAD_HTTP_MAX_CONSUME_WAIT", &cfg.HTTP.MaxConsumeWait); err != nil {
		return err
	}

	if v, ok := os.LookupEnv("NARAD_DATA_DIR"); ok {
		cfg.Storage.DataDir = v
	}
	if v, ok := os.LookupEnv("NARAD_FSYNC"); ok {
		cfg.Storage.Fsync = FsyncMode(v)
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_CODEC"); ok {
		cfg.Storage.Codec = v
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_COMPRESSION_LEVEL"); ok {
		cfg.Storage.CompressionLevel = v
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_FLUSH_BYTES"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_STORAGE_FLUSH_BYTES: %w", err)
		}
		cfg.Storage.FlushBytes = n
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_FLUSH_RECORDS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_STORAGE_FLUSH_RECORDS: %w", err)
		}
		cfg.Storage.FlushRecords = n
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_FLUSH_INTERVAL_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_STORAGE_FLUSH_INTERVAL_MS: %w", err)
		}
		cfg.Storage.FlushIntervalMs = n
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_SEGMENT_BYTES"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("NARAD_STORAGE_SEGMENT_BYTES: %w", err)
		}
		cfg.Storage.SegmentBytes = n
	}
	if v, ok := os.LookupEnv("NARAD_STORAGE_RETENTION_CHECK_INTERVAL_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_STORAGE_RETENTION_CHECK_INTERVAL_MS: %w", err)
		}
		cfg.Storage.RetentionCheckIntervalMs = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS: %w", err)
		}
		cfg.Topic.DefaultRetentionAgeMs = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS: %w", err)
		}
		cfg.Topic.DefaultVisibilityTimeoutMs = n
	}

	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_PARTITIONS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_PARTITIONS: %w", err)
		}
		cfg.Topic.DefaultPartitions = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_MAX_PARTITIONS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_MAX_PARTITIONS: %w", err)
		}
		cfg.Topic.MaxPartitions = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_REPLICATION_FACTOR"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_REPLICATION_FACTOR: %w", err)
		}
		cfg.Topic.DefaultReplicationFactor = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION: %w", err)
		}
		cfg.Topic.DefaultMaxInFlightPerPartition = n
	}
	if v, ok := os.LookupEnv("NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION: %w", err)
		}
		cfg.Topic.DefaultMaxAckedAheadPerPartition = n
	}

	if v, ok := os.LookupEnv("NARAD_LOG_LEVEL"); ok {
		cfg.Log.Level = v
	}
	if v, ok := os.LookupEnv("NARAD_LOG_FORMAT"); ok {
		cfg.Log.Format = v
	}

	return nil
}

func envDuration(key string, dst *Duration) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = Duration(d)
	return nil
}

func parseClusterPeers(raw string) ([]ClusterPeer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	peers := make([]ClusterPeer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "@")
		if !ok || strings.Contains(addr, "@") || strings.TrimSpace(id) == "" || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("invalid peer %q: want id@host:port", part)
		}
		peers = append(peers, ClusterPeer{ID: strings.TrimSpace(id), Addr: strings.TrimSpace(addr)})
	}
	return peers, nil
}
