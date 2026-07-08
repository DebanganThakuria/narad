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
	if v, ok := os.LookupEnv("NARAD_HTTP_PPROF_ADDR"); ok {
		cfg.HTTP.PprofAddr = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_ADDR"); ok {
		cfg.Cluster.Addr = v
	}
	if v, ok := os.LookupEnv("NARAD_NODE_ID"); ok {
		cfg.Cluster.NodeID = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_INITIAL_MEMBERS"); ok {
		cfg.Cluster.InitialMembers = splitNonEmpty(v)
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
	if err := envInt64("NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS", &cfg.Topic.DefaultRetentionAgeMs); err != nil {
		return err
	}
	if err := envInt64("NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS", &cfg.Topic.DefaultVisibilityTimeoutMs); err != nil {
		return err
	}
	if err := envInt("NARAD_TOPIC_DEFAULT_PARTITIONS", &cfg.Topic.DefaultPartitions); err != nil {
		return err
	}
	if err := envInt("NARAD_TOPIC_MAX_PARTITIONS", &cfg.Topic.MaxPartitions); err != nil {
		return err
	}
	if err := envInt64("NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION", &cfg.Topic.DefaultMaxInFlightPerPartition); err != nil {
		return err
	}
	if err := envInt64("NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION", &cfg.Topic.DefaultMaxAckedAheadPerPartition); err != nil {
		return err
	}
	if err := envInt("NARAD_FANOUT_MAX_BATCH_RECORDS", &cfg.Fanout.MaxBatchRecords); err != nil {
		return err
	}
	if err := envInt64("NARAD_FANOUT_MAX_BATCH_BYTES", &cfg.Fanout.MaxBatchBytes); err != nil {
		return err
	}
	if err := envInt64("NARAD_FANOUT_LINGER_MS", &cfg.Fanout.LingerMs); err != nil {
		return err
	}

	if v, ok := os.LookupEnv("NARAD_LOG_LEVEL"); ok {
		cfg.Log.Level = v
	}
	if v, ok := os.LookupEnv("NARAD_LOG_FORMAT"); ok {
		cfg.Log.Format = v
	}

	if v, ok := os.LookupEnv("NARAD_SECURITY_ENABLED"); ok {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("NARAD_SECURITY_ENABLED: %w", err)
		}
		cfg.Security.Enabled = enabled
	}
	if v, ok := os.LookupEnv("NARAD_ADMIN_PASSWORD"); ok {
		cfg.Security.AdminPassword = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_SECRET"); ok {
		cfg.Security.ClusterSecret = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_TLS_CERT_FILE"); ok {
		cfg.Security.ClusterTLSCertFile = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_TLS_KEY_FILE"); ok {
		cfg.Security.ClusterTLSKeyFile = v
	}
	if v, ok := os.LookupEnv("NARAD_CLUSTER_TLS_CA_FILE"); ok {
		cfg.Security.ClusterTLSCAFile = v
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

func envInt(key string, dst *int) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = n
	return nil
}

func envInt64(key string, dst *int64) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = n
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

// splitNonEmpty splits a comma list, trimming whitespace and dropping
// empty entries.
func splitNonEmpty(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
