package config

// TopicConfig governs the defaults and bounds applied when topics are
// created. Defaults are ergonomic; advanced operators lift the bounds
// via the JSON file or env vars.
type TopicConfig struct {
	DefaultPartitions                int   `json:"default_partitions"`
	MaxPartitions                    int   `json:"max_partitions"`
	DefaultReplicationFactor         int   `json:"default_replication_factor"`
	DefaultRetentionAgeMs            int64 `json:"default_retention_age_ms"`
	DefaultVisibilityTimeoutMs       int64 `json:"default_visibility_timeout_ms"`
	DefaultMaxInFlightPerPartition   int64 `json:"default_max_in_flight_per_partition"`
	DefaultMaxAckedAheadPerPartition int64 `json:"default_max_acked_ahead_per_partition"`
}
