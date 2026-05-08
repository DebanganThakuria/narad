package config

// TopicConfig governs the defaults and bounds applied when topics are
// created. Defaults are ergonomic; advanced operators lift the bounds
// via the JSON file or env vars.
type TopicConfig struct {
	DefaultPartitions        int `json:"default_partitions"`
	MaxPartitions            int `json:"max_partitions"`
	DefaultReplicationFactor int `json:"default_replication_factor"`

	// Per-topic retention defaults — applied to topics that don't
	// explicitly set their own. Zero in either disables that bound.
	DefaultRetentionAgeMs int64 `json:"default_retention_age_ms"`
	DefaultRetentionBytes int64 `json:"default_retention_bytes"`
}
