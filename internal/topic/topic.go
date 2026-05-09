// Package topic defines the value types used across the broker,
// metastore, and partition layers. There is no behavior here — these
// are wire- and storage-stable structs.
package topic

import "time"

// Topic is the user-facing logical stream. ReplicationFactor is fixed
// at create time; Partitions can grow via IncreaseTopicPartitions
// (never shrink); Retention can be altered post-create (future) without
// affecting existing data.
type Topic struct {
	Name                string    `json:"name"`
	Partitions          int       `json:"partitions"`
	ReplicationFactor   int       `json:"replication_factor"`
	RetentionMs         int64     `json:"retention_ms"`
	VisibilityTimeoutMs int64     `json:"visibility_timeout_ms"`
	CreatedAt           time.Time `json:"created_at"`
}

// Details is the response shape for "describe a topic": the topic
// record plus per-partition runtime stats.
type Details struct {
	Topic
	Partitions []PartitionStats `json:"partition_stats"`
}

type PartitionStats struct {
	Index           int       `json:"index"`
	Segments        int       `json:"segments"`
	OldestOffset    int64     `json:"oldest_offset"`
	NextOffset      int64     `json:"next_offset"`
	SizeBytes       int64     `json:"size_bytes"`
	OldestSegmentAt time.Time `json:"oldest_segment_at,omitzero"`
}
