// Package topic defines the value types used across the broker,
// metastore, and partition layers. There is no behavior here — these
// are wire- and storage-stable structs.
//
// All wall-clock fields are Unix-seconds (int64) so the wire format
// is timezone-independent and round-trips through JSON/SQLite without
// any layout-dependent encoding.
package topic

// Topic is the user-facing logical stream. Partitions can grow via
// IncreaseTopicPartitions (never shrink); Retention, visibility, and the
// in-flight caps can be altered post-create without affecting existing
// data. Narad has no follower replication: each partition has a single
// owner whose durable log is the sole copy of the data.
//
// MaxInFlightPerPartition bounds the number of simultaneously-reserved
// offsets per partition (consumer-side parallelism cap). Once reached,
// ReserveNext returns "no message" until a Commit frees a slot or the
// visibility timeout expires entries.
//
// MaxAckedAheadPerPartition bounds the sparse set of acked-but-not-yet-
// contiguous offsets per partition. When full, the broker refuses
// further out-of-order acks to force back-pressure when the head of
// the queue is genuinely stuck (a poison message no one can process).
//
// Zero values for retention / visibility / caps inherit from the broker's
// TopicConfig defaults at create time.
type Topic struct {
	Name                      string `json:"name"`
	Partitions                int    `json:"partitions"`
	RetentionMs               int64  `json:"retention_ms"`
	VisibilityTimeoutMs       int64  `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64  `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64  `json:"max_acked_ahead_per_partition"`
	CreatedAt                 int64  `json:"created_at"`
}

// Details is the response shape for "describe a topic": the topic
// record plus per-partition runtime stats.
type Details struct {
	Topic
	Partitions []PartitionStats `json:"partition_stats"`
}

// PartitionStats reports runtime storage stats for one partition.
type PartitionStats struct {
	Index    int `json:"index"`
	Segments int `json:"segments"`
	// OldestOffset is the lowest offset still retained on disk.
	OldestOffset int64 `json:"oldest_offset"`
	// NextOffset is the offset the next appended record will receive
	// (total records ever appended). It can briefly lead HighWatermark
	// while a record is being committed.
	NextOffset int64 `json:"next_offset"`
	// HighWatermark is the exclusive upper bound of records visible to
	// consumers — the durably-committed frontier.
	HighWatermark   int64 `json:"high_watermark"`
	SizeBytes       int64 `json:"size_bytes"`
	OldestSegmentAt int64 `json:"oldest_segment_at,omitempty"`
}
