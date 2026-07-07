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
	// Owner is the username that created the topic. Alter/delete
	// require the owner or an admin. Empty when security is disabled.
	Owner string `json:"owner,omitempty"`

	// Role, Children, and Parent describe the topic's position in
	// fan-out. They are managed exclusively by the metastore's
	// attach/detach/delete operations — a config update never changes
	// them. The zero value reads as standalone (see EffectiveRole).
	Role Role `json:"role,omitempty"`
	// Children is the set of child topics this parent fans out to,
	// in attach order. Parent role only.
	Children []string `json:"children,omitempty"`
	// Parent is the topic this child receives fan-out from. Child
	// role only.
	Parent string `json:"parent,omitempty"`
	// AttachEpoch identifies this particular attachment of the child
	// to its parent (child role only; assigned at attach, cleared at
	// detach). Fan-out cursor state is scoped to the epoch, so a
	// detach followed by a re-attach starts fresh at the parent's tail
	// instead of resuming — and replaying from — the dead cursor.
	AttachEpoch string `json:"attach_epoch,omitempty"`
	// FanoutDelayMs makes this a DELAY child (child role only): every
	// parent record is delivered to it only once
	// parentCommitTime + FanoutDelayMs has passed. Zero means deliver
	// immediately (a normal child). Set at attach, immutable while
	// attached (detach and re-attach to change it), cleared at detach.
	// Direct produce to a delayed child is rejected — the delay is a
	// guarantee of the topic, not a property of one write path.
	FanoutDelayMs int64 `json:"fanout_delay_ms,omitempty"`
}

// Role classifies a topic's position in fan-out. Roles are exclusive
// and flat: a child never has children of its own, and a parent is
// never itself a child.
type Role string

// The three fan-out roles a topic can hold.
const (
	RoleStandalone Role = "standalone"
	RoleParent     Role = "parent"
	RoleChild      Role = "child"
)

// MaxChildrenPerParent caps how many children a parent may fan out to.
// It matches the topic partition cap and is a safety rail against
// runaway write amplification: a parent's sustainable produce rate is
// roughly cluster capacity divided by (children + 1).
const MaxChildrenPerParent = 108

// MaxFanoutDelayMs caps a delay child's delay at one year. Beyond
// being a sanity rail (a misconfigured nanoseconds-for-milliseconds
// delay would otherwise schedule delivery decades out), the cap keeps
// due-time arithmetic comfortably inside int64.
const MaxFanoutDelayMs int64 = 365 * 24 * 60 * 60 * 1000

// MinRetentionMs is the minimum effective retention for every topic
// (one hour). The parent's retained log is the fan-out buffer for
// lagging children, so the floor guarantees every child at least an
// hour of outage tolerance before drop-behind can lose messages. It
// applies uniformly to all topics; zero retention (keep forever) is
// unaffected.
const MinRetentionMs int64 = 60 * 60 * 1000

// EffectiveRole maps the zero value to RoleStandalone: a topic record
// with no explicit role is an ordinary standalone topic.
func (t Topic) EffectiveRole() Role {
	if t.Role == "" {
		return RoleStandalone
	}
	return t.Role
}

// IsParent reports whether the topic fans out to children.
func (t Topic) IsParent() bool { return t.EffectiveRole() == RoleParent }

// IsChild reports whether the topic receives fan-out from a parent.
func (t Topic) IsChild() bool { return t.EffectiveRole() == RoleChild }

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
