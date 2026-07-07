package topic

// Fan-out value types shared between the broker engine (which reads
// committed parent records) and the cluster fan-out runner (which
// re-keys and commits them to children).

import "time"

// KeyedRecord is one committed record with the produce key and commit
// timestamp recovered from the stored envelope.
type KeyedRecord struct {
	Key string
	// CommittedAtUnixMs is when the record was durably committed to
	// its partition (assigned by the partition owner). Delay children
	// anchor due times to it.
	CommittedAtUnixMs int64
	Payload           []byte
}

// FanoutReadOpts bounds one committed-slab read from a parent
// partition.
type FanoutReadOpts struct {
	// FromOffset is where the read starts; FanoutTailOffset means the
	// current committed tail (the no-backfill anchor of a fresh
	// cursor).
	FromOffset int64
	// MaxRecords / MaxBytes cap the slab; it fills at either bound.
	MaxRecords int
	MaxBytes   int64
	// Wait long-polls the partition's notify broadcast when nothing is
	// readable at FromOffset yet.
	Wait time.Duration
	// MaxCommittedAt, when positive, stops the read at the first
	// record committed after it — the due gate for delay children
	// (records commit in time order per partition, so the deliverable
	// set is always a prefix). The blocking record's commit time is
	// reported via FanoutSlab.BlockedUntilUnixMs.
	MaxCommittedAt int64
}

// FanoutSlab is one contiguous read of committed records from a parent
// partition, plus the log watermarks the cursor needs to advance and
// to detect drop-behind.
type FanoutSlab struct {
	Records []KeyedRecord
	// NextOffset is the offset the next read should start from: one
	// past the last returned record, or past any dropped/skipped range
	// when Records is empty. A read stopped by MaxCommittedAt does NOT
	// advance past the blocking record.
	NextOffset int64
	// OldestOffset is the lowest offset still retained.
	OldestOffset int64
	// HighWatermark is the exclusive committed frontier at read time.
	HighWatermark int64
	// DroppedBehind counts offsets the read had to skip because they
	// aged out of retention below the requested start (drop-behind =
	// data loss for the child).
	DroppedBehind int64
	// SkippedCorrupt counts offsets skipped because their records are
	// permanently unreadable (corrupt frame or a recovery gap).
	SkippedCorrupt int64
	// BlockedUntilUnixMs, when non-zero, is the commit time of the
	// record that MaxCommittedAt stopped the read at: nothing more is
	// deliverable until (that time + the child's delay).
	BlockedUntilUnixMs int64
}

// FanoutTailOffset, passed as a slab read's fromOffset, means "start
// at the parent's current committed tail" — the no-backfill starting
// position of a newly attached child's cursor.
const FanoutTailOffset int64 = -1

// FanoutCursorStat reports one cursor's position for lag accounting:
// the child's fan-out lag on this parent partition is
// HighWatermark - NextOffset.
type FanoutCursorStat struct {
	Child         string `json:"child"`
	Partition     int    `json:"partition"`
	NextOffset    int64  `json:"next_offset"`
	HighWatermark int64  `json:"high_watermark"`
}
