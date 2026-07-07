package topic

// Fan-out value types shared between the broker engine (which reads
// committed parent records) and the cluster fan-out runner (which
// re-keys and commits them to children).

// KeyedRecord is one committed record with its produce key recovered
// from the stored envelope. Records committed before the keyed
// envelope existed carry an empty key.
type KeyedRecord struct {
	Key     string
	Payload []byte
}

// FanoutSlab is one contiguous read of committed records from a parent
// partition, plus the log watermarks the cursor needs to advance and
// to detect drop-behind.
type FanoutSlab struct {
	Records []KeyedRecord
	// NextOffset is the offset the next read should start from: one
	// past the last returned record, or past any dropped/skipped range
	// when Records is empty.
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
}

// FanoutTailOffset, passed as a slab read's fromOffset, means "start
// at the parent's current committed tail" — the no-backfill starting
// position of a newly attached child's cursor.
const FanoutTailOffset int64 = -1
