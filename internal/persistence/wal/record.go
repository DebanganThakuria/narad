package wal

// RecordID locates a record in the log. Seqs are dense and strictly
// increasing across segments; SegmentBase is the seq of the first record
// the containing segment may hold, and Offset is the byte position of
// the record's frame within that segment file.
type RecordID struct {
	SegmentBase uint64
	Offset      int64
	Seq         uint64
}

// Cursor is a resume position for ReplayFromCursor. Replay starts at
// Offset within the segment whose base is SegmentBase and delivers
// records with seq >= Seq.
type Cursor struct {
	SegmentBase uint64
	Offset      int64
	Seq         uint64
}

// Record is a single log entry as delivered by replay.
type Record struct {
	ID      RecordID
	Payload []byte
}

// CursorAfter returns the cursor positioned immediately after record,
// suitable for resuming replay without re-reading it.
func CursorAfter(record Record) Cursor {
	return Cursor{
		SegmentBase: record.ID.SegmentBase,
		Offset:      record.ID.Offset + frameHeaderSize + int64(len(record.Payload)),
		Seq:         record.ID.Seq + 1,
	}
}
