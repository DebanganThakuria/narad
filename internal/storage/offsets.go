package storage

// LatestOffset returns the offset of the most recently appended record.
// Returns 0 for an empty log — use NextOffset to disambiguate "empty"
// from "one record at offset 0".
func (l *Log) LatestOffset() int64 {
	if l.nextOffset == 0 {
		return 0
	}
	return l.nextOffset - 1
}

// NextOffset returns the offset that will be assigned to the next
// successful Append. Equivalently, it is the number of records currently
// in the log. Use this — not LatestOffset — when you need to detect an
// empty log unambiguously.
func (l *Log) NextOffset() int64 {
	return l.nextOffset
}
