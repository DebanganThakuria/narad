package storage

// LatestOffset is the offset of the most recently appended record, or
// 0 for an empty log. Use NextOffset to disambiguate "empty" from
// "one record at offset 0".
func (l *Log) LatestOffset() int64 {
	next := l.buffer.nextOffsetSnapshot()
	if next == 0 {
		return 0
	}
	return next - 1
}

// NextOffset is the offset that will be assigned to the next
// successful Append (== total records ever appended, including
// buffered).
func (l *Log) NextOffset() int64 {
	return l.buffer.nextOffsetSnapshot()
}
