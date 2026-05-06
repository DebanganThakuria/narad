package storage

// Close flushes and releases the underlying file handle. After Close,
// further Append/Read calls return errors from the OS layer.
func (l *Log) Close() error {
	return l.file.Close()
}
