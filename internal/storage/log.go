// Package storage implements the per-partition append-only record log.
//
// Concurrency: Log is not safe for concurrent Append from multiple
// goroutines (the PRD's "one writer per partition" rule). Concurrent Reads
// alongside a single Append are safe because Read seeks before reading and
// the index is only mutated by Append.
package storage

import "os"

// Log is an append-only record log backed by a single file.
type Log struct {
	file       *os.File
	nextOffset int64
	index      map[int64]int64

	// notify is a 1-buffered channel that Append signals after every
	// successful write. Long-pollers select on NotifyC() to be woken
	// without busy-waiting. The buffered "drop on full" pattern is
	// intentional: we only need the wake-up edge, not a count.
	notify chan struct{}
}

// NewLog opens (or creates) the log file at path and replays its tail to
// rebuild the in-memory offset index. A torn record at EOF is truncated.
func NewLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	l := &Log{
		file:   f,
		index:  make(map[int64]int64),
		notify: make(chan struct{}, 1),
	}

	offset, err := l.recover()
	if err != nil {
		return nil, err
	}
	l.nextOffset = offset

	return l, nil
}
