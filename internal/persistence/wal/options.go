package wal

import "time"

const (
	defaultSegmentBytes = 64 << 20
	defaultSyncInterval = 10 * time.Millisecond
	defaultMaxRecord    = 16 << 20
)

// Options configures a Log. Zero values pick sensible defaults.
type Options struct {
	// SegmentBytes caps a segment file's size; an append that would
	// exceed it rolls the log to a new segment.
	SegmentBytes int64

	// SyncInterval is the backstop timer for the sync loop. Every Append
	// wakes the loop immediately (group commit), so this only bounds how
	// long buffered records can wait if a wakeup is ever missed.
	SyncInterval time.Duration

	// MaxRecord is the maximum payload size accepted by Append and the
	// upper bound trusted when validating frame lengths during recovery.
	MaxRecord int
}

func normalizeOptions(opts Options) Options {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaultSyncInterval
	}
	if opts.MaxRecord <= 0 {
		opts.MaxRecord = defaultMaxRecord
	}
	return opts
}
