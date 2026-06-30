package wal

import (
	"log/slog"
	"time"
)

const (
	defaultSegmentBytes = 64 << 20
	defaultSyncInterval = 10 * time.Millisecond
	defaultMaxRecord    = 16 << 20
)

type Options struct {
	SegmentBytes int64
	SyncInterval time.Duration
	MaxRecord    int
	// Logger receives recovery diagnostics (e.g. a truncated frame found
	// while scanning segments at Open). Optional; nil disables them.
	Logger *slog.Logger
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
