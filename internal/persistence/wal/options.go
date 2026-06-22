package wal

import "time"

const (
	defaultSegmentBytes = 64 << 20
	defaultSyncInterval = 10 * time.Millisecond
	defaultSyncBytes    = 1 << 20
	defaultMaxRecord    = 16 << 20
)

type Options struct {
	SegmentBytes      int64
	SyncInterval      time.Duration
	SyncBytes         int64
	MaxRecord         int
	Observer          StageObserver
	ObserverComponent string
	ObserverOperation string
}

type StageObserver interface {
	ObserveHotPathStage(component, operation, stage, outcome string, duration time.Duration)
}

func normalizeOptions(opts Options) Options {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaultSyncInterval
	}
	if opts.SyncBytes < 0 {
		opts.SyncBytes = 0
	}
	if opts.SyncBytes == 0 {
		opts.SyncBytes = defaultSyncBytes
	}
	if opts.MaxRecord <= 0 {
		opts.MaxRecord = defaultMaxRecord
	}
	return opts
}
