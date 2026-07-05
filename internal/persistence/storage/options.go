package storage

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// Options configures a Log. The zero value is usable: NewLog fills in
// sensible defaults for anything left unset.
type Options struct {
	// Codec encodes frame payloads written by this log. Nil means no
	// compression. Reads always resolve the codec from the frame header,
	// so a log can be reopened with a different Codec at any time.
	Codec codec.Codec

	// FlushBytes and FlushRecords are the buffer thresholds that make
	// the flusher drain ASAP; FlushInterval bounds how long a record can
	// sit in the buffer regardless.
	FlushBytes    int
	FlushRecords  int
	FlushInterval time.Duration

	// SyncMode selects the fsync policy; under SyncBatched, SyncInterval
	// and SyncBytes bound how much flushed data may remain unsynced.
	SyncMode     SyncMode
	SyncInterval time.Duration
	SyncBytes    int64

	// HWMSyncInterval bounds how long an advanced high-watermark may
	// stay unpersisted when nothing forces a sync.
	HWMSyncInterval time.Duration

	// SegmentBytes is the size at which the active segment is rolled.
	SegmentBytes int64

	// Retention governs the reaper that deletes old sealed segments.
	Retention RetentionConfig

	// Metrics is an optional observability plug. When nil, every
	// instrumented call site short-circuits to a noop.
	Metrics MetricsRecorder
}

// DefaultOptions returns the production defaults: fast zstd compression,
// batched fsync, 64 MiB segments.
func DefaultOptions() Options {
	c, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		panic(fmt.Sprintf("storage: default zstd codec: %v", err))
	}
	return Options{
		Codec:           c,
		FlushBytes:      1 << 20,
		FlushRecords:    1000,
		FlushInterval:   100 * time.Millisecond,
		SyncMode:        SyncBatched,
		SyncInterval:    time.Second,
		SyncBytes:       8 << 20,
		HWMSyncInterval: 5 * time.Second,
		SegmentBytes:    64 << 20,
	}
}

// withDefaults returns a copy with every unset field replaced by its
// default, so the rest of the package never re-checks for zero values.
func (o Options) withDefaults() Options {
	if o.Codec == nil {
		o.Codec = codec.NewNoopCodec()
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 100 * time.Millisecond
	}
	if o.SyncMode == "" {
		o.SyncMode = SyncBatched
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = time.Second
	}
	if o.SyncBytes < 0 {
		o.SyncBytes = 0
	}
	if o.HWMSyncInterval <= 0 {
		o.HWMSyncInterval = 5 * time.Second
	}
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	return o
}

// SyncMode controls when the background flusher calls file.Sync().
type SyncMode string

const (
	// SyncPerWrite syncs every flushed batch. Appends are still buffered, so
	// this means "per storage batch", not "inside Produce".
	SyncPerWrite SyncMode = "per_write"

	// SyncBatched lets the flusher write many batches before syncing, bounded
	// by Options.SyncInterval, Options.SyncBytes, segment roll, and Close.
	SyncBatched SyncMode = "batched"
)
