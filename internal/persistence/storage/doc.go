// Package storage implements the per-partition append-only record log.
//
// Architecture:
//
//   - Each partition is a directory of segment files. One *active*
//     segment receives writes; older segments are sealed (read-only).
//     The active segment is rolled when it crosses Options.SegmentBytes.
//
//   - Append/AppendBatch push records into an in-memory buffer and
//     return an offset immediately. There is no fsync on the produce
//     path.
//
//   - A single per-Log flusher goroutine drains the buffer to the
//     active segment in batches.
//
//   - A separate reaper goroutine deletes sealed segments past the
//     retention bound.
//
// Many goroutines may call Append/AppendBatch/Read concurrently. Only
// the flusher writes to the active segment file. Reads use positioned
// ReadAt and don't contend among themselves.
//
// File map:
//
//	log.go              Log type, NewLog, segment lookup.
//	options.go          Options, DefaultOptions, SyncMode.
//	append.go           Append / AppendBatch — write path into the buffer.
//	read.go             Read / VerifyDurable — buffer → frame cache → segment file.
//	close.go            Close — final flush + segment-file release.
//	offsets.go          Offset and high-watermark accessors.
//	notify.go           NotifyC / Wake long-poll broadcast.
//	recovery.go         Startup scan; corruption skip-and-continue.
//	buffer.go           In-memory write buffer producers push into.
//	flusher.go          Goroutine that drains buffer → active segment.
//	flushing.go         Flushing snapshot (drain-to-disk retry buffer).
//	segment.go          segment type: one file, its bounds, its I/O.
//	frame.go            Frame encode/read/verify (CRC-checked).
//	format.go           On-disk frame layout constants.
//	index.go            Sparse in-memory frame index per segment.
//	nav_cache.go        LRU of recently-resolved frame positions.
//	decode_cache.go     LRU of decoded frames.
//	hwm.go              Durable high-watermark persistence.
//	consumer_offset.go  Per-partition consumer offset file.
//	codec.go            codecForFlag — resolve a frame's codec on read.
//	retention.go        reaper goroutine; age-based segment deletion.
//	metrics.go          MetricsRecorder interface.
//	errors.go           Sentinel errors and corruption classification.
//	path.go             Partition directory layout.
package storage
