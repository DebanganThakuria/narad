// Package storage — see log.go for the architectural overview. This
// file exists to host the file map for quick navigation:
//
//	log.go         *Log type, Options, NewLog, OldestSegmentAt,
//	               SegmentMTimeForOffset, NotifyC, package doc.
//	append.go      Append / AppendBatch — write path into the buffer.
//	read.go        Read — three-tier lookup (buffer → frame cache → segment file).
//	close.go       Close — final flush + segment-file release.
//	offsets.go     OldestOffset, NextOffset, SegmentCount, SizeBytes — public stats.
//	recovery.go    Frame-by-frame scan on startup; corruption skip-and-continue.
//	buffer.go      Per-Log in-memory write buffer (mutex-guarded ring).
//	flusher.go     Background goroutine that drains buffer → active segment.
//	segment.go     *segment value type (file handle + offset bounds).
//	frame.go       Frame layout helpers (encode/decode CRC-checked frames).
//	format.go      On-disk constants (magic, version, frame header layout).
//	codec.go       Codec interface + zstd / no-op implementations.
//	retention.go   *reaper goroutine; age-based segment deletion.
//	metrics.go     Recorder interface; passed in via Options.Metrics.
//	errors.go      Sentinel errors (ErrOffsetNotFound, ErrCorruptRecord, ...).
package storage
