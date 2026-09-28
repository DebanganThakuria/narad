package ingress

import "github.com/debanganthakuria/narad/internal/persistence/wal"

// ReplayProduce streams every produce record with seq >= from to fn,
// in sequence order. Replay stops at the first error from fn or from
// decoding. Each record's Payload aliases the WAL frame replay read it
// from, which replay allocates per record and never reuses, so fn may
// keep it.
func ReplayProduce(dir string, from uint64, fn func(ProduceRecord) error) error {
	if fn == nil {
		return nil
	}
	return ReplayProduceFromCursor(dir, wal.Cursor{Seq: from}, func(record ProduceRecord, _ wal.Cursor) error {
		return fn(record)
	})
}

// ReplayProduceFromCursor streams produce records starting at an exact
// byte cursor, handing fn each record along with the cursor for the
// record after it — persisting that cursor lets a dispatcher resume
// without rescanning the segment. Payloads alias their WAL frames as in
// ReplayProduce, and topic names are shared across the records of one
// call: the dispatcher runs this every few milliseconds, and copying
// every payload and name twice per record was most of its garbage.
func ReplayProduceFromCursor(dir string, cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	return replayProduce(func(cursor wal.Cursor, fn func(wal.Record, wal.Cursor) error) error {
		return wal.ReplayFromCursor(dir, cursor, 0, fn)
	}, cursor, fn)
}

// replayProduce decodes the records a WAL replay (the package-level one
// over a directory, or an open log's) hands out, aliasing payloads and
// interning topic names as ReplayProduceFromCursor describes.
func replayProduce(replay func(wal.Cursor, func(wal.Record, wal.Cursor) error) error, cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	if fn == nil {
		return nil
	}
	var topics topicInterner
	return replay(cursor, func(record wal.Record, next wal.Cursor) error {
		produce, err := decodeProduceRecord(record.Payload, true, &topics)
		if err != nil {
			return err
		}
		produce.WAL = record.ID
		return fn(produce, next)
	})
}
