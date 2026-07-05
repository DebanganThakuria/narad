package ingress

import "github.com/debanganthakuria/narad/internal/persistence/wal"

// ReplayProduce streams every produce record with seq >= from to fn,
// in sequence order. Replay stops at the first error from fn or from
// decoding.
func ReplayProduce(dir string, from uint64, fn func(ProduceRecord) error) error {
	if fn == nil {
		return nil
	}
	return wal.Replay(dir, from, 0, func(record wal.Record) error {
		produce, err := DecodeProduceRecord(record.Payload)
		if err != nil {
			return err
		}
		produce.WAL = record.ID
		return fn(produce)
	})
}

// ReplayProduceFromCursor streams produce records starting at an exact
// byte cursor, handing fn each record along with the cursor for the
// record after it — persisting that cursor lets a dispatcher resume
// without rescanning the segment.
func ReplayProduceFromCursor(dir string, cursor wal.Cursor, fn func(ProduceRecord, wal.Cursor) error) error {
	if fn == nil {
		return nil
	}
	return wal.ReplayFromCursor(dir, cursor, 0, func(record wal.Record, next wal.Cursor) error {
		produce, err := DecodeProduceRecord(record.Payload)
		if err != nil {
			return err
		}
		produce.WAL = record.ID
		return fn(produce, next)
	})
}

// nextProduceSeq scans the WAL and returns one past the highest
// recovered sequence (0 for an empty log).
func nextProduceSeq(dir string) (uint64, error) {
	var next uint64
	err := wal.Replay(dir, 0, 0, func(record wal.Record) error {
		if record.ID.Seq >= next {
			next = record.ID.Seq + 1
		}
		return nil
	})
	return next, err
}
