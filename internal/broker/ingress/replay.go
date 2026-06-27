package ingress

import "github.com/debanganthakuria/narad/internal/persistence/wal"

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
