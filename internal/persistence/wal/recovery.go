package wal

import (
	"fmt"
	"io"
	"os"
)

func scanForOpen(segments []segmentInfo, maxRecord int) (uint64, int64, error) {
	var next uint64
	var lastValidEnd int64
	for i, segment := range segments {
		validEnd, maxSeq, sawRecord, err := scanSegment(segment, maxRecord)
		if err != nil {
			return 0, 0, err
		}
		if sawRecord && maxSeq >= next {
			next = maxSeq + 1
		}
		if i == len(segments)-1 {
			lastValidEnd = validEnd
		}
	}
	return next, lastValidEnd, nil
}

func scanSegment(segment segmentInfo, maxRecord int) (int64, uint64, bool, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return 0, 0, false, fmt.Errorf("wal: open segment: %w", err)
	}
	defer file.Close()

	var validEnd int64
	var maxSeq uint64
	var sawRecord bool
	for {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, 0, false, fmt.Errorf("wal: segment offset: %w", err)
		}
		record, ok, err := readFrame(file, segment.base, offset, maxRecord)
		if err != nil {
			return 0, 0, false, err
		}
		if !ok {
			return validEnd, maxSeq, sawRecord, nil
		}
		sawRecord = true
		validEnd = offset + frameHeaderSize + int64(len(record.Payload))
		if record.ID.Seq > maxSeq {
			maxSeq = record.ID.Seq
		}
	}
}
