package cluster

// A moved copy keeps each segment's age. Retention and the cold walk
// judge a segment by its file's modification time, and a copy written
// fresh restarted the retention clock of every moved record: each move
// kept the partition's data a full retention period longer.

import (
	"errors"
	"os"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// maxFutureSegmentModTime bounds how far in the future a carried
// segment modification time may lie before the copy ignores it and
// keeps the time it was written: a clock that far ahead is broken, and
// honouring it would keep the segment past its retention.
const maxFutureSegmentModTime = time.Hour

// localSegmentModTime is the modification time, on this node's clock,
// of a source segment the source last modified at modTime (unix nanos,
// its clock), listed at listedAt (its clock) and received here at
// receivedAt. The segment keeps its age whatever the skew between the
// two clocks; a copy received a round trip after the listing only looks
// that much younger. A source that reports no listing time has its
// time used as is; one that reports no modification time yields 0 (the
// copy keeps the time it was written).
func localSegmentModTime(modTime, listedAt int64, receivedAt time.Time) int64 {
	if modTime <= 0 {
		return 0
	}
	if listedAt <= 0 {
		return modTime
	}
	return receivedAt.UnixNano() - max(listedAt-modTime, 0)
}

// noteSegmentAges records the modification times a listing reports, on
// this node's clock, for stampSegmentAges.
func (s *MoveSession) noteSegmentAges(segs []storage.SegmentInfo, listedAt int64, receivedAt time.Time) {
	for _, seg := range segs {
		if mt := localSegmentModTime(seg.ModTimeUnixNano, listedAt, receivedAt); mt > 0 {
			s.modTimes[seg.BaseOffset] = mt
		}
	}
}

// stampSegmentAges gives each staged segment the modification time its
// source reported, once the staged copy is complete (nothing writes a
// segment after it). A time far in the future is ignored, and a failed
// stamp only leaves the segment with the time it was written.
func (s *MoveSession) stampSegmentAges() {
	limit := time.Now().Add(maxFutureSegmentModTime).UnixNano()
	for base, ns := range s.modTimes {
		if ns > limit {
			continue
		}
		err := storage.SetSegmentModTime(s.stagingDir, base, time.Unix(0, ns))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			s.m.logger.Warn("move: stamp a staged segment with its source's age; it keeps the time it was copied",
				"topic", s.topic, "partition", s.partition, "segment", base, "err", err)
		}
	}
}
