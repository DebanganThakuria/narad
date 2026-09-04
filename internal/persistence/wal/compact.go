package wal

import (
	"errors"
	"fmt"
	"math"
	"os"
)

// CompactBefore deletes segment files that contain only records with
// sequence numbers below seq. A segment is deletable when it is not the
// active segment and the next segment's base proves every record in it
// is below seq; the last listed segment is therefore always kept. When
// EVERY record in the log is below seq, the active segment is first
// rolled (past a size floor) so it too becomes deletable — otherwise a
// fully-compacted active segment stays on disk until new appends
// overflow it, which never happens once its writers go quiet.
func (l *Log) CompactBefore(seq uint64) error {
	if err := l.rotateFullyCompacted(seq); err != nil {
		return err
	}
	l.mu.Lock()
	active := l.segmentBase
	floor := l.compactFloor
	l.mu.Unlock()

	// The dispatcher calls this on every progressing pass. Listing the
	// directory each time is wasted work whenever no sealed segment can
	// have become deletable since the last listing: that needs seq to
	// reach the base of the segment after the oldest sealed one, which
	// the previous listing recorded as the floor. A roll resets the floor
	// (rollLocked), so a freshly sealed segment is always re-listed.
	if seq < floor {
		return nil
	}

	segments, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	kept := segments[:0:0]
	for i, segment := range segments {
		if segment.base == active {
			kept = append(kept, segment)
			continue
		}
		if i+1 >= len(segments) || segments[i+1].base > seq {
			kept = append(kept, segment)
			continue
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: compact segment %s: %w", segment.path, err)
		}
	}

	// Next floor: the base of the segment following the oldest remaining
	// sealed one. With only the active segment left nothing is deletable
	// until a roll.
	newFloor := uint64(math.MaxUint64)
	for i, segment := range kept {
		if segment.base == active {
			continue
		}
		if i+1 < len(kept) {
			newFloor = kept[i+1].base
		}
		break
	}
	l.mu.Lock()
	// Only raise the floor if no roll happened meanwhile (a roll resets
	// it to 0 and the next listing must see the new sealed segment).
	if l.compactFloor == floor {
		l.compactFloor = newFloor
	}
	l.mu.Unlock()
	return nil
}

// rotateFullyCompacted rolls the active segment when every record in the
// log — the active segment's included — is below seq and the segment has
// grown past the rotation floor. The fresh (empty) segment's base is the
// next unassigned seq, so recovery's base floor preserves the sequence
// space even after the rolled segment is removed. A non-empty segment
// always has records, so its base is strictly below the new one and the
// O_EXCL create in rollLocked cannot collide.
func (l *Log) rotateFullyCompacted(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.syncErr != nil || l.file == nil {
		return nil // compaction of sealed segments can still proceed
	}
	if l.nextSeq > seq || l.segmentSize < l.opts.CompactRotateBytes {
		return nil
	}
	// Flush anything staged before the file swap, mirroring the roll on
	// the append path.
	batch, err := l.syncLocked()
	completeBatch(batch, err)
	if err != nil {
		return err
	}
	return l.rollLocked()
}
