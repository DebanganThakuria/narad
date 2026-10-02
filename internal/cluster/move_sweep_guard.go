package cluster

// What the new owner has to vouch for before the old owner deletes its
// stale copy of a moved partition.
//
// The sweep's leader confirmation proves the partition LIVES elsewhere.
// It does not prove the records do: the new owner can hold less than the
// copy the move gave it. A destination that rolled its install back after
// a flip that committed anyway serves an empty log. A new owner that came
// back on an empty volume under the same ID answers with no marker, no
// segments and hwm 0. A new owner that lost power before the kernel wrote
// back the copied segments can be missing sealed segments or hold them
// short. In each case the old owner's copy is the only one left, and
// deleting it loses every record written before the move.
//
// So the owner's listing is compared with the local copy before any
// reclaim, and anything the owner cannot vouch for is set aside
// (quarantined), never deleted:
//
//   - the local copy holds no record retention has not already expired:
//     reclaim with a guard at the position the owner vouches for (the
//     reclaim still quarantines a copy that recovers past it);
//   - the owner lists no records while the local copy holds unexpired
//     ones: set aside;
//   - the owner holds records but no move marker, so they did not come
//     from this copy: set aside;
//   - a local segment below the vouched position is missing from the
//     owner's listing (and not expired), or a sealed one wholly below it
//     is longer than the owner's: set aside;
//   - otherwise reclaim with a guard at the vouched position: the owner's
//     high-watermark, and never more than the move marker's promoted one.
//
// One local copy came from the owner rather than going to it: an install
// whose flip never committed (the destination died with its flip
// pending and the controller cleared the target, or its worker was
// cancelled and the move re-planned elsewhere), left at the partition's
// path with a move marker that names the current owner as its source.
// The owner held every record in it when it was copied, and needs no
// marker of its own to vouch for them: the owner's listing and its live
// high-watermark judge it, and the owner's own marker (from an earlier
// move onto it) does not cap the position. Its first bytes are compared
// with the owner's too, so an owner that came back empty under the same
// ID and took new records at the same offsets cannot pass for the one
// the copy was taken from.
//
// A copy the owner cannot vouch for is not kept at the partition's path,
// where a later move of the partition back onto this node meets it; it
// is set aside, and every set aside is logged at error level. A move back
// that comes before the sweep runs meets the copy in the install, which
// sets it aside whenever it holds unexpired records (setAsideLiveCopy).

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// localSegment is one segment file of the local stale copy.
type localSegment struct {
	base    int64
	size    int64
	modTime time.Time
}

// listLocalSegments lists a partition directory's segment files in
// base-offset order with their sizes and modification times. A missing
// directory has none.
func listLocalSegments(dir string) ([]localSegment, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segs []localSegment
	for _, e := range entries {
		base, ok := storage.ParseSegmentFileName(e.Name())
		if !ok || e.IsDir() {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat segment %s: %w", filepath.Join(dir, e.Name()), err)
		}
		segs = append(segs, localSegment{base: base, size: info.Size(), modTime: info.ModTime()})
	}
	slices.SortFunc(segs, func(a, b localSegment) int { return cmp.Compare(a.base, b.base) })
	return segs, nil
}

// ownerReclaimGuard decides from the new owner's listing (info) and the
// local copy's segments how the local copy may be reclaimed. The guard is
// always KNOWN; SetAside is set (with the reason) when the owner cannot
// vouch for the copy. retention is the topic's age bound (zero keeps
// forever): a local segment older than it holds nothing retention would
// not have removed anyway, so the owner no longer listing it is not a
// gap.
func ownerReclaimGuard(info messaging.PartitionTransferInfo, local []localSegment, retention time.Duration, now time.Time) messaging.ReclaimGuard {
	// The position the owner vouches for: its live high-watermark, capped
	// at the promoted one when the move left a marker. An owner whose hwm
	// is below the promoted position lost records it was given; the local
	// records past it exist only here.
	vouched := info.HighWatermark
	if m := info.MoveMarker; m != nil {
		vouched = min(vouched, m.HighWatermark)
	}
	guard := messaging.ReclaimGuard{PromotedHWM: vouched, Known: true}
	setAside := func(format string, args ...any) messaging.ReclaimGuard {
		guard.SetAside = fmt.Sprintf(format, args...)
		return guard
	}

	expired := func(s localSegment) bool { return segmentExpired(s, retention, now) }
	if !liveRecords(local, retention, now) {
		return guard
	}
	ownerSize := make(map[int64]int64, len(info.Segments))
	ownerRecords := false
	for _, s := range info.Segments {
		ownerSize[s.BaseOffset] = s.SizeBytes
		if s.SizeBytes > 0 {
			ownerRecords = true
		}
	}
	if !ownerRecords {
		return setAside("the owner lists no records (hwm %d, %d segments, move marker %v) while the local copy holds unexpired records",
			info.HighWatermark, len(info.Segments), info.MoveMarker != nil)
	}
	if info.MoveMarker == nil {
		// Every release since v2.2.0 leaves a marker in the copy a move
		// installs, so an owner without one holds records that did not
		// come from this copy: it rolled its install back and served new
		// produce from offset 0, or it was placed there some other way.
		// Its records are not these even at the same offsets, and its
		// high-watermark vouches for none of them.
		return setAside("the owner holds records (hwm %d) but no move marker, so they did not come from this copy", info.HighWatermark)
	}
	for i, s := range local {
		if s.size == 0 || s.base >= vouched {
			// Records at or past the vouched position are the guard's to
			// judge: it quarantines a copy that recovers past it.
			continue
		}
		size, listed := ownerSize[s.base]
		if !listed {
			if expired(s) {
				continue
			}
			return setAside("the owner lists no segment at base offset %d (local %d bytes, below the vouched position %d)", s.base, s.size, vouched)
		}
		// A sealed local segment wholly below the vouched position was
		// copied byte for byte, so the owner's must be at least as long.
		// The local tail segment may hold bytes past its committed
		// records, and a segment that straddles the vouched position
		// diverges after it; the guard covers both.
		wholeBelow := i+1 < len(local) && local[i+1].base <= vouched
		if wholeBelow && size < s.size {
			return setAside("the owner's segment at base offset %d is shorter than the local one (%d < %d bytes)", s.base, size, s.size)
		}
	}
	return guard
}

// segmentExpired reports whether retention (zero keeps forever) has
// already passed over a local segment.
func segmentExpired(s localSegment, retention time.Duration, now time.Time) bool {
	return retention > 0 && s.modTime.Before(now.Add(-retention))
}

// liveRecords reports whether any local segment holds bytes retention
// has not expired.
func liveRecords(local []localSegment, retention time.Duration, now time.Time) bool {
	for _, s := range local {
		if s.size > 0 && !segmentExpired(s, retention, now) {
			return true
		}
	}
	return false
}

// moveUnsyncedCopyWriteback is how long after an install the old owner's
// sweep waits before it trusts a new owner whose move marker carries no
// durable stamp: a release that did not sync the copy before the flip,
// whose listing can report segments that are still only in its page
// cache. Linux writes dirty pages back within about 35 s by default
// (vm.dirty_expire_centisecs plus the flusher interval); the rest is
// margin for a loaded disk and for clock skew between the two nodes.
const moveUnsyncedCopyWriteback = 5 * time.Minute

// unsyncedInstallTooRecent reports whether the owner's move marker m
// says its copy was installed without being synced, too recently for the
// kernel to have written it back.
func unsyncedInstallTooRecent(m *messaging.MoveMarker, now time.Time) bool {
	return m != nil && m.DurableAtUnixMs == 0 && m.InstalledAtUnixMs > 0 &&
		now.Sub(time.UnixMilli(m.InstalledAtUnixMs)) < moveUnsyncedCopyWriteback
}
