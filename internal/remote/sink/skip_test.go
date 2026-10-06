package sink

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// An admin can skip every record of one slab, one by one, before the
// cursor advances past it. A restart or a partition move reads that
// slab again from the persisted offset, so the link must still hold
// every one of those skips, or the cursor blocks again on a record an
// admin already skipped.
func TestSkipsKeptCoverEveryRecordOfAFullSlab(t *testing.T) {
	slab := int64(topic.MaxRemoteLanes * SlabRecordsPerLane)
	const start = 1000
	var skip map[int][]int64
	for off := int64(start); off < start+slab; off++ {
		skip = topic.WithSkip(skip, 0, off)
	}
	link := topic.RemoteLink{Skip: skip}
	for off := int64(start); off < start+slab; off++ {
		if !link.Skipped(0, off) {
			t.Fatalf("the skip of offset %d (%d of a %d-record slab) was trimmed", off, off-start+1, slab)
		}
	}
}
