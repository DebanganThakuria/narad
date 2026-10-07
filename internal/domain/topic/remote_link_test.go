package topic

import (
	"slices"
	"testing"
)

// A partition already holding the maximum number of skips, all above a
// newly blocked record, still keeps the skip of that record: the lowest
// of the others goes instead, so the admin's skip is never a no-op.
func TestWithSkipKeepsTheOffsetJustAdded(t *testing.T) {
	var skip map[int][]int64
	for i := range MaxRemoteSkipsPerPartition {
		skip = WithSkip(skip, 3, int64(1000+i))
	}
	got := WithSkip(skip, 3, 500)
	link := RemoteLink{Skip: got}
	if !link.Skipped(3, 500) {
		t.Fatalf("skip of 500 trimmed at once: %v", got[3])
	}
	if len(got[3]) != MaxRemoteSkipsPerPartition || link.Skipped(3, 1000) || !link.Skipped(3, int64(1000+MaxRemoteSkipsPerPartition-1)) {
		t.Fatalf("skips = %v, want 500 and the %d highest others", got[3], MaxRemoteSkipsPerPartition-1)
	}
	if !slices.IsSorted(got[3]) {
		t.Fatalf("skips not ascending: %v", got[3])
	}
	// The source map is not changed.
	if len(skip[3]) != MaxRemoteSkipsPerPartition || skip[3][0] != 1000 {
		t.Fatalf("WithSkip changed its input: %v", skip[3])
	}
	// A newest offset trims the lowest, as before.
	higher := int64(1000 + 2*MaxRemoteSkipsPerPartition)
	if got := WithSkip(skip, 3, higher); got[3][0] != 1001 || got[3][len(got[3])-1] != higher {
		t.Fatalf("skips after a higher offset = %v", got[3])
	}
}
