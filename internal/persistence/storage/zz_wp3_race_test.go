//go:build race

package storage

// wp3RaceEnabled lets the allocation-count tests stand down under the
// race detector, which changes what escapes to the heap.
const wp3RaceEnabled = true
