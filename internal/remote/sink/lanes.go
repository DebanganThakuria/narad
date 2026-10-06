package sink

import (
	"hash/fnv"
	"math/rand/v2"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// LiveLanesAllowed makes a link's lane count changeable while it runs
// (Q11): each slab splits by the lanes the stub holds when it is read.
// It is safe because a slab completes before the next is read, so a
// key's records never overtake each other across a change.
const LiveLanesAllowed = true

// LaneOf is the lane rec ships on: fnv32a of the key modulo lanes, so a
// key always uses one lane; a keyless record by its parent offset.
func LaneOf(rec topic.KeyedRecord, lanes int) int {
	if lanes <= 1 {
		return 0
	}
	if rec.Key == "" {
		return int(uint64(rec.Offset) % uint64(lanes))
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(rec.Key))
	return int(h.Sum32() % uint32(lanes))
}

// SplitLanes splits recs into lanes, each in slab order. The lanes share
// one backing array.
func SplitLanes(recs []topic.KeyedRecord, lanes int) [][]topic.KeyedRecord {
	lanes = max(lanes, 1)
	if lanes == 1 {
		return [][]topic.KeyedRecord{recs}
	}
	picks := make([]int, len(recs))
	counts := make([]int, lanes)
	for i := range recs {
		picks[i] = LaneOf(recs[i], lanes)
		counts[picks[i]]++
	}
	all := make([]topic.KeyedRecord, len(recs))
	out := make([][]topic.KeyedRecord, lanes)
	off := 0
	for l, n := range counts {
		out[l] = all[off : off : off+n]
		off += n
	}
	for i := range recs {
		out[picks[i]] = append(out[picks[i]], recs[i])
	}
	return out
}

// Backoff is exponential backoff with full jitter: each Next waits a
// uniformly random time up to the current ceiling, which doubles from
// min to max. Not safe for concurrent use.
type Backoff struct {
	Min, Max time.Duration
	ceiling  time.Duration
}

// LaneBackoff is the per-lane backoff of a single unavailable answer
// (ch. 6.6): 250 ms to 2 s.
func LaneBackoff() Backoff { return Backoff{Min: 250 * time.Millisecond, Max: 2 * time.Second} }

// Next returns the next wait and doubles the ceiling.
func (b *Backoff) Next() time.Duration {
	if b.ceiling < b.Min {
		b.ceiling = b.Min
	}
	d := jitter(b.ceiling)
	b.ceiling = min(b.ceiling*2, b.Max)
	return d
}

// Reset starts the next wait from Min again.
func (b *Backoff) Reset() { b.ceiling = 0 }

// jitter is full jitter: uniform in (0, d].
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d))) + 1
}
