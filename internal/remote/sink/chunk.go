package sink

import "time"

// Chunk limits (ch. 6.4, 10.1).
const (
	// DefaultMaxChunkMessages is the target's batch-produce message cap
	// before Q13: 100.
	DefaultMaxChunkMessages = 100
	// ProbedMaxChunkMessages is the cap once the target showed it takes
	// 1,000 messages a batch (Q13, see ProbeCapabilities).
	ProbedMaxChunkMessages = 1000
	// MaxChunkBytes is the most body one chunk carries: under the 1 MiB
	// body cap of a target without Q4, and a size a degraded WAN path
	// still uploads inside the request timeout.
	MaxChunkBytes = 960 << 10
	// MinChunkBytes is the floor the adaptive cap halves down to.
	MinChunkBytes = 64 << 10
	// growAfterSuccesses is how many consecutive accepted chunks double
	// a lane's shrunken cap back.
	growAfterSuccesses = 20

	// RetentionWarnMs is the parent retention below which an attach and
	// its dry run warn (Q15): a regional outage plus the catch-up.
	RetentionWarnMs int64 = 72 * 60 * 60 * 1000

	// SlabRecordsPerLane and SlabBytesPerLane size a remote cursor's slab
	// (lanes x 500 records, lanes x 1 MiB): about five chunks per lane.
	SlabRecordsPerLane = 500
	SlabBytesPerLane   = 1 << 20

	// StallRetry is how often a link stalled on something only a change
	// can fix (a missing remote, a refused grant, a rejected record, ...)
	// tries again when nothing changed.
	StallRetry = 30 * time.Second
)

// ChunkCap is one lane's adaptive chunk byte cap: a timeout or a
// "read body" 400 halves it, down to MinChunkBytes, and
// growAfterSuccesses consecutive accepted chunks double it back up to
// MaxChunkBytes. Not safe for concurrent use: one lane owns it.
type ChunkCap struct {
	bytes  int
	streak int
}

// NewChunkCap starts at MaxChunkBytes.
func NewChunkCap() ChunkCap { return ChunkCap{bytes: MaxChunkBytes} }

// Bytes is the current cap.
func (c *ChunkCap) Bytes() int {
	if c.bytes == 0 {
		c.bytes = MaxChunkBytes
	}
	return c.bytes
}

// Shrink halves the cap (not below MinChunkBytes) and resets the streak.
func (c *ChunkCap) Shrink() {
	c.bytes = max(c.Bytes()/2, MinChunkBytes)
	c.streak = 0
}

// Accepted counts one accepted chunk, doubling a shrunken cap after
// growAfterSuccesses in a row.
func (c *ChunkCap) Accepted() {
	if c.Bytes() >= MaxChunkBytes {
		c.streak = 0
		return
	}
	c.streak++
	if c.streak >= growAfterSuccesses {
		c.bytes = min(c.bytes*2, MaxChunkBytes)
		c.streak = 0
	}
}
