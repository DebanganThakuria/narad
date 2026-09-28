package partition

import (
	"hash/fnv"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
)

// defaultCursorLimit is how many topics' round-robin cursors one
// generation holds before it retires (see HashRoundRobin). Two
// generations are live at most, so the table never tracks more than
// twice this many topics, however many are created and deleted.
const defaultCursorLimit = 4096

// HashRoundRobin is the default Manager: FNV-1a hash for keyed
// messages, round-robin per topic for unkeyed ones. The zero value is
// ready for use.
//
// Each topic has its own round-robin cursor, so a producer that writes
// several topics in a fixed rotation still spreads every topic's
// keyless messages over all of its partitions (a cursor shared by all
// topics would give one topic every k-th value and, with k topics of k
// partitions, one partition). A topic's cursor starts at a random
// value, so nodes, and a node after a restart, do not all send a
// topic's first keyless message to partition 0.
//
// The cursors live in two generations. A pick looks in the current
// generation, then in the previous one, moving the topic's cursor
// forward into the current one when it finds it there; a topic in
// neither gets a fresh cursor. When the current generation reaches
// the limit it becomes the previous one and the old previous one is
// dropped. A topic that keeps producing is carried forward and keeps
// its exact round-robin; a deleted or idle topic falls out within two
// generations, so topic churn cannot grow the table without bound.
type HashRoundRobin struct {
	// limit overrides defaultCursorLimit when positive (tests).
	limit int64

	cur    atomic.Pointer[cursorGeneration]
	prev   atomic.Pointer[cursorGeneration]
	retire sync.Mutex // serializes generation turnover
}

// cursorGeneration is one generation of per-topic cursors.
type cursorGeneration struct {
	cursors sync.Map     // topic name -> *atomic.Uint64
	size    atomic.Int64 // topics stored, counted on first store
}

// NewHashRoundRobin returns a Manager ready for use.
func NewHashRoundRobin() *HashRoundRobin {
	return &HashRoundRobin{}
}

// Pick returns the partition index for (topic, key): the key's FNV-1a
// hash modulo partitions for a keyed message, the topic's next
// round-robin position for a keyless one.
func (h *HashRoundRobin) Pick(topic string, key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}
	if key == "" {
		n := h.cursor(topic).Add(1) - 1
		return int(n % uint64(partitions))
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(partitions))
}

// cursor returns topic's round-robin cursor. The common case, a topic
// already in the current generation, is one lock-free map load.
func (h *HashRoundRobin) cursor(topic string) *atomic.Uint64 {
	gen := h.cur.Load()
	if gen == nil {
		h.cur.CompareAndSwap(nil, new(cursorGeneration))
		gen = h.cur.Load()
	}
	if c, ok := gen.cursors.Load(topic); ok {
		return c.(*atomic.Uint64)
	}
	return h.carryOrCreate(gen, topic)
}

// carryOrCreate stores topic's cursor in gen: the previous
// generation's cursor when it has one, so the topic's round-robin
// continues where it was, else a fresh cursor at a random start.
// Retires gen once it holds the limit.
func (h *HashRoundRobin) carryOrCreate(gen *cursorGeneration, topic string) *atomic.Uint64 {
	var c *atomic.Uint64
	if prev := h.prev.Load(); prev != nil {
		if v, ok := prev.cursors.Load(topic); ok {
			c = v.(*atomic.Uint64)
		}
	}
	if c == nil {
		c = new(atomic.Uint64)
		c.Store(rand.Uint64())
	}
	// Clone: the caller's topic may be a slice of a larger string (a
	// request path) that the table would otherwise keep alive.
	v, loaded := gen.cursors.LoadOrStore(strings.Clone(topic), c)
	if !loaded && gen.size.Add(1) >= h.cursorLimit() {
		h.retireGeneration(gen)
	}
	return v.(*atomic.Uint64)
}

// retireGeneration makes full the previous generation and starts an
// empty current one, dropping the old previous generation. A no-op when
// another caller already retired full.
func (h *HashRoundRobin) retireGeneration(full *cursorGeneration) {
	h.retire.Lock()
	defer h.retire.Unlock()
	if h.cur.Load() != full {
		return
	}
	h.prev.Store(full)
	h.cur.Store(new(cursorGeneration))
}

func (h *HashRoundRobin) cursorLimit() int64 {
	if h.limit > 0 {
		return h.limit
	}
	return defaultCursorLimit
}
