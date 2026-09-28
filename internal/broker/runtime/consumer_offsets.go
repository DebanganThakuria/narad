package runtime

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

const (
	// defaultConsumerOffsetCommitInterval is the durability interval D
	// when none is configured: how long an acked frontier can wait for a
	// device flush.
	defaultConsumerOffsetCommitInterval = time.Second
	// consumerOffsetMaxTick caps the write cadence T: every tick hands
	// the changed records to the page cache, so a process crash loses
	// at most this much of acks whatever D is.
	consumerOffsetMaxTick = 100 * time.Millisecond
	// consumerOffsetLevelEvery is how often, at most, a partition's
	// consumer.offset is brought up to the frontier consumer.ahead
	// carries while the broker runs.
	consumerOffsetLevelEvery = 30 * time.Second
	// consumerOffsetMaxFDs caps the consumer.ahead descriptors held open
	// (see offsetFDCap).
	consumerOffsetMaxFDs = 4096
	// consumerAheadFileSize is both slots.
	consumerAheadFileSize = 2 * storage.ConsumerAheadSlotSize
)

type offsetCommitKey struct {
	topic     string
	partition int
}

type offsetCommit struct {
	key    offsetCommitKey
	offset int64
}

// AheadSource returns, for one partition, the committed frontier, the
// offsets acked out of order above it, and a version that changes
// whenever that set changes; ok=false when the node holds no state for
// the partition. consumer.InFlight.AheadSnapshot is the production
// source.
type AheadSource func(topic string, partition int) (committed int64, offsets []int64, version uint64, ok bool)

// ConsumerOffsetCommitter persists acked consumer state, best effort:
// ack commits are authoritative in memory and this writer only seeds
// recovery. It runs at two cadences.
//
// Every tick (T, the durability interval capped at 100ms) each
// partition whose acked state changed gets its consumer.ahead record,
// the frontier and the offsets acked out of order above it, written
// into the page cache through a held descriptor, with no sync. A
// process crash loses no page-cache write, so it redelivers about a
// tick of acks.
//
// Every durability interval (D, storage.consumer_offset_commit_interval_ms)
// each partition written since is written out (fdatasync on Linux,
// fsync(2) on macOS) and, on macOS, the tick then flushes the drive
// cache once per device for all of them (F_FULLFSYNC) instead of once
// per partition. A power loss or kernel crash redelivers at most about
// D plus a tick plus the writeout time of acks. The writeouts of an
// interval are spread over its ticks by a hash of the partition, so
// they do not arrive at the disk in one burst.
//
// consumer.ahead has two slots. The anchor holds the newest record
// known durable and is never written while it is the anchor; ticks
// write the other slot, the window, and a successful writeout flips
// the two. A torn window fails its checksum and recovery falls back to
// the anchor, so no crash of the process or of the machine leaves a
// partition with neither, and every record holds only acked values, so
// a recovered frontier never passes an acked one.
//
// consumer.offset, the 8-byte frontier file that older binaries and
// operators read, is levelled with consumer.ahead's frontier at a
// partition's writeout when it was last levelled more than 30s ago, and
// at Close, which leaves both files exact. Recovery takes the larger
// frontier of the two, and so must every reader of a persisted
// frontier. The file formats are v3.0.1's, so a rollback reads them.
type ConsumerOffsetCommitter struct {
	dataDir string
	// durable is D, tick is T, and every is D/T: a dirty partition is
	// written out on one tick of every such run of ticks.
	durable time.Duration
	tick    time.Duration
	every   uint64
	log     *slog.Logger
	io      offsetIO
	burst   bool
	crash   func(point offsetPoint, key offsetCommitKey) bool

	mu      sync.Mutex
	pending map[offsetCommitKey]int64
	ahead   AheadSource

	// tickMu serializes ticks: the loop's, flush's and Close's.
	tickMu    sync.Mutex
	tickNo    uint64
	lastAge   time.Time
	lastSlow  time.Time
	lastStats offsetTickStats

	// ioMu guards the per-partition state and every open of a consumer
	// state file by path. Forget takes it, so a by-path open either
	// finishes before a Forget returns or sees it; the ack path never
	// takes it (Commit uses mu).
	ioMu sync.Mutex
	// parts is what the committer knows about each partition it wrote.
	parts map[offsetCommitKey]*offsetPart
	// forgotten holds the partitions forgotten since the running tick
	// began: a snapshot taken before a Forget must not be written
	// through a descriptor opened after it.
	forgotten map[offsetCommitKey]struct{}
	// clean lists the durable partitions holding a descriptor, least
	// recently written first: the only ones the descriptor cap evicts.
	clean  offsetLRU
	held   int
	maxFDs int

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// offsetIO is the committer's durability seam: the platform's
// primitives in production, a model of the disk in crash tests.
type offsetIO struct {
	writeOut    func(*os.File) error
	flushDevice func(*os.File) error
	syncDir     func(*os.File) error
}

// offsetPoint names a step of a tick, for the crash tests' hook.
type offsetPoint uint8

const (
	offsetPointPrimed     offsetPoint = iota + 1 // a prime read and extended its file, before its writeout
	offsetPointPrimeOut                          // the primes were written out, before their device flush
	offsetPointWrite                             // one partition's window was written
	offsetPointWritten                           // every window of the tick was written, before any writeout
	offsetPointLevel                             // one consumer.offset was levelled, before its writeout
	offsetPointWrittenOut                        // the writeouts ran, before the device flush
	offsetPointFlushed                           // the device flush ran, before the flips
)

// errOffsetCrash ends a tick at a crash point a test chose; nothing
// after the point is written, as if the process died there.
var errOffsetCrash = errors.New("consumer offsets: test crash point")

// errOffsetDirGone reports a partition directory that no longer holds
// the files the committer primed: removed, or replaced by another
// directory. The committer drops its state and never recreates it.
var errOffsetDirGone = errors.New("consumer offsets: partition directory removed or replaced")

// offsetPart is the committer's state for one partition. The
// descriptor is a cache entry: the state survives its eviction.
type offsetPart struct {
	key   offsetCommitKey
	dir   string
	phase uint64

	// f is the held consumer.ahead descriptor, nil when evicted.
	f *os.File
	// file and dirInfo identify the consumer.ahead and the partition
	// directory the prime opened; dev is their device.
	file    os.FileInfo
	dirInfo os.FileInfo
	dev     uint64

	// primed: the slots were read and anchor is known. syncing: the
	// prime's writeout has not completed, so no window may be written.
	primed  bool
	syncing bool
	// anchor is the slot of the newest durable record (-1: none valid).
	anchor int
	// seq is the highest record sequence written or read.
	seq uint64
	// durable is the anchor record's frontier (-1: none).
	durable int64

	// written is what the last window write carried, which after a flip
	// is also the anchor's content.
	written offsetWritten
	// dirtySince is when the window first held a record that is not
	// yet durable; zero when the window matches the anchor.
	dirtySince time.Time
	// needsRewrite: a write or writeout failed, so the next tick must
	// write the window in full even when the snapshot did not change
	// (after a failed sync the page cache is not to be trusted).
	needsRewrite bool
	// reprime: the last prime's writeout failed, so the next prime
	// writes the anchor's bytes back before its writeout (a kernel may
	// mark pages clean after failing to write them back).
	reprime bool

	// level is what this process knows consumer.offset holds (-1: none
	// or unknown); levelAt is when this process last wrote it.
	level   int64
	levelAt time.Time

	dead             bool
	lruPrev, lruNext *offsetPart
	inLRU            bool
}

type offsetWritten struct {
	version   uint64
	committed int64
	ok        bool
}

// window is the slot ticks write: the one that is not the anchor.
func (st *offsetPart) window() int {
	if st.anchor == 0 {
		return 1
	}
	return 0
}

// frontier is the newest frontier this process wrote or read for the
// partition, -1 when none.
func (st *offsetPart) frontier() int64 {
	if st.written.ok {
		return st.written.committed
	}
	return st.durable
}

// offsetTickStats is what one tick did, for the warnings and tests.
type offsetTickStats struct {
	written, wroteOut, levelled, primed int
	flushes                             int
	// late counts partitions whose writes waited, or have waited so
	// far, more than twice the durability interval for their writeout;
	// oldest is the longest such wait.
	late   int
	oldest time.Duration
}

// committerOptions are the test hooks of newConsumerOffsetCommitter.
type committerOptions struct {
	// io replaces the durability primitives; a nil field keeps the
	// platform's.
	io offsetIO
	// manual runs no loop: ticks happen only through flush, tickAt and
	// Close.
	manual bool
	// maxFDs overrides the descriptor cap when positive.
	maxFDs int
	// burst puts every partition in phase 0, so every dirty partition is
	// written out on the same tick of each interval: the shape the hash
	// spread replaced, kept for A/B runs.
	burst bool
	// crash, when it returns true, ends the tick at that point.
	crash func(point offsetPoint, key offsetCommitKey) bool
}

// NewConsumerOffsetCommitter starts the background loop. interval is
// the durability interval D; a non-positive one falls back to the
// default. Callers must Close to stop the loop and persist what is
// pending.
func NewConsumerOffsetCommitter(dataDir string, interval time.Duration, log *slog.Logger) *ConsumerOffsetCommitter {
	return newConsumerOffsetCommitter(dataDir, interval, log, committerOptions{})
}

func newConsumerOffsetCommitter(dataDir string, interval time.Duration, log *slog.Logger, opts committerOptions) *ConsumerOffsetCommitter {
	if interval <= 0 {
		interval = defaultConsumerOffsetCommitInterval
	}
	tick := min(interval, consumerOffsetMaxTick)
	c := &ConsumerOffsetCommitter{
		dataDir:   dataDir,
		durable:   interval,
		tick:      tick,
		every:     uint64(max(1, interval/tick)),
		log:       log,
		io:        opts.io,
		burst:     opts.burst,
		crash:     opts.crash,
		pending:   make(map[offsetCommitKey]int64),
		parts:     make(map[offsetCommitKey]*offsetPart),
		forgotten: make(map[offsetCommitKey]struct{}),
		maxFDs:    offsetFDCap(),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	if c.io.writeOut == nil {
		c.io.writeOut = offsetWriteOut
	}
	if c.io.flushDevice == nil {
		c.io.flushDevice = offsetFlushDevice
	}
	if c.io.syncDir == nil {
		c.io.syncDir = syncfile.Sync
	}
	if opts.maxFDs > 0 {
		c.maxFDs = opts.maxFDs
	}
	if opts.manual {
		close(c.done)
	} else {
		go c.run()
	}
	return c
}

// SetAheadSource registers the acked-ahead snapshot source. Without one
// only the frontier Commit reports is persisted. Call before serving.
func (c *ConsumerOffsetCommitter) SetAheadSource(fn AheadSource) {
	c.mu.Lock()
	c.ahead = fn
	c.mu.Unlock()
}

// Commit marks a partition dirty for the next tick, which persists the
// source's snapshot of it. Without a source the offset itself is the
// frontier, and offsets only move forward: a smaller one never
// overwrites a pending larger one. A call with the current frontier (no
// advance) still marks the partition dirty, which is how an
// out-of-order ack reaches the next tick.
func (c *ConsumerOffsetCommitter) Commit(topic string, partition int, offset int64) {
	key := offsetCommitKey{topic: topic, partition: partition}
	c.mu.Lock()
	if current, ok := c.pending[key]; !ok || offset > current {
		c.pending[key] = offset
	}
	c.mu.Unlock()
}

// Forget drops everything the committer holds for a partition: its
// descriptor, its state and what is pending. Call before a partition's
// directory is replaced or removed under the committer (a move
// installs a copy, a reclaim quarantines it, a purge removes it) and
// again after; once Forget returns, no snapshot taken before it is
// written, and the next tick for the partition starts from the files
// then on disk.
func (c *ConsumerOffsetCommitter) Forget(topic string, partition int) {
	key := offsetCommitKey{topic: topic, partition: partition}
	c.ioMu.Lock()
	c.forgotten[key] = struct{}{}
	if st := c.parts[key]; st != nil {
		c.dropLocked(st)
	}
	c.ioMu.Unlock()
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()
}

// Close stops the loop and runs a final tick that writes out every
// partition written since its last writeout and levels every
// consumer.offset behind its consumer.ahead, all in one batch with one
// device flush, so after a graceful stop both files are exact and
// durable. It then closes the held descriptors. Idempotent.
func (c *ConsumerOffsetCommitter) Close() error {
	c.once.Do(func() { close(c.stop) })
	<-c.done
	err := c.tickAt(time.Now(), offsetTickClose)
	c.ioMu.Lock()
	for _, st := range c.parts {
		c.releaseLocked(st)
	}
	c.ioMu.Unlock()
	return err
}

// run ticks every T. A tick that takes longer than T is followed at
// once by the next; that is logged, at most once a minute (a tick that
// primed partitions excepted), as is a partition whose writes have
// waited more than twice D for their writeout: persisted offsets then
// lag acks by more than the setting says.
//
// A tick writes out its partitions one at a time on purpose.
// Overlapping their data syncs, 8 at once, made a flush 2.4 to 3.7
// times shorter, but on macOS the overlapped bursts held back the
// produce syncs sharing the disk: beside 12 dirty partitions an owner's
// 24-record commit went from 10ms to 22ms at p99 (see
// BenchmarkZZWP16OwnerCommitUnderOffsetFlush and
// BenchmarkZZWP16SpreadCommitUnderOffsetFlush in the messaging package).
func (c *ConsumerOffsetCommitter) run() {
	defer close(c.done)

	ticker := time.NewTicker(c.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			start := time.Now()
			if err := c.tickAt(start, offsetTickNormal); err != nil && c.log != nil {
				c.log.Error("consumer offset batch write failed", "err", err)
			}
			c.warn(start, time.Since(start))
		case <-c.stop:
			return
		}
	}
}

// warn logs, at most once a minute each, a tick that overran T and
// partitions whose writes waited more than 2D for their writeout.
func (c *ConsumerOffsetCommitter) warn(now time.Time, took time.Duration) {
	if c.log == nil {
		return
	}
	c.tickMu.Lock()
	stats := c.lastStats
	// A tick that primed partitions paid for their first touch (after a
	// start, most of them at once); only the steady cost counts.
	slow := took > c.tick && stats.primed == 0 && now.Sub(c.lastSlow) >= time.Minute
	if slow {
		c.lastSlow = now
	}
	late := stats.late > 0 && now.Sub(c.lastAge) >= time.Minute
	if late {
		c.lastAge = now
	}
	c.tickMu.Unlock()
	if slow {
		c.log.Warn("consumer offset commits cannot keep to their interval: persisted offsets lag acks by about the flush time",
			"partitions", max(stats.written, stats.wroteOut), "flush_took", took, "interval", c.tick)
	}
	if late {
		c.log.Warn("consumer offsets wait longer than their durability interval for a device flush: a power loss would redeliver more acks than it promises",
			"partitions", stats.late, "oldest", stats.oldest, "durability_interval", c.durable)
	}
}

// flush runs one tick that writes out every dirty partition, whatever
// its phase. Tests use it to drive the committer by hand.
func (c *ConsumerOffsetCommitter) flush() error {
	return c.tickAt(time.Now(), offsetTickFlush)
}

// offsetTickMode is what a tick writes out.
type offsetTickMode uint8

const (
	// offsetTickNormal writes out the dirty partitions whose phase this
	// tick is or that have waited D.
	offsetTickNormal offsetTickMode = iota
	// offsetTickFlush writes out every dirty partition.
	offsetTickFlush
	// offsetTickClose also levels every consumer.offset behind its
	// frontier.
	offsetTickClose
)

// offsetWant is one partition's snapshot for a tick.
type offsetWant struct {
	key       offsetCommitKey
	committed int64
	offsets   []int64
	version   uint64
	st        *offsetPart
}

// offsetSync is one file a tick writes out: a primed consumer.ahead, a
// window, or a levelled consumer.offset.
type offsetSync struct {
	st        *offsetPart
	f         *os.File
	dev       uint64
	transient bool
	// dir, when set, is synced after f: f was created.
	dir *os.File
	// window: f is st's consumer.ahead and its window flips on success.
	window bool
	// level is the frontier written when f is consumer.offset (neither
	// a prime nor a window).
	level int64
	// requeue is the commit a failed prime queues again.
	requeue int64
	err     error
	gone    bool
}

// tickAt runs one tick at now: snapshot the partitions committed since
// the last tick, prime the ones seen for the first time, write their
// windows, then write out the partitions due (see offsetTickMode) and
// flip them.
func (c *ConsumerOffsetCommitter) tickAt(now time.Time, mode offsetTickMode) error {
	c.tickMu.Lock()
	defer c.tickMu.Unlock()
	start := time.Now()
	c.tickNo++
	c.lastStats = offsetTickStats{}

	c.ioMu.Lock()
	clear(c.forgotten)
	c.ioMu.Unlock()

	commits, source := c.drain()
	wants := c.snapshots(commits, source)

	var errs offsetErrs
	primes, err := c.prime(wants, source != nil, &errs)
	if err != nil {
		return err
	}
	if err := c.syncPrimes(primes, &errs); err != nil {
		return err
	}
	syncs, err := c.writeWindows(now, wants, source != nil, mode, &errs)
	if err != nil {
		return err
	}
	if err := c.writeOut(syncs); err != nil {
		return err
	}
	// now may be a test's clock: the flips happen the tick's own
	// duration after it.
	c.flip(now, now.Add(time.Since(start)), syncs, &errs)
	return errs.err
}

// snapshots takes each committed partition's acked state from source,
// outside every lock of the committer. A partition the source holds no
// shard for is skipped: its commit came from a shard that was dropped
// (acks call Commit after releasing the shard lock), and the directory
// under its name may already be another lineage's.
func (c *ConsumerOffsetCommitter) snapshots(commits []offsetCommit, source AheadSource) []offsetWant {
	wants := make([]offsetWant, 0, len(commits))
	for _, commit := range commits {
		if source == nil {
			wants = append(wants, offsetWant{key: commit.key, committed: commit.offset})
			continue
		}
		committed, offsets, version, ok := source(commit.key.topic, commit.key.partition)
		if !ok {
			continue
		}
		wants = append(wants, offsetWant{key: commit.key, committed: committed, offsets: offsets, version: version})
	}
	return wants
}

// prime resolves each snapshot's state, creating it on first touch,
// and primes the ones not primed yet. Under ioMu, so no Forget lands
// between the forgotten check and a by-path open.
func (c *ConsumerOffsetCommitter) prime(wants []offsetWant, hasSource bool, errs *offsetErrs) ([]offsetSync, error) {
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	var primes []offsetSync
	for i := range wants {
		w := &wants[i]
		if _, gone := c.forgotten[w.key]; gone {
			if hasSource {
				// The snapshot may be of the dropped shard: take it again.
				c.Commit(w.key.topic, w.key.partition, w.committed)
			}
			continue
		}
		st := c.parts[w.key]
		if st == nil {
			st = c.newPart(w.key)
			c.parts[w.key] = st
		}
		w.st = st
		if st.primed {
			continue
		}
		p, err := c.primeLocked(st)
		if errors.Is(err, errOffsetDirGone) {
			c.dropLocked(st)
			continue
		}
		if err != nil {
			errs.add(w.key, "prime", err)
			c.Commit(w.key.topic, w.key.partition, w.committed)
			continue
		}
		p.requeue = w.committed
		primes = append(primes, p)
		c.lastStats.primed++
		if c.crashed(offsetPointPrimed, w.key) {
			return nil, errOffsetCrash
		}
	}
	return primes, nil
}

func (c *ConsumerOffsetCommitter) newPart(key offsetCommitKey) *offsetPart {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key.topic))
	_, _ = h.Write([]byte(strconv.Itoa(key.partition)))
	phase := h.Sum64() % c.every
	if c.burst {
		phase = 0
	}
	return &offsetPart{
		key:     key,
		dir:     storage.TopicPartitionDir(c.dataDir, key.topic, key.partition),
		phase:   phase,
		anchor:  -1,
		durable: -1,
		level:   -1,
	}
}

// primeLocked opens a partition's consumer.ahead for the first time
// since the committer started, since a Forget, or since its directory
// changed: it extends a file shorter than both slots (a move's copy
// writes slot 0 only; a fresh one is empty), reads both slots through
// the descriptor, and anchors on the newest valid record. That record
// may exist only in the page cache, left by a process that crashed
// before its writeout, and the first window write overwrites the other
// slot, which may hold the only durable record; so the caller writes
// the file out before any window write (syncPrimes). A missing
// directory is never recreated. Caller holds ioMu.
func (c *ConsumerOffsetCommitter) primeLocked(st *offsetPart) (offsetSync, error) {
	dir, err := os.Open(st.dir)
	if errors.Is(err, os.ErrNotExist) {
		return offsetSync{}, errOffsetDirGone
	}
	if err != nil {
		return offsetSync{}, err
	}
	keepDir := false
	defer func() {
		if !keepDir {
			_ = dir.Close()
		}
	}()
	dirInfo, err := dir.Stat()
	if err != nil {
		return offsetSync{}, err
	}
	if !dirInfo.IsDir() {
		return offsetSync{}, fmt.Errorf("consumer state partition path is not a directory: %s", st.dir)
	}
	path := filepath.Join(st.dir, storage.ConsumerAheadFileName)
	created := false
	f, err := syncfile.OpenFile(path, os.O_RDWR, storage.ConsumerStateFileMode)
	if errors.Is(err, os.ErrNotExist) {
		f, err = syncfile.OpenFile(path, os.O_RDWR|os.O_CREATE, storage.ConsumerStateFileMode)
		created = true
	}
	if errors.Is(err, os.ErrNotExist) {
		return offsetSync{}, errOffsetDirGone
	}
	if err != nil {
		return offsetSync{}, err
	}
	fail := func(err error) (offsetSync, error) {
		_ = f.Close()
		return offsetSync{}, err
	}
	// The file must have been opened in the directory held open above,
	// not in one renamed over its name since (a move's install).
	if now, err := os.Stat(st.dir); err != nil || !os.SameFile(now, dirInfo) {
		return fail(errOffsetDirGone)
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if info.Size() < consumerAheadFileSize {
		if err := syncfile.Truncate(f, consumerAheadFileSize); err != nil {
			return fail(err)
		}
	}
	buf := make([]byte, consumerAheadFileSize)
	if n, err := f.ReadAt(buf, 0); err != nil && n < len(buf) {
		return fail(err)
	}
	st.anchor, st.seq, st.durable = -1, 0, -1
	for slot := range 2 {
		rec, ok := storage.DecodeConsumerAheadSlot(buf[slot*storage.ConsumerAheadSlotSize : (slot+1)*storage.ConsumerAheadSlotSize])
		if !ok {
			continue
		}
		// The reader keeps the first of two equal sequences; so does the
		// anchor.
		if st.anchor < 0 || rec.Seq > st.seq {
			st.anchor, st.durable = slot, rec.Committed
		}
		st.seq = max(st.seq, rec.Seq)
	}
	if st.reprime && st.anchor >= 0 {
		lo := st.anchor * storage.ConsumerAheadSlotSize
		if _, err := syncfile.WriteAt(f, buf[lo:lo+storage.ConsumerAheadSlotSize], int64(lo)); err != nil {
			return fail(err)
		}
	}
	if level, ok, err := storage.ReadConsumerOffset(st.dir); err == nil && ok {
		st.level = level
	}
	st.file, st.dirInfo, st.dev = info, dirInfo, offsetDevice(info)
	st.primed, st.syncing = true, true
	st.written = offsetWritten{}
	st.dirtySince = time.Time{}
	st.needsRewrite = false
	p := offsetSync{st: st, f: f, dev: st.dev}
	if c.held < c.maxFDs || c.evictLocked() {
		st.f = f
		c.held++
	} else {
		p.transient = true
	}
	if created {
		p.dir = dir
		keepDir = true
	}
	return p, nil
}

// syncPrimes writes out every file primed this tick and flushes their
// devices, before the tick writes any window. A failed one is closed
// and primed again next tick.
func (c *ConsumerOffsetCommitter) syncPrimes(primes []offsetSync, errs *offsetErrs) error {
	if len(primes) == 0 {
		return nil
	}
	for i := range primes {
		p := &primes[i]
		p.err = c.io.writeOut(p.f)
		if p.err == nil && p.dir != nil {
			p.err = c.io.syncDir(p.dir)
		}
	}
	if c.crashed(offsetPointPrimeOut, offsetCommitKey{}) {
		return errOffsetCrash
	}
	c.flushDevices(primes)

	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	for i := range primes {
		p := &primes[i]
		if p.dir != nil {
			_ = p.dir.Close()
		}
		if p.transient {
			_ = p.f.Close()
		}
		st := p.st
		if st.dead {
			continue
		}
		if p.err != nil {
			errs.add(st.key, "prime sync", p.err)
			c.releaseLocked(st)
			st.primed, st.reprime = false, true
			c.Commit(st.key.topic, st.key.partition, p.requeue)
			continue
		}
		st.syncing, st.reprime = false, false
		c.cleanLocked(st)
	}
	return nil
}

// writeWindows writes each changed snapshot into its partition's
// window, through the held descriptor, into the page cache, and
// collects the files due for writeout: the dirty partitions whose
// phase this tick is or that have waited D, and the consumer.offset
// files due a level.
func (c *ConsumerOffsetCommitter) writeWindows(now time.Time, wants []offsetWant, hasSource bool, mode offsetTickMode, errs *offsetErrs) ([]offsetSync, error) {
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	for i := range wants {
		w := &wants[i]
		st := w.st
		if st == nil || st.dead || !st.primed || st.syncing {
			continue
		}
		committed := w.committed
		if !hasSource {
			// No source: the commit is the frontier, which never moves
			// back.
			committed = max(committed, st.frontier())
		}
		if st.written.ok && st.written.version == w.version && st.written.committed == committed && !st.needsRewrite {
			continue
		}
		seq := max(st.seq+1, uint64(now.UnixNano()))
		rec := storage.EncodeConsumerAhead(seq, committed, w.offsets)
		f, transient, err := c.fdLocked(st)
		if errors.Is(err, errOffsetDirGone) {
			c.dropLocked(st)
			continue
		}
		if err == nil {
			_, err = syncfile.WriteAt(f, rec, int64(st.window())*storage.ConsumerAheadSlotSize)
			if transient {
				_ = f.Close()
			}
		}
		if err != nil {
			errs.add(st.key, "write", err)
			st.needsRewrite = true
			// A failed write leaves the descriptor suspect; reopen.
			c.releaseLocked(st)
			c.Commit(st.key.topic, st.key.partition, committed)
			continue
		}
		st.seq = seq
		st.written = offsetWritten{version: w.version, committed: committed, ok: true}
		st.needsRewrite = false
		if st.dirtySince.IsZero() {
			st.dirtySince = now
			c.lruRemoveLocked(st)
		}
		c.lastStats.written++
		if c.crashed(offsetPointWrite, st.key) {
			return nil, errOffsetCrash
		}
	}
	if c.crashed(offsetPointWritten, offsetCommitKey{}) {
		return nil, errOffsetCrash
	}

	var syncs []offsetSync
	for _, st := range c.parts {
		if st.dead || !st.primed || st.syncing {
			continue
		}
		dirty := !st.dirtySince.IsZero()
		// A window whose last write failed is not written out: its bytes
		// are not a record until the rewrite.
		due := dirty && !st.needsRewrite && (mode != offsetTickNormal || c.tickNo%c.every == st.phase || now.Sub(st.dirtySince) >= c.durable)
		if dirty && !due {
			c.noteAge(now.Sub(st.dirtySince))
		}
		if due {
			f, transient, err := c.fdLocked(st)
			if errors.Is(err, errOffsetDirGone) {
				c.dropLocked(st)
				continue
			}
			if err != nil {
				errs.add(st.key, "reopen", err)
				c.Commit(st.key.topic, st.key.partition, st.frontier())
				continue
			}
			syncs = append(syncs, offsetSync{st: st, f: f, dev: st.dev, transient: transient, window: true})
		}
		// consumer.offset is levelled with a partition's writeout, or on
		// its phase once it went quiet, at most every 30s; Close levels
		// every one.
		frontier := st.frontier()
		levelAll := mode == offsetTickClose
		onTick := due || (!dirty && c.tickNo%c.every == st.phase)
		if frontier <= st.level || !(levelAll || onTick && now.Sub(st.levelAt) >= consumerOffsetLevelEvery) {
			continue
		}
		lv, err := c.levelLocked(st, frontier)
		if errors.Is(err, errOffsetDirGone) {
			// The window's writeout, if any, finds the directory gone too.
			continue
		}
		if err != nil {
			errs.add(st.key, "level", err)
			continue
		}
		syncs = append(syncs, lv)
		if c.crashed(offsetPointLevel, st.key) {
			return nil, errOffsetCrash
		}
	}
	return syncs, nil
}

// levelLocked writes frontier into the partition's consumer.offset in
// place, through a transient descriptor the caller writes out with the
// tick's batch. The directory must still be the one primed: its
// consumer.ahead must be the file the committer holds. Caller holds
// ioMu.
func (c *ConsumerOffsetCommitter) levelLocked(st *offsetPart, frontier int64) (offsetSync, error) {
	if err := c.checkFileLocked(st); err != nil {
		return offsetSync{}, err
	}
	path := filepath.Join(st.dir, storage.ConsumerOffsetFileName)
	created := false
	f, err := syncfile.OpenFile(path, os.O_WRONLY, storage.ConsumerStateFileMode)
	if errors.Is(err, os.ErrNotExist) {
		f, err = syncfile.OpenFile(path, os.O_WRONLY|os.O_CREATE, storage.ConsumerStateFileMode)
		created = true
	}
	if errors.Is(err, os.ErrNotExist) {
		return offsetSync{}, errOffsetDirGone
	}
	if err != nil {
		return offsetSync{}, err
	}
	buf := storage.EncodeConsumerOffset(frontier)
	if _, err := syncfile.WriteAt(f, buf[:], 0); err != nil {
		_ = f.Close()
		return offsetSync{}, err
	}
	lv := offsetSync{st: st, f: f, dev: st.dev, transient: true, level: frontier}
	if created {
		if lv.dir, err = os.Open(st.dir); err != nil {
			_ = f.Close()
			return offsetSync{}, err
		}
	}
	return lv, nil
}

// writeOut writes out the tick's batch one file at a time (see run),
// then flushes each device once. A window whose path no longer names
// the file written (the directory was removed or replaced) is not
// written out; flip drops its state.
func (c *ConsumerOffsetCommitter) writeOut(syncs []offsetSync) error {
	for i := range syncs {
		s := &syncs[i]
		if s.window && !sameFileAt(s.f, filepath.Join(s.st.dir, storage.ConsumerAheadFileName)) {
			s.gone = true
			continue
		}
		s.err = c.io.writeOut(s.f)
		if s.err == nil && s.dir != nil {
			s.err = c.io.syncDir(s.dir)
		}
	}
	if c.crashed(offsetPointWrittenOut, offsetCommitKey{}) {
		return errOffsetCrash
	}
	c.flushDevices(syncs)
	if c.crashed(offsetPointFlushed, offsetCommitKey{}) {
		return errOffsetCrash
	}
	return nil
}

// flushDevices completes the durability point of every file written
// out: one flushDevice per device, through any of that device's files
// (a concurrent Forget may have closed one). A failed flush fails every
// file of its device.
func (c *ConsumerOffsetCommitter) flushDevices(syncs []offsetSync) {
	byDev := make(map[uint64][]int, 1)
	for i := range syncs {
		if syncs[i].err == nil && !syncs[i].gone {
			byDev[syncs[i].dev] = append(byDev[syncs[i].dev], i)
		}
	}
	for _, idx := range byDev {
		var err error
		for _, i := range idx {
			if err = c.io.flushDevice(syncs[i].f); !errors.Is(err, os.ErrClosed) {
				break
			}
		}
		c.lastStats.flushes++
		if err != nil {
			for _, i := range idx {
				syncs[i].err = err
			}
		}
	}
}

// flip completes the tick: a window written out and flushed becomes the
// anchor, a level becomes known; a failure leaves the anchor alone and
// queues the partition again, its window to be rewritten in full.
func (c *ConsumerOffsetCommitter) flip(now, flipAt time.Time, syncs []offsetSync, errs *offsetErrs) {
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	for i := range syncs {
		s := &syncs[i]
		if s.transient {
			_ = s.f.Close()
		}
		if s.dir != nil {
			_ = s.dir.Close()
		}
		st := s.st
		if st.dead {
			continue
		}
		switch {
		case s.gone:
			c.dropLocked(st)
		case s.err != nil:
			errs.add(st.key, "sync", s.err)
			if s.window {
				st.needsRewrite = true
				c.Commit(st.key.topic, st.key.partition, st.frontier())
			}
		case s.window:
			c.noteAge(flipAt.Sub(st.dirtySince))
			st.anchor = st.window()
			st.durable = st.written.committed
			st.dirtySince = time.Time{}
			c.cleanLocked(st)
			c.lastStats.wroteOut++
		default:
			st.level, st.levelAt = s.level, now
			c.lastStats.levelled++
		}
	}
}

// fdLocked returns a descriptor for st's consumer.ahead: the held one,
// or one reopened by path and verified to be the file primed. Past the
// descriptor cap the least recently written clean partition gives its
// up; when none is clean the descriptor is transient and the caller
// closes it. Caller holds ioMu.
func (c *ConsumerOffsetCommitter) fdLocked(st *offsetPart) (*os.File, bool, error) {
	if st.f != nil {
		return st.f, false, nil
	}
	f, err := syncfile.OpenFile(filepath.Join(st.dir, storage.ConsumerAheadFileName), os.O_RDWR, storage.ConsumerStateFileMode)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, errOffsetDirGone
	}
	if err != nil {
		return nil, false, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, false, err
	}
	dirInfo, err := os.Stat(st.dir)
	if err != nil || !os.SameFile(info, st.file) || !os.SameFile(dirInfo, st.dirInfo) {
		_ = f.Close()
		return nil, false, errOffsetDirGone
	}
	if c.held < c.maxFDs || c.evictLocked() {
		st.f = f
		c.held++
		return f, false, nil
	}
	return f, true, nil
}

// checkFileLocked reports errOffsetDirGone when st's directory no
// longer holds the consumer.ahead the committer primed. Caller holds
// ioMu.
func (c *ConsumerOffsetCommitter) checkFileLocked(st *offsetPart) error {
	info, err := os.Stat(filepath.Join(st.dir, storage.ConsumerAheadFileName))
	if errors.Is(err, os.ErrNotExist) {
		return errOffsetDirGone
	}
	if err != nil {
		return err
	}
	want := st.file
	if st.f != nil {
		if held, err := st.f.Stat(); err == nil {
			want = held
		}
	}
	if !os.SameFile(info, want) {
		return errOffsetDirGone
	}
	return nil
}

// sameFileAt reports whether path still names the file f has open.
func sameFileAt(f *os.File, path string) bool {
	held, err := f.Stat()
	if err != nil {
		return false
	}
	now, err := os.Stat(path)
	return err == nil && os.SameFile(held, now)
}

// dropLocked forgets st: closes its descriptor and removes it, so the
// next tick for the partition primes from the files then on disk. A
// tick still holding st sees it dead. Caller holds ioMu.
func (c *ConsumerOffsetCommitter) dropLocked(st *offsetPart) {
	c.releaseLocked(st)
	st.dead = true
	if c.parts[st.key] == st {
		delete(c.parts, st.key)
	}
}

// releaseLocked closes st's held descriptor, if any. Caller holds ioMu.
func (c *ConsumerOffsetCommitter) releaseLocked(st *offsetPart) {
	c.lruRemoveLocked(st)
	if st.f != nil {
		_ = st.f.Close()
		st.f = nil
		c.held--
	}
}

// cleanLocked records that st's window matches its anchor: a clean
// partition holding a descriptor is the cap's to evict. Caller holds
// ioMu.
func (c *ConsumerOffsetCommitter) cleanLocked(st *offsetPart) {
	if st.f != nil && st.dirtySince.IsZero() {
		c.lruRemoveLocked(st)
		c.clean.pushBack(st)
	}
}

func (c *ConsumerOffsetCommitter) lruRemoveLocked(st *offsetPart) {
	if st.inLRU {
		c.clean.remove(st)
	}
}

// evictLocked closes the least recently written clean partition's
// descriptor; false when there is none. Caller holds ioMu.
func (c *ConsumerOffsetCommitter) evictLocked() bool {
	st := c.clean.front()
	if st == nil {
		return false
	}
	c.releaseLocked(st)
	return true
}

// noteAge records how long a partition's writes waited, or have
// waited so far, for their durability point, for the warning past 2D.
func (c *ConsumerOffsetCommitter) noteAge(age time.Duration) {
	if age > 2*c.durable {
		c.lastStats.late++
		c.lastStats.oldest = max(c.lastStats.oldest, age)
	}
}

// crashed consults the crash tests' hook; false in production.
func (c *ConsumerOffsetCommitter) crashed(point offsetPoint, key offsetCommitKey) bool {
	return c.crash != nil && c.crash(point, key)
}

func (c *ConsumerOffsetCommitter) drain() ([]offsetCommit, AheadSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil, c.ahead
	}
	commits := make([]offsetCommit, 0, len(c.pending))
	for key, offset := range c.pending {
		commits = append(commits, offsetCommit{key: key, offset: offset})
	}
	clear(c.pending)
	return commits, c.ahead
}

// offsetErrs keeps a tick's first error.
type offsetErrs struct{ err error }

func (e *offsetErrs) add(key offsetCommitKey, op string, err error) {
	if e.err == nil {
		e.err = fmt.Errorf("persist consumer state %s/%d: %s: %w", key.topic, key.partition, op, err)
	}
}

// offsetLRU is an intrusive list of clean partitions holding a
// descriptor, least recently written first.
type offsetLRU struct{ head, tail *offsetPart }

func (l *offsetLRU) pushBack(st *offsetPart) {
	st.lruPrev, st.lruNext, st.inLRU = l.tail, nil, true
	if l.tail != nil {
		l.tail.lruNext = st
	} else {
		l.head = st
	}
	l.tail = st
}

func (l *offsetLRU) remove(st *offsetPart) {
	if st.lruPrev != nil {
		st.lruPrev.lruNext = st.lruNext
	} else {
		l.head = st.lruNext
	}
	if st.lruNext != nil {
		st.lruNext.lruPrev = st.lruPrev
	} else {
		l.tail = st.lruPrev
	}
	st.lruPrev, st.lruNext, st.inLRU = nil, nil, false
}

func (l *offsetLRU) front() *offsetPart { return l.head }
