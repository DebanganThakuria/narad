package storage

import (
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// One reaper goroutine for the whole process, not one per partition.
//
// Retention used to run a goroutine and a ticker per open log. That is
// two problems at scale. A node holding 150k partition logs carried
// 150k goroutines and 150k timers for work that is slow, periodic and
// entirely uncoordinated — a minute apart, by default. Worse, a log
// with retention DISABLED still parked a goroutine forever on its stop
// channel, doing nothing at all for the life of the process.
//
// Retention has no per-partition timing requirement: nothing observes
// when a sweep happens, only that expired segments eventually go. So
// one goroutine walks the registered logs and sweeps the ones that are
// due, replacing a timer per log with a single loop.
//
// The walk is O(registered logs) and it happens every reaperSweepFloor,
// so the cost is a map iteration per second rather than the per-log
// timer fires it replaced. That is a good trade at the partition counts
// this was built for and a bad one at very high ones: a node holding
// tens of thousands of logs with retention pays a tens-of-thousands
// entry scan every second, under the lock that log open and close both
// need. If that becomes the shape, the fix is a heap or timing wheel
// keyed on each entry's next deadline so a tick touches only what is
// actually due; the per-entry `next` field is already what such a
// structure would order on.
//
// Logs with no age bound are never registered at all: no goroutine, no
// entry, no work.

// reaperSweepFloor is how often the shared loop wakes, and therefore the
// granularity of every log's check interval. Retention is not
// latency-sensitive: a sweep a second late is invisible, whereas waking
// more often would trade the per-log timers this removed for one busy
// timer. A log asking for a shorter interval than this gets this.
const reaperSweepFloor = time.Second

// reaperStallAfter is how long the shared loop may go without ticking
// before it is treated as dead and a replacement is started. Generous
// against a slow sweep (one log's pass that unlinks thousands of files on
// a struggling disk, during which the loop does not tick), tight enough
// that retention resumes within a minute of the loop dying rather than
// never. Measured on a monotonic clock, so a wall-clock step (NTP, a VM
// resume) cannot look like a stall.
const reaperStallAfter = 60 * reaperSweepFloor

// reaperClockStart anchors the monotonic tick stamps: lastTick holds
// nanoseconds since this instant, and time.Since reads the monotonic
// clock.
var reaperClockStart = time.Now()

func reaperNow() int64 { return int64(time.Since(reaperClockStart)) }

// maxReaperRestarts bounds how many replacement loops a process starts.
// A loop that keeps stalling is wedged on something replacements cannot
// fix; past the cap the pool stops spawning goroutines and the metric
// plus the log line are the alarm.
const maxReaperRestarts = 16

// sharedReaper is the process-wide retention loop. Lazily started on
// the first registration so a process that opens no logs (or only logs
// without retention) never starts it.
var sharedReaper = &reaperPool{logs: make(map[*Log]*reaperEntry)}

type reaperEntry struct {
	r    *reaper
	next time.Time
	// sweeping is set while a sweep of this log is in progress. A
	// replacement loop skips such a log: if the previous sweep wedged
	// (a stuck flusher, a hung disk) the replacement must not wedge on it
	// too, and the dir is named in the log instead.
	sweeping atomic.Bool
}

type reaperPool struct {
	mu      sync.Mutex
	logs    map[*Log]*reaperEntry
	started bool
	// gen numbers the loop that is currently expected to be running. A
	// loop that finds itself superseded exits, so a replacement started
	// because the old one stalled never runs alongside it for long.
	gen uint64
	// lastTick is when the running loop last made progress, in monotonic
	// nanoseconds since reaperClockStart (reaperNow). register reads it
	// to decide whether the loop is still alive.
	lastTick atomic.Int64
	// restarts counts replacement loops started for a stalled one.
	restarts atomic.Int64
	// sweepHook, when set, runs before each log's sweep. Tests use it to
	// make one sweep panic and to count sweeps.
	sweepHook func(*Log)
}

// ReaperRestarts reports how many times the shared retention loop had to
// be replaced because it stopped ticking. Exported for the metrics
// poller; any value above zero deserves a look at the log.
func ReaperRestarts() int64 { return sharedReaper.restarts.Load() }

// register enrolls a log's reaper in the shared loop. A reaper with no
// age bound has nothing to do and is not enrolled, which is the common
// case for a topic configured to keep everything.
func (p *reaperPool) register(r *reaper) {
	if r == nil || r.cfg.MaxAge <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logs[r.log] = &reaperEntry{r: r, next: r.cfg.Now().Add(r.cfg.CheckInterval)}
	p.ensureRunningLocked()
}

// ensureRunningLocked starts the loop on first use and replaces it when
// it has stopped ticking. Called with p.mu held. The check rides on
// register because every log open goes through it: a node that keeps
// opening logs keeps checking, and a node that opens none has nothing
// for retention to do until it does.
func (p *reaperPool) ensureRunningLocked() {
	now := reaperNow()
	if !p.started {
		p.started = true
		p.gen = 1
		p.lastTick.Store(now)
		go p.run(p.gen)
		return
	}
	stalled := time.Duration(now - p.lastTick.Load())
	if stalled <= reaperStallAfter {
		return
	}
	if p.restarts.Load() >= maxReaperRestarts {
		return
	}
	p.gen++
	p.restarts.Add(1)
	p.lastTick.Store(now)
	slog.Default().Error("storage: shared retention loop stopped ticking; starting a replacement",
		"stalled_for", stalled, "generation", p.gen, "restarts", p.restarts.Load())
	go p.run(p.gen)
}

// EnsureReaperRunning replaces the shared retention loop if it has
// stopped ticking. register does the same on every log open, but a node
// whose logs are all open and stay open never registers anything, so the
// cold-retention walk calls this on each of its own ticks as a watchdog.
func EnsureReaperRunning() {
	sharedReaper.mu.Lock()
	defer sharedReaper.mu.Unlock()
	if sharedReaper.started {
		sharedReaper.ensureRunningLocked()
	}
}

// current reports whether gen is the loop expected to be running.
func (p *reaperPool) current(gen uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gen == gen
}

// unregister removes a log, called when it closes. Idempotent.
func (p *reaperPool) unregister(l *Log) {
	p.mu.Lock()
	delete(p.logs, l)
	p.mu.Unlock()
}

// run is the single retention goroutine. It never exits: the pool is
// process-wide, and a node that closed every log will simply find
// nothing to sweep.
// The tick is FIXED rather than derived from the registered intervals.
// Each entry carries its own next-sweep time, so per-log intervals are
// honoured by sweepDue regardless of how often the loop wakes; the tick
// only sets the granularity. Deriving it from the shortest registered
// interval was a bug: the ticker is created once, so a log registered
// later with a shorter interval was swept on the older, longer period.
// TestSharedReaperHonoursPerLogIntervals is the regression test.
func (p *reaperPool) run(gen uint64) {
	ticker := time.NewTicker(reaperSweepFloor)
	defer ticker.Stop()

	for range ticker.C {
		if !p.current(gen) {
			// Superseded: a replacement was started while this loop was
			// stalled. Exactly one loop sweeps from here on.
			return
		}
		p.lastTick.Store(reaperNow())
		p.sweepDueSafe()
	}
}

// sweepDueSafe runs one pass and survives it: a panic anywhere in the
// pass is logged with its stack and the loop carries on to the next
// tick. Without this one bad partition would take retention down for
// the whole process, and with the process-wide loop that is every
// partition on the node.
func (p *reaperPool) sweepDueSafe() {
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Error("storage: retention pass panicked; the loop continues",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	p.sweepDue()
}

// sweepDue runs one pass: collect the logs whose next sweep has come
// round, then sweep them with the pool lock released. Holding it across
// a sweep would serialise retention behind whichever partition has the
// slowest unlink.
func (p *reaperPool) sweepDue() {
	due, hook := p.collectDue()

	for _, e := range due {
		// A log closed between the collect and here is harmless: sweep
		// takes the log's own lock and finds nothing to do.
		p.sweepOne(e, hook)
	}
}

// collectDue picks the logs whose next sweep has come round and moves
// their schedules on, under the pool lock. The unlock is deferred so a
// panic here (a log's clock, say) surfaces through sweepDueSafe with the
// lock released, rather than wedging every later register and close.
func (p *reaperPool) collectDue() (due []*reaperEntry, hook func(*Log)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hook = p.sweepHook
	for _, e := range p.logs {
		// Each entry keeps its own clock so a test driving a fake one
		// still controls its own log's schedule.
		if e.r.cfg.Now().Before(e.next) {
			continue
		}
		if e.sweeping.Load() {
			slog.Default().Warn("storage: retention sweep still running from an earlier pass; skipping", "dir", e.r.log.dir)
			continue
		}
		due = append(due, e)
		e.next = e.r.cfg.Now().Add(e.r.cfg.CheckInterval)
	}
	return due, hook
}

// sweepOne sweeps a single log and contains its failure: a panic in one
// partition's sweep is logged with the directory and the pass moves on
// to the next log rather than skipping the rest of the node.
func (p *reaperPool) sweepOne(e *reaperEntry, hook func(*Log)) {
	r := e.r
	e.sweeping.Store(true)
	defer e.sweeping.Store(false)
	defer func() {
		if rec := recover(); rec != nil {
			slog.Default().Error("storage: retention sweep panicked for one partition; continuing",
				"dir", r.log.dir, "panic", rec, "stack", string(debug.Stack()))
		}
	}()
	// Liveness is progress, not tick cadence: a pass that unlinks
	// thousands of files on a slow disk can outlast reaperStallAfter, and
	// stamping per log keeps it from being mistaken for a dead loop.
	p.lastTick.Store(reaperNow())
	if hook != nil {
		hook(r.log)
	}
	r.sweep()
}

// snapshot lists the currently enrolled logs. Test-only.
func (p *reaperPool) snapshot() []*Log {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Log, 0, len(p.logs))
	for l := range p.logs {
		out = append(out, l)
	}
	return out
}
