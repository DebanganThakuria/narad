package storage

import (
	"sync"
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

// sharedReaper is the process-wide retention loop. Lazily started on
// the first registration so a process that opens no logs (or only logs
// without retention) never starts it.
var sharedReaper = &reaperPool{logs: make(map[*Log]*reaperEntry)}

type reaperEntry struct {
	r    *reaper
	next time.Time
}

type reaperPool struct {
	mu      sync.Mutex
	logs    map[*Log]*reaperEntry
	started bool
}

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
	if !p.started {
		p.started = true
		go p.run()
	}
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
func (p *reaperPool) run() {
	ticker := time.NewTicker(reaperSweepFloor)
	defer ticker.Stop()

	for range ticker.C {
		p.sweepDue()
	}
}

// sweepDue runs one pass: collect the logs whose next sweep has come
// round, then sweep them with the pool lock released. Holding it across
// a sweep would serialise retention behind whichever partition has the
// slowest unlink.
func (p *reaperPool) sweepDue() {
	var due []*reaper

	p.mu.Lock()
	for _, e := range p.logs {
		// Each entry keeps its own clock so a test driving a fake one
		// still controls its own log's schedule.
		if e.r.cfg.Now().Before(e.next) {
			continue
		}
		due = append(due, e.r)
		e.next = e.r.cfg.Now().Add(e.r.cfg.CheckInterval)
	}
	p.mu.Unlock()

	for _, r := range due {
		// A log closed between the collect and here is harmless: sweep
		// takes the log's own lock and finds nothing to do.
		r.sweep()
	}
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
