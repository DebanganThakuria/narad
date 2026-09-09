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
// due. The walk is O(open logs) but runs on the order of once a minute,
// not once a second, so it costs a rounding error of CPU where the old
// shape cost a timer per log.
//
// Logs with no age bound are never registered at all: no goroutine, no
// entry, no work.

// reaperSweepFloor bounds how often the shared loop wakes, however
// short a topic's configured check interval is. Retention is not
// latency-sensitive, and waking more often than this would trade the
// per-log timers we just removed for one busy timer.
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
	// tick is the loop's period: the shortest check interval any
	// registered log asked for, floored at reaperSweepFloor.
	tick time.Duration
}

// register enrolls a log's reaper in the shared loop. A reaper with no
// age bound has nothing to do and is not enrolled, which is the common
// case for a topic configured to keep everything.
func (p *reaperPool) register(r *reaper) {
	if r == nil || r.cfg.MaxAge <= 0 {
		return
	}
	interval := max(r.cfg.CheckInterval, reaperSweepFloor)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.logs[r.log] = &reaperEntry{r: r, next: r.cfg.Now().Add(r.cfg.CheckInterval)}
	if p.tick == 0 || interval < p.tick {
		p.tick = interval
	}
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
func (p *reaperPool) run() {
	p.mu.Lock()
	tick := p.tick
	p.mu.Unlock()
	ticker := time.NewTicker(tick)
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
