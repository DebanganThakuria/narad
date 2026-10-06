package storage

import "time"

// Retention alters on an open log.
//
// A log's age bound used to be fixed when it opened. A topic's retention
// alter reached only the node that ran it (the Raft leader closes its
// cached logs there), and every other owner kept serving, and reaping, a
// partition under the bound it opened with: raising retention to protect
// a backlog still deleted records at the old bound, and lowering it to
// free disk freed nothing. SetRetentionMaxAge swaps the bound in place,
// so the owner of the per-node log map can apply an altered topic record
// to the logs it already has open without closing them under their
// readers.

// SetRetentionMaxAge makes d the log's age-based retention bound from
// now on (zero or negative keeps forever) and reports whether it
// changed. The shared reaper's enrolment follows: a log that gains a
// bound is enrolled, one that loses it leaves the loop, and a changed
// bound is swept at the loop's next tick rather than a check interval
// later, so a lowered bound frees disk promptly. The age-based roll of
// the active segment follows the new bound too. A closed log records
// the value and stays out of the loop. Safe for concurrent use with
// every other Log method, Close included.
func (l *Log) SetRetentionMaxAge(d time.Duration) (changed bool) {
	if l == nil || l.reaper == nil {
		return false
	}
	return sharedReaper.retune(l.reaper, max(d, 0))
}

// retune swaps r's live bound and re-enrols it, under the pool lock.
// Close marks the log closed before it deregisters under the same lock,
// so a retune that still sees the log open enrols it before that
// deregistration runs, and one that runs after the mark sees the log
// closed and leaves it out: a closed log is never left in the loop.
func (p *reaperPool) retune(r *reaper, d time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Duration(r.maxAge.Swap(int64(d))) == d {
		return false
	}
	if d <= 0 || r.log.closed.Load() {
		delete(p.logs, r.log)
		return true
	}
	now := r.cfg.Now()
	if e, ok := p.logs[r.log]; ok {
		if now.Before(e.next) {
			e.next = now
		}
		return true
	}
	p.logs[r.log] = &reaperEntry{r: r, next: now}
	p.ensureRunningLocked()
	return true
}
