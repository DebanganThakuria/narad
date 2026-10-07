package remote

import (
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Rate limits of the registry (ch. 5.10).
const (
	// WritesPerMinute is the per-node remote write limit: each write
	// costs a seal from a bounded budget and a Raft entry.
	WritesPerMinute = 10
	// CheckInterval is the least time between two checks of one remote
	// on one member, whoever asked.
	CheckInterval = 5 * time.Second
)

// WriteLimiter allows WritesPerMinute remote writes in any sliding
// minute on this node.
type WriteLimiter struct {
	mu    sync.Mutex
	times []time.Time
	limit int
	now   func() time.Time
}

// NewWriteLimiter returns a limiter of WritesPerMinute.
func NewWriteLimiter() *WriteLimiter {
	return &WriteLimiter{limit: WritesPerMinute, now: time.Now}
}

// Allow takes one write, reporting false when the minute is full.
func (l *WriteLimiter) Allow() bool {
	_, ok := l.Take()
	return ok
}

// Take takes one write, or reports false and how long until the
// sliding minute frees a slot (when its oldest write leaves it).
func (l *WriteLimiter) Take() (wait time.Duration, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-time.Minute)
	kept := l.times[:0]
	for _, t := range l.times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	l.times = kept
	if len(l.times) >= l.limit {
		return l.times[len(l.times)-l.limit].Add(time.Minute).Sub(now), false
	}
	l.times = append(l.times, now)
	return 0, true
}

// WriteThrottledError is errs.ErrRemoteThrottled from the write limit,
// with the wait until it frees a slot (the 429's Retry-After).
type WriteThrottledError struct {
	RetryAfter time.Duration
}

func (e *WriteThrottledError) Error() string { return errs.ErrRemoteThrottled.Error() }

func (e *WriteThrottledError) Unwrap() error { return errs.ErrRemoteThrottled }

// CheckLimiter lets one check per remote run at a time on this member,
// and at most one start per CheckInterval. The limit lives on the node
// that runs the check, so spreading requests over ingress nodes gains
// nothing.
type CheckLimiter struct {
	mu      sync.Mutex
	running map[string]bool
	last    map[string]time.Time
	every   time.Duration
	now     func() time.Time
}

// NewCheckLimiter returns a limiter of one check per CheckInterval.
func NewCheckLimiter() *CheckLimiter {
	return &CheckLimiter{running: map[string]bool{}, last: map[string]time.Time{}, every: CheckInterval, now: time.Now}
}

// Acquire starts a check of remote, or reports false (429). The
// returned release ends it.
func (l *CheckLimiter) Acquire(remote string) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.running[remote] || (!l.last[remote].IsZero() && now.Sub(l.last[remote]) < l.every) {
		return nil, false
	}
	l.running[remote] = true
	l.last[remote] = now
	return func() {
		l.mu.Lock()
		delete(l.running, remote)
		l.mu.Unlock()
	}, true
}
