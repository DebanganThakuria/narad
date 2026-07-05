package storage

// NotifyC returns the current broadcast channel. It is CLOSED (never
// sent on) whenever new records may have become available — pushed
// into the buffer, flushed to disk, made visible by an HWM advance,
// or on Wake/Close — so every waiter blocked on it wakes at once.
// Because the channel is replaced after each broadcast, callers must
// fetch it BEFORE checking for data and re-fetch it before every
// subsequent wait.
func (l *Log) NotifyC() <-chan struct{} {
	l.notifyMu.Lock()
	l.notifyWaiters = true
	ch := l.notify
	l.notifyMu.Unlock()
	return ch
}

// notifyAll broadcasts to every goroutine blocked on the channel
// returned by NotifyC: it closes the current channel and installs a
// fresh one for the next round of waiters. When no one fetched the
// current channel (notifyWaiters clear) there is nothing to wake, so it
// returns without touching the channel — keeping Append/AppendBatch/
// AdvanceHighWatermark allocation-free in the no-waiter common case.
func (l *Log) notifyAll() {
	l.notifyMu.Lock()
	if l.notifyWaiters {
		close(l.notify)
		l.notify = make(chan struct{})
		l.notifyWaiters = false
	}
	l.notifyMu.Unlock()
}

// Wake broadcasts to long-poll waiters without any log-state change.
// Used when records become deliverable again for reasons the log
// cannot see — e.g. an in-flight reservation's visibility timeout
// expired. Safe to call concurrently and after Close.
func (l *Log) Wake() { l.notifyAll() }
