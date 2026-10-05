package runtime

// Containing the cold-retention walk's failures.
//
// The walk runs one partition's retention sweep synchronously on its own
// goroutine, which serve.go starts with nothing above it to recover. The
// shared reaper's loop contains a panicking sweep per log (storage's
// sweepOne) and goes on; the walk did not, so a panic in one closed
// partition's sweep (a corrupt segment, a metrics recorder) took the
// whole node down, and a deterministic one did it again on every
// restart. The walk now contains a panic per partition the same way:
// logged at error with the partition's name and the stack, counted in
// narad_cold_retention_panics_total, the walk-owned log closed, the
// partition left alone for coldWalkRetryAfter, and the walk carries on
// with the next one.

import (
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// newColdPanicsCounter is narad_cold_retention_panics_total, unregistered:
// serve.go registers it on the process registry (ColdRetentionPanics).
func newColdPanicsCounter() prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Name: "narad_cold_retention_panics_total",
		Help: "Cold retention walk partitions whose open, sweep or close panicked; each was contained, logged at error with its stack, and left alone for 30 minutes.",
	})
}

// ColdRetentionPanics is narad_cold_retention_panics_total for this
// node's walk. Any value above zero deserves a look at the error log,
// which names the partition and carries the stack.
func (g *Logs) ColdRetentionPanics() prometheus.Counter { return g.coldPanics }

// errColdSweepPanicked reports that a partition's open, sweep or close
// in the cold walk panicked; the panic was contained.
var errColdSweepPanicked = errors.New("cold retention: partition sweep panicked")

// coldPanicError carries a sweep panic contained by sweepContained to
// sweepColdSafe, which logs it with the partition's name.
type coldPanicError struct {
	value any
	stack []byte
}

func (e *coldPanicError) Error() string {
	return fmt.Sprintf("%v: %v", errColdSweepPanicked, e.value)
}

func (e *coldPanicError) Unwrap() error { return errColdSweepPanicked }

// sweepColdSafe is sweepColdPartition surviving a panic anywhere in the
// partition's open, sweep or close. A panic, contained here or in the
// sweep itself (sweepContained), is logged and counted and comes back as
// an error wrapping errColdSweepPanicked.
func (g *Logs) sweepColdSafe(topicName string, idx int) (changed bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			stack := debug.Stack()
			g.closeWalkOwnedAfterPanic(topicName, idx)
			changed, err = false, g.coldPanic(topicName, idx, rec, stack)
		}
	}()
	changed, err = g.sweepColdPartition(topicName, idx)
	var pe *coldPanicError
	if errors.As(err, &pe) {
		return false, g.coldPanic(topicName, idx, pe.value, pe.stack)
	}
	return changed, err
}

// sweepContained runs one retention sweep of a log the walk opened,
// surviving a panic in it, so the caller still closes the log.
func sweepContained(l *storage.Log) (changed bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			changed = false
			err = &coldPanicError{value: rec, stack: debug.Stack()}
		}
	}()
	return l.SweepRetentionNow(), nil
}

// closeWalkOwnedAfterPanic closes the partition's log if the walk still
// owns it after a panic outside the sweep (in the open or the close), so
// a walk-owned entry nobody would close is not left open for good. It
// survives a second panic: it runs from a recover, where one would
// escape.
func (g *Logs) closeWalkOwnedAfterPanic(topicName string, idx int) {
	defer func() {
		if rec := recover(); rec != nil {
			g.logger.Error("cold retention: closing a partition after a contained panic panicked too; its log stays open until idle eviction or shutdown",
				"topic", topicName, "partition", idx, "panic", rec)
		}
	}()
	key := keyOf(topicName, idx)
	g.mu.RLock()
	e, ok := g.logs[key]
	g.mu.RUnlock()
	if !ok || !e.walkOwned.Load() {
		return
	}
	_, _ = g.closeIfStill(key, e, func(cur *logEntry) bool { return cur.walkOwned.Load() }, "cold_retention_close")
}

// coldPanic logs and counts a contained panic of one partition's walk
// and returns the error the walk carries on with.
func (g *Logs) coldPanic(topicName string, idx int, rec any, stack []byte) error {
	g.logger.Error("cold retention: partition sweep panicked; the walk continues and leaves the partition alone for a while",
		"topic", topicName, "partition", idx, "panic", rec, "stack", string(stack),
		"retry_after", coldWalkRetryAfter)
	g.coldPanics.Inc()
	return fmt.Errorf("%w: %s/%d: %v", errColdSweepPanicked, topicName, idx, rec)
}
