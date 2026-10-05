package metastore

// Fail-stop: the FSM never consumes an entry it could not apply.
//
// Apply used to treat every error alike: returned as the entry's
// response, the entry counted as applied. That is right for a
// deterministic refusal (not found, a compare-and-set miss, a removed
// member, a schema out of order), which every replica reaches the same
// way, and for an entry whose bytes do not decode, which is identical
// on every replica. It is wrong for two other kinds, which dropped a
// committed change on this replica alone, silently and for good, while
// the node kept reporting ready:
//
//   - an entry type this build does not know, proposed by a newer
//     release's leader;
//   - a write the local database refused (a full volume while bbolt
//     grows the file at commit, an I/O error, a closed database).
//
// For both the FSM now stops applying. It logs why at error once,
// answers every later Apply, Snapshot and Restore with ErrStoppedApplying
// (which wraps errs.ErrUnavailable: an unknown outcome for the proposer,
// never a refusal), and has Raft shut down so the node stops voting,
// leading and acknowledging writes its own replica lacks. serve then
// exits non-zero. The entry is not marked applied, so a restart replays
// it: after the disk is fixed it applies, and on a build that does not
// know its type it stops again, a loud crash loop with the reason
// rather than a diverged replica.
//
// A storage failure is first retried in place, 100 ms doubling to 5 s
// for up to 30 s, since a volume can recover (space freed, a transient
// I/O error). Only bbolt's Begin and Commit (and the index write) count
// as storage failures: the apply closure only edits the transaction in
// memory, bbolt allocates pages at commit, so a closure error is always
// the deterministic kind.

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// ErrStoppedApplying is wrapped by every answer of an FSM that has
// stopped applying Raft entries. The error also wraps
// errs.ErrUnavailable, so a proposer reads the outcome as unknown.
var ErrStoppedApplying = errors.New("metastore: stopped applying raft entries")

// Storage retry ladder and transaction seams; vars so tests can shorten
// the ladder and fail a commit.
var (
	applyRetryInitial = 100 * time.Millisecond
	applyRetryMax     = 5 * time.Second
	applyRetryBudget  = 30 * time.Second
	applySleep        = time.Sleep
	applyNow          = time.Now

	beginWriteTx = func(db *bolt.DB) (*bolt.Tx, error) { return db.Begin(true) }
	commitTx     = func(tx *bolt.Tx) error { return tx.Commit() }
)

// Apply error kinds, counted for the metastore metrics.
const (
	applyErrStorage = iota
	applyErrUnknownEntryType
	applyErrUndecodable
	applyErrNewerDatabase
	applyErrKinds
)

// stoppedError is the error an FSM that stopped applying answers with.
type stoppedError struct {
	msg   string
	cause error
}

func (e *stoppedError) Error() string { return e.msg }

func (e *stoppedError) Unwrap() []error {
	return []error{ErrStoppedApplying, errs.ErrUnavailable, e.cause}
}

// haltState records that the FSM stopped applying. The zero value is
// usable: ch is made on first use.
type haltState struct {
	once   sync.Once
	chOnce sync.Once
	ch     chan struct{}
	err    atomic.Pointer[stoppedError]
	// hook, once set, runs (at most once) when the FSM stops: the Store
	// sets it to shut Raft down. It runs on its own goroutine, never
	// inside Apply.
	hook     atomic.Pointer[func()]
	hookOnce sync.Once
}

func (h *haltState) channel() chan struct{} {
	h.chOnce.Do(func() { h.ch = make(chan struct{}) })
	return h.ch
}

func (h *haltState) runHook() {
	if fn := h.hook.Load(); fn != nil {
		h.hookOnce.Do(func() { go (*fn)() })
	}
}

// stopErr returns the error the FSM answers with once it has stopped
// applying, or nil.
func (f *fsmState) stopErr() error {
	if e := f.halt.err.Load(); e != nil {
		return e
	}
	return nil
}

// setOnHalt sets what runs when the FSM stops; if it already has
// stopped (while Raft restored a snapshot at start, say), it runs now.
func (f *fsmState) setOnHalt(fn func()) {
	f.halt.hook.Store(&fn)
	if f.stopErr() != nil {
		f.halt.runHook()
	}
}

// stop records why the FSM stops applying, logs it once at error, and
// returns the error every later Apply, Snapshot and Restore answers
// with. index is the entry it stopped on (0 for a snapshot); msg says
// what happened and what to do about it.
func (f *fsmState) stop(index uint64, entryType uint64, msg string, cause error) error {
	f.halt.once.Do(func() {
		e := &stoppedError{msg: ErrStoppedApplying.Error() + ": " + msg, cause: cause}
		f.halt.err.Store(e)
		f.log.Error("metastore: stopped applying raft entries; this node leaves raft and exits",
			"index", index, "entry_type", entryType, "build", buildName(f.build), "error", e)
		close(f.halt.channel())
		f.halt.runHook()
	})
	return f.stopErr()
}

// stopOnUnknownEntryType stops on an entry a newer release proposed.
func (f *fsmState) stopOnUnknownEntryType(index, entryType uint64) error {
	f.applyErrors[applyErrUnknownEntryType].Add(1)
	cause := fmt.Errorf("unknown raft entry type %d", entryType)
	return f.stop(index, entryType, fmt.Sprintf(
		"raft entry at index %d has entry type %d, which this build (%s) does not know (it applies entry types 1 to %d): a newer Narad release proposed it; run the cluster's release, or newer, on this node",
		index, entryType, buildName(f.build), MaxEntryType), cause)
}

// update runs fn in a write transaction. Inside Apply it is the entry's
// transaction (applyTx); anywhere else (tests, tools) a plain update.
func (f *fsmState) update(fn func(*bolt.Tx) error) error {
	if f.cur.index == 0 {
		return f.db.Update(fn)
	}
	return f.applyTx(fn)
}

// applyTx commits fn's effects together with the entry's index. fn's
// error is the entry's deterministic answer and is returned as is. A
// storage failure is retried for applyRetryBudget, and then stops the
// FSM without consuming the entry.
func (f *fsmState) applyTx(fn func(*bolt.Tx) error) error {
	env, err := f.tryApplyTx(fn)
	if !env {
		return err
	}
	f.applyErrors[applyErrStorage].Add(1)
	f.stalled.Store(true)
	defer f.stalled.Store(false)
	f.log.Warn("metastore: could not write raft entry; retrying",
		"index", f.cur.index, "entry_type", f.cur.entryType, "path", f.dbPath, "error", err, "retry_for", applyRetryBudget)
	started := applyNow()
	deadline := started.Add(applyRetryBudget)
	for delay := applyRetryInitial; applyNow().Add(delay).Before(deadline); delay = min(2*delay, applyRetryMax) {
		applySleep(delay)
		if env, err = f.tryApplyTx(fn); !env {
			if err == nil {
				f.log.Info("metastore: wrote raft entry on retry", "index", f.cur.index, "after", applyNow().Sub(started).Round(time.Millisecond))
			}
			return err
		}
	}
	return f.stop(f.cur.index, uint64(f.cur.entryType), fmt.Sprintf(
		"could not write raft entry at index %d (entry type %d) to %s after retrying for %s: %v; free space or fix the volume, then restart the node",
		f.cur.index, f.cur.entryType, f.dbPath, applyNow().Sub(started).Round(time.Millisecond), err), err)
}

// tryApplyTx is one begin, fn, index write and commit. env reports that
// err came from the storage rather than from fn.
func (f *fsmState) tryApplyTx(fn func(*bolt.Tx) error) (env bool, err error) {
	tx, err := beginWriteTx(f.db)
	if err != nil {
		return true, err
	}
	defer func() {
		// Still open when fn failed or panicked; a failed Commit has
		// already rolled back (DB() is nil then).
		if tx.DB() != nil {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return false, err
	}
	if err := putFSMMeta(tx, f.cur.index, f.cur.entryType); err != nil {
		return true, err
	}
	if err := commitTx(tx); err != nil {
		return true, err
	}
	f.cur.persisted = true
	return false, nil
}
