package storage

import (
	"errors"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Aliases of the canonical sentinels in internal/errs.
var (
	// ErrOffsetNotFound reports a read of an offset the log does not
	// hold: past the tail, or lost to retention/corruption gaps.
	ErrOffsetNotFound = errs.ErrOffsetNotFound

	// ErrCorruptRecord reports an on-disk frame that failed validation.
	ErrCorruptRecord = errs.ErrCorruptRecord

	// ErrLogClosed reports an operation on a Log after Close.
	ErrLogClosed = errs.ErrLogClosed
)

// VerifyError reports that CommitDurable's post-fsync CRC read-back of
// offsets [First, Last] failed. It wraps the underlying storage error so
// IsCorrupt and errors.Is keep working; callers classify it separately
// from a write or sync failure.
type VerifyError struct {
	First int64
	Last  int64
	Err   error
}

func (e VerifyError) Error() string {
	return "storage: durability verify [" + itoa(e.First) + "," + itoa(e.Last) + "]: " + e.Err.Error()
}

func (e VerifyError) Unwrap() error { return e.Err }

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Internal sentinels: recovery handles these by resyncing.
var (
	errBadMagic = errors.New("storage: frame magic mismatch")
	errCorrupt  = errors.New("storage: frame integrity check failed")
)

// IsCorrupt reports whether err indicates on-disk frame corruption: a CRC
// mismatch, a bad frame magic, or a malformed record stream. Such an offset is
// permanently unreadable — narad keeps a single copy, so there is no replica to
// heal from — as distinct from a transient failure (I/O error, log closed) or a
// not-yet-available offset. The consume path uses this to decide that a poison
// offset may be skipped (with the loss recorded), rather than retried forever.
func IsCorrupt(err error) bool {
	return errors.Is(err, errCorrupt) ||
		errors.Is(err, errBadMagic) ||
		errors.Is(err, ErrCorruptRecord)
}
