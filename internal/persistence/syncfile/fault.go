package syncfile

import (
	"errors"
	"os"
	"sync/atomic"
)

// Fault injection.
//
// Every durability-relevant file operation of the storage engine and
// the ingress WAL goes through the wrappers in this file (SyncData,
// Sync, Write, WriteAt, Truncate, OpenFile, Rename). In production the
// wrappers are the plain syscall behind one atomic nil check; a test
// can install a FaultHook to make a chosen call fail with EIO or
// ENOSPC, or to make an fsync report success without reaching the
// kernel (a lying disk), for the Nth call or for a named file.
//
// There is deliberately no build tag: the seam is the same code in CI
// and in production, so a test exercises exactly the paths a broker
// runs.

// Op identifies the file operation a FaultHook is consulted about.
type Op uint8

const (
	// OpSyncData is SyncData: the data-only sync of an append-only
	// data file (segments, the WAL, the 8-byte watermark files).
	OpSyncData Op = iota + 1
	// OpSync is Sync: a full fsync (directories, truncates).
	OpSync
	// OpWrite is Write or WriteAt.
	OpWrite
	// OpTruncate is Truncate.
	OpTruncate
	// OpOpen is OpenFile (segment creation on a roll, watermark files).
	OpOpen
	// OpRename is Rename (atomic replace of a marker or cursor file).
	OpRename
)

// String names the operation for test logs.
func (op Op) String() string {
	switch op {
	case OpSyncData:
		return "syncdata"
	case OpSync:
		return "sync"
	case OpWrite:
		return "write"
	case OpTruncate:
		return "truncate"
	case OpOpen:
		return "open"
	case OpRename:
		return "rename"
	}
	return "unknown"
}

// FaultHook decides the fate of one file operation before it runs.
// path is the file's name (os.File.Name for an open handle, the
// destination for Rename). Returning nil lets the real syscall run; any
// other error fails the operation without performing it, and the
// wrapper returns that error to its caller. Returning ErrLie makes the
// wrapper report success without performing the operation: the fsync
// that returns before the bytes reached stable storage, or the write
// that a failing controller acknowledged and dropped.
//
// The hook runs on the calling goroutine, under whatever locks the
// caller holds. It must be fast and must not call back into the
// packages it is injected into.
type FaultHook func(op Op, path string) error

// ErrLie is the FaultHook result that skips the operation and reports
// success. See FaultHook.
var ErrLie = errors.New("syncfile: fault hook: report success without performing the operation")

// faultHook is the installed hook, nil in production. atomic so a test
// can install and clear it while flusher goroutines are running.
var faultHook atomic.Pointer[FaultHook]

// SetFaultHook installs h (nil clears the hook) and returns a function
// that restores the previous hook. Tests only: the hook is global to
// the process, so tests that install one must not run in parallel with
// each other.
func SetFaultHook(h FaultHook) (restore func()) {
	var prev *FaultHook
	if h == nil {
		prev = faultHook.Swap(nil)
	} else {
		prev = faultHook.Swap(&h)
	}
	return func() { faultHook.Store(prev) }
}

// consult asks the installed hook, if any, about one operation. skip
// reports that the syscall must not run; err is what the wrapper
// returns in that case (nil for a lie).
func consult(op Op, path string) (skip bool, err error) {
	h := faultHook.Load()
	if h == nil {
		return false, nil
	}
	err = (*h)(op, path)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, ErrLie) {
		return true, nil
	}
	return true, err
}

// SyncData makes f's data (and the size needed to read it back)
// durable with the cheapest primitive the kernel offers; see the
// package documentation for the per-OS choice.
func SyncData(f *os.File) error {
	if skip, err := consult(OpSyncData, f.Name()); skip {
		return err
	}
	return syncData(f)
}

// Sync is os.File.Sync: the full fsync, for directory handles and for a
// truncate whose new size must survive a crash.
func Sync(f *os.File) error {
	if skip, err := consult(OpSync, f.Name()); skip {
		return err
	}
	return f.Sync()
}

// Write is os.File.Write. A lying hook reports the whole buffer
// written.
func Write(f *os.File, b []byte) (int, error) {
	if skip, err := consult(OpWrite, f.Name()); skip {
		if err != nil {
			return 0, err
		}
		return len(b), nil
	}
	return f.Write(b)
}

// WriteAt is os.File.WriteAt. A lying hook reports the whole buffer
// written.
func WriteAt(f *os.File, b []byte, off int64) (int, error) {
	if skip, err := consult(OpWrite, f.Name()); skip {
		if err != nil {
			return 0, err
		}
		return len(b), nil
	}
	return f.WriteAt(b, off)
}

// Truncate is os.File.Truncate.
func Truncate(f *os.File, size int64) error {
	if skip, err := consult(OpTruncate, f.Name()); skip {
		return err
	}
	return f.Truncate(size)
}

// OpenFile is os.OpenFile. A lie cannot open a file, so a lying hook
// lets the open proceed.
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if skip, err := consult(OpOpen, name); skip && err != nil {
		return nil, err
	}
	return os.OpenFile(name, flag, perm)
}

// Rename is os.Rename; the hook sees the destination path.
func Rename(oldpath, newpath string) error {
	if skip, err := consult(OpRename, newpath); skip {
		return err
	}
	return os.Rename(oldpath, newpath)
}
