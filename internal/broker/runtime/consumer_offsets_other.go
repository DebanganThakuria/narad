//go:build !unix

package runtime

import (
	"os"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// offsetWriteOut makes f durable on its own where there is no cheaper
// write-out-then-flush split: syncfile.SyncData.
func offsetWriteOut(f *os.File) error { return syncfile.SyncData(f) }

// offsetFlushDevice has nothing left to do: offsetWriteOut was durable.
func offsetFlushDevice(*os.File) error { return nil }

// offsetSyncDir makes a directory's new entry durable on its own.
func offsetSyncDir(dir *os.File) error { return syncfile.Sync(dir) }

// offsetDevice groups every file on one device: offsetFlushDevice is a
// no-op here.
func offsetDevice(os.FileInfo) uint64 { return 0 }

// offsetFDCap is the committer's descriptor cap where there is no
// open-file limit to take a share of.
func offsetFDCap() int { return consumerOffsetMaxFDs }
