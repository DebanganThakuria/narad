package config

// StorageConfig governs the on-disk log layout.
type StorageConfig struct {
	DataDir string    `json:"data_dir"`
	Fsync   FsyncMode `json:"fsync"`
}

// FsyncMode controls how aggressively the storage layer flushes writes.
// PerWrite is safe but slow; Batched is faster but accepts a small
// data-loss window on crash.
type FsyncMode string

const (
	FsyncPerWrite FsyncMode = "per_write"
	FsyncBatched  FsyncMode = "batched"
)
