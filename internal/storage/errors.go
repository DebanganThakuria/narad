package storage

import "errors"

var (
	ErrOffsetNotFound = errors.New("storage: offset not found")
	ErrCorruptRecord  = errors.New("storage: corrupt record")
	ErrLogClosed      = errors.New("storage: log closed")
)

// Internal — recovery handles these by resyncing.
var (
	errBadMagic = errors.New("storage: frame magic mismatch")
	errCorrupt  = errors.New("storage: frame integrity check failed")
)
