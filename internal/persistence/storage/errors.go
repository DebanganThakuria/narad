package storage

import (
	"errors"

	"github.com/debanganthakuria/narad/internal/errs"
)

// Aliases of the canonical sentinels in internal/errs.
var (
	ErrOffsetNotFound = errs.ErrOffsetNotFound
	ErrCorruptRecord  = errs.ErrCorruptRecord
	ErrLogClosed      = errs.ErrLogClosed
)

// Internal sentinels — recovery handles these by resyncing.
var (
	errBadMagic = errors.New("storage: frame magic mismatch")
	errCorrupt  = errors.New("storage: frame integrity check failed")
)
