package storage

import "errors"

// Sentinel errors returned by the Log API. Callers should compare with
// errors.Is rather than ==.
var (
	ErrOffsetNotFound = errors.New("storage: offset not found")
	ErrCorruptRecord  = errors.New("storage: corrupt record")
)
