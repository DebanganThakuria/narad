package metastore

import "errors"

// Sentinel errors returned by Metastore implementations.
var (
	ErrNotFound      = errors.New("metastore: not found")
	ErrAlreadyExists = errors.New("metastore: already exists")
)
