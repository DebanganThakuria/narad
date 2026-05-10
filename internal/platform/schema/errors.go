package schema

import "errors"

// Sentinel errors returned by Registry implementations.
var (
	ErrSchemaNotFound = errors.New("schema: not registered for topic")
	ErrIncompatible   = errors.New("schema: incompatible with previous version")
)
