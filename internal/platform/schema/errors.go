package schema

import "github.com/debanganthakuria/narad/internal/errs"

// Aliases of the canonical sentinels in internal/errs.
var (
	ErrSchemaNotFound = errs.ErrSchemaNotFound
	ErrIncompatible   = errs.ErrSchemaIncompatible
)
