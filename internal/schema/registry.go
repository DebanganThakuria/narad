// Package schema is the contract for per-topic JSON Schema validation.
//
// The wiring pass ships an AlwaysValid stub so produce requests aren't
// blocked. A real draft-07 (or later) validator will land here, also
// hand-rolled to keep the zero-dep policy.
package schema

import "context"

// Registry stores per-topic JSON Schemas and validates payloads
// against them. Evolution rules (per the PRD): additive-only, no field
// removal, no type changes — those go in the real implementation.
type Registry interface {
	Register(ctx context.Context, topic string, schema []byte) (version int, err error)
	Validate(ctx context.Context, topic string, payload []byte) error
}
