// Package schema is the contract for per-topic JSON Schema validation.
//
// The wiring pass ships a JSON Schema validator backed by
// github.com/santhosh-tekuri/jsonschema. A permissive AlwaysValid
// stub is available for tests.
package schema

import "context"

// Registry stores per-topic JSON Schemas and validates payloads
// against them. Evolution rules: additive-only, no field
// removal, no type changes — those go in the real implementation.
type Registry interface {
	Register(ctx context.Context, topic string, schema []byte) (version int, err error)
	Validate(ctx context.Context, topic string, payload []byte) error
}
