// Package schema is the contract for per-topic JSON Schema validation.
//
// The wiring pass ships a JSON Schema validator backed by
// github.com/santhosh-tekuri/jsonschema. A permissive AlwaysValid
// stub is available for tests.
package schema

import "context"

// Registry stores per-topic JSON Schemas and validates payloads
// against them. Schema evolution is additive-only — no property removal
// and no type changes — enforced by the JSONSchema implementation on
// Register.
type Registry interface {
	ValidateDefinition(ctx context.Context, topic string, schema []byte) error
	Register(ctx context.Context, topic string, schema []byte) (version int, err error)
	Load(ctx context.Context, topic string, version int, schema []byte) error
	Unload(ctx context.Context, topic string, version int) error
	Validate(ctx context.Context, topic string, payload []byte) error
}
