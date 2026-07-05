// Package schema validates message payloads against per-topic JSON
// Schemas.
//
// JSONSchema is the production Registry, backed by
// github.com/santhosh-tekuri/jsonschema. AlwaysValid is a permissive
// stub for tests and for topics without schema enforcement.
package schema

import "context"

// Registry stores per-topic JSON Schemas and validates payloads against
// the latest registered version. Register enforces additive-only
// evolution: no property removal, no type changes, no new required
// fields — every document accepted by the previous version must remain
// valid.
//
//   - ValidateDefinition checks that a schema compiles without storing it.
//   - Register stores a schema and returns its auto-assigned version.
//   - Load rehydrates a persisted (topic, version, schema) at startup.
//   - Unload removes one version; DropTopic removes all of a topic's
//     versions so a recreated topic starts schema-less.
//   - Validate checks a payload against the topic's latest schema.
type Registry interface {
	ValidateDefinition(ctx context.Context, topic string, schema []byte) error
	Register(ctx context.Context, topic string, schema []byte) (version int, err error)
	Load(ctx context.Context, topic string, version int, schema []byte) error
	Unload(ctx context.Context, topic string, version int) error
	DropTopic(ctx context.Context, topic string) error
	Validate(ctx context.Context, topic string, payload []byte) error
}
