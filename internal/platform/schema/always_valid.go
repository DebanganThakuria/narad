package schema

import "context"

// AlwaysValid is a permissive schema.Registry that accepts every payload.
// It is intended for tests and for running with validation disabled;
// production uses JSONSchema.
type AlwaysValid struct{}

// NewAlwaysValid returns the permissive stub.
func NewAlwaysValid() AlwaysValid { return AlwaysValid{} }

// Register pretends to register a schema and always returns version 1.
func (AlwaysValid) Register(_ context.Context, _ string, _ []byte) (int, error) {
	return 1, nil
}

// ValidateDefinition accepts any schema definition.
func (AlwaysValid) ValidateDefinition(_ context.Context, _ string, _ []byte) error {
	return nil
}

// Load accepts any persisted schema.
func (AlwaysValid) Load(_ context.Context, _ string, _ int, _ []byte) error {
	return nil
}

// Unload drops a schema version.
func (AlwaysValid) Unload(_ context.Context, _ string, _ int) error {
	return nil
}

// Validate accepts any payload.
func (AlwaysValid) Validate(_ context.Context, _ string, _ []byte) error {
	return nil
}
