package schema

import "context"

// AlwaysValid is a permissive Registry that accepts every schema and
// every payload. Use it in tests and wherever schema enforcement is
// disabled.
type AlwaysValid struct{}

// NewAlwaysValid returns the permissive stub.
func NewAlwaysValid() AlwaysValid { return AlwaysValid{} }

// ValidateDefinition accepts any schema definition.
func (AlwaysValid) ValidateDefinition(_ context.Context, _ string, _ []byte) error {
	return nil
}

// CheckCompatible accepts any evolution.
func (AlwaysValid) CheckCompatible(_ context.Context, _ string, _, _ []byte) error {
	return nil
}

// Load accepts any persisted schema.
func (AlwaysValid) Load(_ context.Context, _ string, _ int, _ []byte) error {
	return nil
}

// ReplaceTopic accepts any history.
func (AlwaysValid) ReplaceTopic(_ context.Context, _ string, _ []Version) error {
	return nil
}

// DropTopic drops every schema version for a topic.
func (AlwaysValid) DropTopic(_ context.Context, _ string) error {
	return nil
}

// Validate accepts any payload.
func (AlwaysValid) Validate(_ context.Context, _ string, _ []byte) error {
	return nil
}
