package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// JSONSchema is a schema.Registry backed by santhosh-tekuri/jsonschema.
// Schemas are pre-compiled on Register for fast repeated validation.
type JSONSchema struct {
	mu       sync.RWMutex
	versions map[string]int                       // topic → latest version number
	schemas  map[string]map[int]*jsonschema.Schema // topic → version → compiled schema
}

// NewJSONSchema returns an empty JSONSchema registry.
func NewJSONSchema() *JSONSchema {
	return &JSONSchema{
		versions: map[string]int{},
		schemas:  map[string]map[int]*jsonschema.Schema{},
	}
}

// Register compiles the JSON Schema bytes and stores it under topic. It
// returns the auto-assigned version number (monotonically increasing per
// topic, starting at 1).
func (r *JSONSchema) Register(_ context.Context, topic string, schemaBytes []byte) (int, error) {
	schemaDoc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(schemaBytes)))
	if err != nil {
		return 0, fmt.Errorf("schema: invalid JSON: %w", err)
	}

	c := jsonschema.NewCompiler()
	resource := topic + ".json"
	if err := c.AddResource(resource, schemaDoc); err != nil {
		return 0, fmt.Errorf("schema: %w", err)
	}
	compiled, err := c.Compile(resource)
	if err != nil {
		return 0, fmt.Errorf("schema: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	version := r.versions[topic] + 1
	r.versions[topic] = version
	if r.schemas[topic] == nil {
		r.schemas[topic] = map[int]*jsonschema.Schema{}
	}
	r.schemas[topic][version] = compiled

	return version, nil
}

// Validate unmarshals the payload and checks it against the latest
// compiled schema for the topic. Returns ErrSchemaNotFound if no schema
// has been registered.
func (r *JSONSchema) Validate(_ context.Context, topic string, payload []byte) error {
	r.mu.RLock()
	version, ok := r.versions[topic]
	if !ok {
		r.mu.RUnlock()
		return ErrSchemaNotFound
	}
	compiled := r.schemas[topic][version]
	r.mu.RUnlock()

	var instance any
	if err := json.Unmarshal(payload, &instance); err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}

	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	return nil
}