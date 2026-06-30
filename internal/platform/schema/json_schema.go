package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// JSONSchema is a schema.Registry backed by santhosh-tekuri/jsonschema.
// Schemas are pre-compiled on Register for fast repeated validation.
type JSONSchema struct {
	mu       sync.RWMutex
	versions map[string]int                        // topic → latest version number
	schemas  map[string]map[int]*jsonschema.Schema // topic → version → compiled schema
	raw      map[string]map[int][]byte             // topic → version → raw schema bytes
}

// NewJSONSchema returns an empty JSONSchema registry.
func NewJSONSchema() *JSONSchema {
	return &JSONSchema{
		versions: map[string]int{},
		schemas:  map[string]map[int]*jsonschema.Schema{},
		raw:      map[string]map[int][]byte{},
	}
}

// ValidateDefinition compiles schemaBytes without registering it.
func (r *JSONSchema) ValidateDefinition(_ context.Context, topic string, schemaBytes []byte) error {
	_, err := compileSchema(topic, 0, schemaBytes)
	return err
}

// Register compiles the JSON Schema bytes and stores it under topic. It
// returns the auto-assigned version number (monotonically increasing per
// topic, starting at 1).
//
// If a previous version exists, Register checks backwards compatibility:
// new schemas must accept every document accepted by the previous version.
// Specifically: no property removals, no type changes on existing
// properties, and the old required set must still be valid under the new
// schema.
func (r *JSONSchema) Register(_ context.Context, topic string, schemaBytes []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if prevVersion := r.versions[topic]; prevVersion > 0 {
		prevRaw := r.raw[topic][prevVersion]
		if err := checkCompatible(prevRaw, schemaBytes); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrIncompatible, err)
		}
	}

	version := r.versions[topic] + 1
	if err := r.loadLocked(topic, version, schemaBytes); err != nil {
		return 0, err
	}
	return version, nil
}

// Load compiles and stores a persisted schema version for startup rehydration.
func (r *JSONSchema) Load(_ context.Context, topic string, version int, schemaBytes []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadLocked(topic, version, schemaBytes)
}

// Unload removes a compiled schema version from the registry.
func (r *JSONSchema) Unload(_ context.Context, topic string, version int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.schemas[topic] == nil {
		return nil
	}
	delete(r.schemas[topic], version)
	delete(r.raw[topic], version)
	if r.versions[topic] != version {
		return nil
	}
	latest := 0
	for candidate := range r.schemas[topic] {
		if candidate > latest {
			latest = candidate
		}
	}
	if latest == 0 {
		delete(r.schemas, topic)
		delete(r.raw, topic)
		delete(r.versions, topic)
		return nil
	}
	r.versions[topic] = latest
	return nil
}

func (r *JSONSchema) loadLocked(topic string, version int, schemaBytes []byte) error {
	compiled, err := compileSchema(topic, version, schemaBytes)
	if err != nil {
		return err
	}
	if r.schemas[topic] == nil {
		r.schemas[topic] = map[int]*jsonschema.Schema{}
		r.raw[topic] = map[int][]byte{}
	}
	copied := make([]byte, len(schemaBytes))
	copy(copied, schemaBytes)
	if version > r.versions[topic] {
		r.versions[topic] = version
	}
	r.schemas[topic][version] = compiled
	r.raw[topic][version] = copied
	return nil
}

func compileSchema(topic string, version int, schemaBytes []byte) (*jsonschema.Schema, error) {
	schemaDoc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("schema: invalid JSON: %w", err)
	}

	c := jsonschema.NewCompiler()
	resource := fmt.Sprintf("%s-%d.json", topic, version)
	if err := c.AddResource(resource, schemaDoc); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	compiled, err := c.Compile(resource)
	if err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	return compiled, nil
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

// checkCompatible validates that newSchema accepts every document
// accepted by oldSchema. It parses both as raw JSON to compare
// properties, required fields, and types.
func checkCompatible(oldRaw, newRaw []byte) error {
	oldSchema, err := parseSchemaShape(oldRaw)
	if err != nil {
		return err
	}
	newSchema, err := parseSchemaShape(newRaw)
	if err != nil {
		return err
	}

	oldRequired := setOf(oldSchema.Required)
	newRequired := setOf(newSchema.Required)

	for name, oldProp := range oldSchema.Properties {
		newProp, ok := newSchema.Properties[name]
		if !ok {
			return fmt.Errorf("property %q removed", name)
		}
		if err := compatibleTypes(oldProp, newProp); err != nil {
			return fmt.Errorf("property %q: %w", name, err)
		}
	}

	// Previously-required fields must still exist and be compatible.
	for name := range oldRequired {
		if _, ok := newSchema.Properties[name]; !ok {
			return fmt.Errorf("required property %q removed", name)
		}
	}
	for name := range newRequired {
		if !oldRequired[name] {
			return fmt.Errorf("required property %q added", name)
		}
	}

	return nil
}

type schemaShape struct {
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
}

func parseSchemaShape(raw []byte) (schemaShape, error) {
	var s schemaShape
	if err := json.Unmarshal(raw, &s); err != nil {
		return schemaShape{}, err
	}
	return s, nil
}

type propShape struct {
	Type any `json:"type"` // string or []string
}

func compatibleTypes(oldProp, newProp json.RawMessage) error {
	var oldPS, newPS propShape
	if err := json.Unmarshal(oldProp, &oldPS); err != nil {
		return err
	}
	if err := json.Unmarshal(newProp, &newPS); err != nil {
		return err
	}

	oldTypes := normalizeType(oldPS.Type)
	newTypes := normalizeType(newPS.Type)
	if len(oldTypes) == 0 || len(newTypes) == 0 {
		return nil
	}

	newSet := setOf(newTypes)
	for _, oldType := range oldTypes {
		if !newSet[oldType] {
			return fmt.Errorf("type changed from %v to %v", oldTypes, newTypes)
		}
	}
	return nil
}

// normalizeType converts the JSON Schema "type" field to a []string.
// JSON Schema allows type to be a single string or an array.
func normalizeType(t any) []string {
	switch v := t.(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func setOf(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
