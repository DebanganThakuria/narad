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

	if prevVersion := r.versions[topic]; prevVersion > 0 {
		prevRaw := r.raw[topic][prevVersion]
		if err := checkCompatible(prevRaw, schemaBytes); err != nil {
			return 0, fmt.Errorf("%w: %w", ErrIncompatible, err)
		}
	}

	version := r.versions[topic] + 1
	r.versions[topic] = version
	if r.schemas[topic] == nil {
		r.schemas[topic] = map[int]*jsonschema.Schema{}
		r.raw[topic] = map[int][]byte{}
	}
	r.schemas[topic][version] = compiled
	r.raw[topic][version] = make([]byte, len(schemaBytes))
	copy(r.raw[topic][version], schemaBytes)

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

	for name := range oldSchema.Properties {
		newProp, ok := newSchema.Properties[name]
		if !ok {
			return fmt.Errorf("property %q removed", name)
		}

		oldProp := oldSchema.Properties[name]
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

	oldSet := setOf(oldTypes)
	for _, nt := range newTypes {
		if oldSet[nt] {
			return nil // at least one old type overlaps
		}
	}
	return fmt.Errorf("type changed from %v to %v", oldTypes, newTypes)
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