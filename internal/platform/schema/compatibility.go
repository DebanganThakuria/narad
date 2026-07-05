package schema

import (
	"encoding/json"
	"fmt"
)

// checkCompatible validates that the new schema accepts every document
// accepted by the old one. It compares the raw JSON shapes directly —
// properties, required fields, and types — rather than reasoning about
// full JSON Schema semantics.
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
	for name := range newRequired {
		if !oldRequired[name] {
			return fmt.Errorf("required property %q added", name)
		}
	}

	return nil
}

// schemaShape is the subset of a JSON Schema document the compatibility
// check inspects.
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

// compatibleTypes rejects a property whose new type set no longer covers
// its old one; widening a union is fine, narrowing or changing is not.
// A missing "type" on either side is treated as unconstrained.
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

// normalizeType converts a JSON Schema "type" field, which may be a
// single string or an array of strings, to a []string.
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
