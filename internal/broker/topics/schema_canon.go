package topics

import (
	"bytes"
	"encoding/json"

	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// canonicalSchema returns the schema with insignificant whitespace
// removed (audit schemas:6). Registration used to store a request's raw
// bytes, so a client's indentation became part of the replicated
// history, and of every fan-out child's copy, while every read path
// hands schemas back compacted. New versions are stored compacted;
// histories written before stay as they are. A body that is not JSON is
// returned unchanged for the definition check to report.
func canonicalSchema(raw []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return raw
	}
	return buf.Bytes()
}

// annotationKeywords document a schema and never change what it
// accepts.
var annotationKeywords = map[string]bool{
	"title": true, "description": true, "examples": true, "$comment": true,
	"default": true, "deprecated": true, "readOnly": true, "writeOnly": true,
}

// Keywords whose value is a schema, a list of schemas, or a map of
// schemas: the positions stripAnnotations descends into. Anything else
// is compared as it stands, so a change under an unknown keyword (or a
// property named "description") always makes a new version.
var (
	schemaValuedKeywords = map[string]bool{
		"not": true, "if": true, "then": true, "else": true,
		"additionalProperties": true, "additionalItems": true, "contains": true,
		"propertyNames": true, "unevaluatedItems": true, "unevaluatedProperties": true,
		"contentSchema": true, "items": true,
	}
	schemaListKeywords = map[string]bool{
		"allOf": true, "anyOf": true, "oneOf": true, "prefixItems": true, "items": true,
	}
	schemaMapKeywords = map[string]bool{
		"properties": true, "patternProperties": true, "$defs": true,
		"definitions": true, "dependentSchemas": true, "dependencies": true,
	}
)

// annotationOnlyChange reports whether next differs from prev only in
// annotation keywords at schema positions (audit H14). Such a change
// accepts exactly what prev accepts, and an automated PATCH loop that
// only reworded a description appended a full version, copied into
// every child, each time; it now registers nothing.
func annotationOnlyChange(prev, next []byte) bool {
	a, ok := decodeSchemaValue(prev)
	if !ok {
		return false
	}
	b, ok := decodeSchemaValue(next)
	if !ok {
		return false
	}
	sa, err := json.Marshal(stripAnnotations(a))
	if err != nil {
		return false
	}
	sb, err := json.Marshal(stripAnnotations(b))
	if err != nil {
		return false
	}
	return schema.Equal(sa, sb)
}

func decodeSchemaValue(raw []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}
	return v, true
}

// stripAnnotations returns a copy of the schema value v without the
// annotation keywords at any schema position.
func stripAnnotations(v any) any {
	obj, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := make(map[string]any, len(obj))
	for k, val := range obj {
		if annotationKeywords[k] {
			continue
		}
		switch {
		case schemaMapKeywords[k]:
			if m, ok := val.(map[string]any); ok {
				mm := make(map[string]any, len(m))
				for name, sub := range m {
					mm[name] = stripAnnotations(sub)
				}
				val = mm
			}
		case schemaListKeywords[k]:
			if list, ok := val.([]any); ok {
				ll := make([]any, len(list))
				for i, sub := range list {
					ll[i] = stripAnnotations(sub)
				}
				val = ll
			} else if schemaValuedKeywords[k] {
				val = stripAnnotations(val)
			}
		case schemaValuedKeywords[k]:
			val = stripAnnotations(val)
		}
		out[k] = val
	}
	return out
}
