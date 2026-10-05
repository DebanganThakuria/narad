package metastore

// JSON value equality for the attach-time schema comparison. Schemas
// were registered as the client's raw bytes, whitespace and key order
// included, while every read path serves them compacted, so a child
// whose history was copied back through the API could never be
// re-attached to its parent: opAttachChild compares the histories byte
// for byte. opAttachChildIf and opCreateTopicWith compare them by JSON
// value instead, the notion of "the same schema" the rest of Narad uses.
//
// The state machine runs this on every replica, so it is self-contained
// and must never change meaning: object key order is ignored, a
// duplicate key keeps its last value (as encoding/json does), and
// numbers compare by exact decimal value, never through float64 (which
// rounds) or big.Rat (whose exponent handling would let a hostile
// "1e999999999" allocate without bound).

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// jsonValueEqual reports whether a and b hold the same JSON value. When
// either does not parse as one JSON value they are equal only if their
// bytes are.
func jsonValueEqual(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	va, ok := decodeJSONValue(a)
	if !ok {
		return false
	}
	vb, ok := decodeJSONValue(b)
	if !ok {
		return false
	}
	return jsonValuesEqual(va, vb)
}

func decodeJSONValue(raw []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, false
	}
	return v, true
}

func jsonValuesEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, ok := bv[k]
			if !ok || !jsonValuesEqual(x, y) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonValuesEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case json.Number:
		bv, ok := b.(json.Number)
		return ok && jsonNumbersEqual(string(av), string(bv))
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case nil:
		return b == nil
	}
	return false
}

// jsonNumbersEqual compares two JSON number literals by exact value.
func jsonNumbersEqual(a, b string) bool {
	if a == b {
		return true
	}
	na, ok := canonicalJSONNumber(a)
	if !ok {
		return false
	}
	nb, ok := canonicalJSONNumber(b)
	return ok && na == nb
}

// canonicalJSONNumber renders a JSON number literal as its sign, its
// significant digits and an exponent ("-15e-1" for -1.50), so equal
// values render alike. It refuses an exponent that does not fit in an
// int32, which no schema needs.
func canonicalJSONNumber(lit string) (string, bool) {
	neg := strings.HasPrefix(lit, "-")
	lit = strings.TrimPrefix(lit, "-")
	mantissa, expPart, hasExp := strings.Cut(strings.ToLower(lit), "e")
	exp := int64(0)
	if hasExp {
		e, err := strconv.ParseInt(expPart, 10, 32)
		if err != nil {
			return "", false
		}
		exp = e
	}
	intPart, fracPart, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(intPart+fracPart, "0")
	exp -= int64(len(fracPart))
	if digits == "" {
		return "0", true
	}
	trimmed := strings.TrimRight(digits, "0")
	exp += int64(len(digits) - len(trimmed))
	sign := ""
	if neg {
		sign = "-"
	}
	return sign + trimmed + "e" + strconv.FormatInt(exp, 10), true
}

// schemaHistoriesValueEqual compares two schema histories version by
// version, by JSON value.
func schemaHistoriesValueEqual(a, b map[int][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for version, schema := range a {
		other, ok := b[version]
		if !ok || !jsonValueEqual(schema, other) {
			return false
		}
	}
	return true
}
