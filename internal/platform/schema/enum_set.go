package schema

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// enumSetMinValues is the enum size from which membership is a hash
// lookup instead of the library's scan. The scan compares numbers by
// parsing both sides into a big.Rat, so a 45k-value allowlist of IDs
// cost 14.8 ms per payload item; a handful of values scans faster than
// a lookup builds its key.
const enumSetMinValues = 16

// enumSet checks enum membership with the library's equality (numbers
// by exact value, objects regardless of key order) through hash
// lookups. It replaces a compiled schema's Enum as an extension, which
// the validator runs after the schema's other keywords: the outcome is
// the same conjunction, only the order of reported errors differs.
type enumSet struct {
	values []any               // the enum as written, for the error
	strs   map[string]struct{} // string members
	lits   map[string]struct{} // numeric members, by literal text
	keys   map[string]struct{} // every non-string member, by canonical form
}

func newEnumSet(values []any) *enumSet {
	e := &enumSet{
		values: values,
		strs:   map[string]struct{}{},
		lits:   map[string]struct{}{},
		keys:   map[string]struct{}{},
	}
	for _, v := range values {
		if s, ok := v.(string); ok {
			e.strs[s] = struct{}{}
			continue
		}
		key, ok := enumKey(v)
		if !ok {
			// A number big.Rat cannot read equals nothing in the
			// library either.
			continue
		}
		e.keys[key] = struct{}{}
		if n, ok := v.(json.Number); ok {
			e.lits[string(n)] = struct{}{}
		}
	}
	return e
}

func (e *enumSet) contains(v any) bool {
	switch t := v.(type) {
	case string:
		_, ok := e.strs[t]
		return ok
	case json.Number:
		// The same literal is the same number (its parse succeeded
		// when the member was indexed).
		if _, ok := e.lits[string(t)]; ok {
			return true
		}
	}
	key, ok := enumKey(v)
	if !ok {
		return false
	}
	_, ok = e.keys[key]
	return ok
}

// Validate implements jsonschema.SchemaExt.
func (e *enumSet) Validate(ctx *jsonschema.ValidatorContext, v any) {
	if !e.contains(v) {
		ctx.AddError(&kind.Enum{Got: v, Want: e.values})
	}
}

// enumKey renders v so that two values are equal to the library
// exactly when their keys are: object members sorted by name, numbers
// as exact rationals (1, 1.0 and 1e0 alike). ok is false when v holds
// a number big.Rat cannot read, which the library's equality never
// matches.
func enumKey(v any) (string, bool) {
	var b strings.Builder
	if !writeEnumKey(&b, v) {
		return "", false
	}
	return b.String(), true
}

func writeEnumKey(b *strings.Builder, v any) bool {
	switch t := v.(type) {
	case map[string]any:
		names := make([]string, 0, len(t))
		for k := range t {
			names = append(names, k)
		}
		slices.Sort(names)
		b.WriteByte('{')
		for i, k := range names {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(k))
			b.WriteByte(':')
			if !writeEnumKey(b, t[k]) {
				return false
			}
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, x := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			if !writeEnumKey(b, x) {
				return false
			}
		}
		b.WriteByte(']')
	case string:
		b.WriteString(strconv.Quote(t))
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(t))
	default:
		r, ok := ratOf(v)
		if !ok {
			return false
		}
		b.WriteByte('#')
		b.WriteString(r.RatString())
	}
	return true
}

// useEnumSet replaces s's linear enum check with an enumSet when the
// enum has at least enumSetMinValues values. Before draft 2019-09 the
// validator returns at $ref without running extensions, while it checks
// enum before $ref; the library compiles no sibling of such a $ref
// today, but an enum there is left where it is so the check could never
// be dropped (TestEnumSetAgreesWithTheLibrary pins the behaviour).
func useEnumSet(s *jsonschema.Schema) {
	if s.Enum != nil && len(s.Enum.Values) >= enumSetMinValues && (s.Ref == nil || s.DraftVersion >= 2019) {
		s.Extensions = append(s.Extensions, newEnumSet(s.Enum.Values))
		s.Enum = nil
	}
}
