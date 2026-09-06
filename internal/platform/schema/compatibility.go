package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// checkCompatible reports an error unless every document accepted by
// prevRaw is also accepted by nextRaw. It is a structural subsumption
// check that FAILS CLOSED: a keyword it cannot reason about is only
// allowed to stay byte-identical or (where dropping is a pure widening)
// disappear; anything else is rejected with a message naming the
// keyword and its location.
//
// Soundness argument. A schema is a conjunction of keyword constraints.
// If every constraint in the new schema is implied by the constraint
// the same keyword carried in the previous schema, the new conjunction
// is implied by the old one, so every previously valid document stays
// valid. Each keyword below is checked against its own predecessor
// only; implications across keywords (old "const": 5 implying a new
// "minimum": 3) are not derived, so such updates are rejected even
// though they would be safe. That is the intended direction of error.
// Three places need more than keyword-by-keyword comparison and are
// handled explicitly: enum and const on one node intersect; a new
// property name that matches a previous patternProperties pattern was
// constrained by that pattern's schema, not by additionalProperties;
// and "identical" for a keyword the check does not model follows
// $ref into $defs, since the referenced body can change while the
// keyword's text does not.
//
// Numbers are compared exactly (big.Rat), matching the validator: a
// float64 comparison would call minimum 2^53 and 2^53+1 equal and
// accept a tightening.
//
// Recursion. Subschemas reached through $ref can form cycles. A pair
// of subschemas already being compared further up the stack is
// assumed compatible (the usual coinductive rule for recursive
// subtyping: sound here because the validator only accepts a document
// through a finite derivation, and every step of that derivation
// re-enters the pair with a smaller instance). Results are memoised
// per pair so branching combinators cannot make the check exponential,
// and a global step budget fails closed on anything that still grows.
//
// Allowlist (the keywords the check understands):
//
//	type                   new set must include every old type; integer may widen to number
//	enum, const            old value set must be a subset of the new one; may be dropped
//	minimum, exclusiveMinimum, maximum, exclusiveMaximum
//	                       bounds may only loosen or be dropped, never appear
//	multipleOf             new value must divide the old (exactly); may be dropped
//	minLength, minItems, minProperties, minContains
//	                       may only decrease or be dropped; may appear only as 0
//	maxLength, maxItems, maxProperties, maxContains
//	                       may only increase or be dropped; may not appear
//	pattern, format        identical or dropped
//	uniqueItems            may go true -> false or be dropped; may not become true
//	required               new set must be a subset of the old
//	properties             no property removed; existing ones checked recursively;
//	                       a NEW property is allowed (see the exception below) unless
//	                       its name matched a previous patternProperties pattern, in
//	                       which case it must be wider than that pattern's schema
//	additionalProperties   false may open up; a schema may only widen; a closed
//	                       model may not appear, nor a schema where none was
//	items                  checked recursively; may not appear; tuple form rejected
//	anyOf                  every old branch must be covered by some new branch
//	allOf                  every new branch must be implied by some old branch
//	$ref                   only "#/..." pointers into the same document, with no
//	                       sibling keywords (annotations, $schema and a root $id
//	                       aside); resolved on both sides before comparing; a chain
//	                       of nothing but $ref that loops accepts nothing
//	$schema                must not change
//
// Droppable-only keywords (dropping is a pure widening; any other change
// is rejected): oneOf, not, if, then, else, contains, propertyNames,
// dependentRequired, dependentSchemas, dependencies, patternProperties
// (only when the new schema does not also constrain additional
// properties), prefixItems (only when the new schema has no items).
//
// Annotations ignored entirely: title, description, default, examples,
// $comment, deprecated, readOnly, writeOnly, $defs, definitions,
// $anchor, and $id at the root.
//
// Everything else (unevaluatedProperties, unevaluatedItems, $dynamicRef,
// nested $id, draft-04 boolean exclusive bounds, tuple-form items, ...)
// must be absent from the new schema, or the whole subschema it sits in
// must be byte-for-byte identical to the previous one.
//
// The one deliberate gap: adding a typed optional property under an
// open content model (no additionalProperties, or additionalProperties:
// true). Strictly, a document that already carried that key with a
// value of another type was valid before and is not any more. Every
// user of the registry relies on "add an optional field" being the
// normal evolution, so it is allowed and documented; use
// additionalProperties: false (a closed model) when exact semantics
// matter.
func checkCompatible(prevRaw, nextRaw []byte) error {
	prev, err := jsonschema.UnmarshalJSON(bytes.NewReader(prevRaw))
	if err != nil {
		return fmt.Errorf("previous schema is not valid JSON: %w", err)
	}
	next, err := jsonschema.UnmarshalJSON(bytes.NewReader(nextRaw))
	if err != nil {
		return fmt.Errorf("new schema is not valid JSON: %w", err)
	}
	c := newCompatChecker(prev, next)
	return c.check(prev, next, "", 0)
}

// maxCompatDepth bounds genuine nesting (through $ref chains into
// $defs as well as inline subschemas). Cycles never reach it: a pair
// already on the stack is assumed compatible instead.
const maxCompatDepth = 64

// maxCompatSteps bounds the number of subschema comparisons one check
// may make. Memoisation keeps ordinary schemas far below it; it is the
// fail-closed backstop for anything that still grows.
const maxCompatSteps = 100_000

var (
	annotationKeywords = map[string]bool{
		"title": true, "description": true, "default": true, "examples": true,
		"$comment": true, "deprecated": true, "readOnly": true, "writeOnly": true,
		"$defs": true, "definitions": true, "$anchor": true,
	}
	// droppableKeywords are opaque to the check but removing them can
	// only widen the accepted set (each one, on its own, only ever
	// rejects documents).
	droppableKeywords = map[string]bool{
		"oneOf": true, "not": true, "if": true, "then": true, "else": true,
		"contains": true, "propertyNames": true, "dependentRequired": true,
		"dependentSchemas": true, "dependencies": true,
	}
	// unsupportedInNext may not appear in a new schema at all unless the
	// subschema is identical: their meaning depends on sibling keywords
	// the check may have allowed to change.
	unsupportedInNext = map[string]bool{
		"unevaluatedProperties": true, "unevaluatedItems": true,
		"$dynamicRef": true, "$dynamicAnchor": true, "$recursiveRef": true, "$recursiveAnchor": true,
	}
)

const defaultDraftURL = "https://json-schema.org/draft/2020-12/schema"

// pairKey identifies one (previous subschema, new subschema) pair by
// the identity of the two decoded objects.
type pairKey struct{ prev, next uintptr }

type compatChecker struct {
	prevRoot any
	nextRoot any

	// inProgress holds the pairs on the current comparison stack;
	// passed and failed memoise finished pairs. assumptions logs every
	// use of an in-progress pair so a success that leaned on one is
	// only memoised once that pair itself has passed.
	inProgress  map[pairKey]bool
	passed      map[pairKey]bool
	failed      map[pairKey]error
	assumptions []pairKey
	steps       int
	fatal       error

	// ignored holds the keywords the validator does not apply under
	// the schemas' draft (const before draft-06, if/then/else before
	// draft-07, ...). They are annotations to it and must be
	// annotations here too: reading a draft-04 "const" as a constraint
	// made {const: 1} look narrower than {enum: [1, 2]}, and the check
	// accepted an update that turned an ignored keyword into an
	// enforced one. Taken from the previous schema; a draft change is
	// rejected anyway.
	ignored map[string]bool
}

func newCompatChecker(prevRoot, nextRoot any) *compatChecker {
	var draft string
	if root, ok := prevRoot.(map[string]any); ok {
		draft = draftOf(root["$schema"])
	} else {
		draft = defaultDraftURL
	}
	return &compatChecker{
		prevRoot:   prevRoot,
		nextRoot:   nextRoot,
		inProgress: map[pairKey]bool{},
		passed:     map[pairKey]bool{},
		failed:     map[pairKey]error{},
		ignored:    ignoredByDraft(draft),
	}
}

// ignoredByDraft lists the keywords the validator leaves unapplied
// under a draft, mirroring the library's per-draft compilation.
func ignoredByDraft(draft string) map[string]bool {
	out := map[string]bool{}
	add := func(keys ...string) {
		for _, k := range keys {
			out[k] = true
		}
	}
	switch draft {
	case "http://json-schema.org/draft-04/schema":
		add("const", "contains", "propertyNames")
		fallthrough
	case "http://json-schema.org/draft-06/schema":
		add("if", "then", "else")
		fallthrough
	case "http://json-schema.org/draft-07/schema":
		add("minContains", "maxContains", "dependentRequired", "dependentSchemas",
			"unevaluatedItems", "unevaluatedProperties", "$recursiveRef", "$recursiveAnchor")
		fallthrough
	case "https://json-schema.org/draft/2019-09/schema":
		add("prefixItems", "$dynamicRef", "$dynamicAnchor")
	default:
		add("additionalItems")
	}
	return out
}

func at(path string) string {
	if path == "" {
		return "schema root"
	}
	return "at " + path
}

// mapID returns the identity of a decoded JSON object.
func mapID(v any) (uintptr, bool) {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return 0, false
	}
	return reflect.ValueOf(m).Pointer(), true
}

func isRootObject(v, root any) bool {
	a, ok := mapID(v)
	if !ok {
		return false
	}
	b, ok := mapID(root)
	return ok && a == b
}

// check compares one subschema of the previous schema with the
// corresponding subschema of the new one. It resolves $ref on both
// sides, then consults the memo tables before doing the comparison.
func (c *compatChecker) check(prev, next any, path string, depth int) error {
	if c.fatal != nil {
		return c.fatal
	}
	c.steps++
	if c.steps > maxCompatSteps {
		c.fatal = fmt.Errorf("%s: the compatibility check gave up after %d subschema comparisons; simplify the schema", at(path), maxCompatSteps)
		return c.fatal
	}
	if depth > maxCompatDepth {
		c.fatal = fmt.Errorf("%s: schema nests deeper than %d levels", at(path), maxCompatDepth)
		return c.fatal
	}
	prev, err := c.resolve(prev, c.prevRoot, path)
	if err != nil {
		return fmt.Errorf("previous version %w", err)
	}
	next, err = c.resolve(next, c.nextRoot, path)
	if err != nil {
		return fmt.Errorf("new version %w", err)
	}

	pid, pok := mapID(prev)
	nid, nok := mapID(next)
	if !pok || !nok {
		return c.checkNode(prev, next, path, depth)
	}
	key := pairKey{pid, nid}
	if c.inProgress[key] {
		c.assumptions = append(c.assumptions, key)
		return nil
	}
	if err, ok := c.failed[key]; ok {
		return err
	}
	if c.passed[key] {
		return nil
	}
	c.inProgress[key] = true
	mark := len(c.assumptions)
	err = c.checkNode(prev, next, path, depth)
	delete(c.inProgress, key)
	if err != nil {
		// A failure reached under optimistic assumptions is a failure
		// without them too; the assumptions it made no longer matter.
		c.assumptions = c.assumptions[:mark]
		if c.fatal == nil {
			c.failed[key] = err
		}
		return err
	}
	// Assumptions about this very pair are now discharged. Anything
	// else the subtree assumed belongs to a frame further up, and this
	// success stays conditional on it (so it is not memoised).
	kept := c.assumptions[:mark]
	for _, a := range c.assumptions[mark:] {
		if a != key {
			kept = append(kept, a)
		}
	}
	c.assumptions = kept
	if len(kept) == mark {
		c.passed[key] = true
	}
	return nil
}

// checkNode is the keyword-by-keyword comparison of two resolved
// subschemas.
func (c *compatChecker) checkNode(prev, next any, path string, depth int) error {
	prevObj, prevOK := schemaObject(prev)
	nextObj, nextOK := schemaObject(next)
	if prevOK && prevObj == nil {
		// The previous schema accepted nothing here; anything is wider.
		return nil
	}
	if nextOK && nextObj == nil {
		return fmt.Errorf("%s: new schema is false (accepts nothing) where the previous version accepted documents", at(path))
	}
	if !prevOK {
		return fmt.Errorf("%s: previous schema is neither an object nor a boolean", at(path))
	}
	if !nextOK {
		return fmt.Errorf("%s: new schema is neither an object nor a boolean", at(path))
	}

	for k := range nextObj {
		if unsupportedInNext[k] && !c.ignored[k] {
			if c.equalResolved(prevObj, nextObj) {
				return nil
			}
			return fmt.Errorf("%s: %q is outside the compatibility check's vocabulary; a subschema using it may only be kept identical (including anything it references)", at(path), k)
		}
	}
	// A nested subschema that has become true (or carries nothing but
	// annotations) accepts every value at its position, so it is wider
	// than whatever was there; the keyword-by-keyword walk would call a
	// vanished "properties" a removal. The root is not shortcut: the
	// documented rule is that {} is not a widening of the whole schema.
	if path != "" && !c.constrains(nextObj) {
		return nil
	}
	nestedID := !isRootObject(prevObj, c.prevRoot) || !isRootObject(nextObj, c.nextRoot)

	if err := c.checkNumericBounds(prevObj, nextObj, path); err != nil {
		return err
	}
	if err := c.checkEnumConst(prevObj, nextObj, path); err != nil {
		return err
	}

	keys := make([]string, 0, len(prevObj)+len(nextObj))
	for k := range prevObj {
		keys = append(keys, k)
	}
	for k := range nextObj {
		if _, dup := prevObj[k]; !dup {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	for _, k := range keys {
		if c.ignored[k] {
			continue
		}
		pv, pok := prevObj[k]
		nv, nok := nextObj[k]
		var err error
		switch k {
		case "minimum", "exclusiveMinimum", "maximum", "exclusiveMaximum", "enum", "const":
			// handled above as groups
		case "$schema":
			if draftOf(pv) != draftOf(nv) {
				err = fmt.Errorf("%s: $schema changed from %q to %q; the draft must stay the same", at(path), draftOf(pv), draftOf(nv))
			}
		case "$id":
			if nestedID {
				err = fmt.Errorf("%s: nested $id is not supported by the compatibility check", at(path))
			}
		case "type":
			err = checkType(pv, nv, path)
		case "multipleOf":
			err = checkMultipleOf(pv, nv, path)
		case "minLength", "minItems", "minProperties":
			err = checkLengthMin(k, pv, pok, nv, nok, path, 0)
		case "minContains":
			err = checkLengthMin(k, pv, pok, nv, nok, path, 1)
		case "maxLength", "maxItems", "maxProperties", "maxContains":
			err = checkLengthMax(k, pv, pok, nv, nok, path)
		case "pattern", "format":
			if nok && !(pok && jsonEqual(pv, nv)) {
				err = fmt.Errorf("%s: %s %s; it may only stay identical or be dropped", at(path), k, addedOrChanged(pok))
			}
		case "uniqueItems":
			if nv == true && pv != true {
				err = fmt.Errorf("%s: uniqueItems: true added", at(path))
			}
		case "required":
			err = checkRequired(pv, nv, path)
		case "properties":
			err = c.checkProperties(prevObj, nextObj, path, depth)
		case "additionalProperties":
			err = c.checkAdditionalProperties(pv, pok, nv, nok, path, depth)
		case "items":
			err = c.checkItems(pv, pok, nv, nok, path, depth)
		case "anyOf":
			err = c.checkAnyOf(pv, pok, nv, nok, path, depth)
		case "allOf":
			err = c.checkAllOf(pv, pok, nv, nok, path, depth)
		case "patternProperties":
			err = c.checkOpaque(k, pv, pok, nv, nok, path, !constrainsAdditional(nextObj))
		case "prefixItems":
			_, nextHasItems := nextObj["items"]
			err = c.checkOpaque(k, pv, pok, nv, nok, path, !nextHasItems)
		default:
			if annotationKeywords[k] {
				continue
			}
			err = c.checkOpaque(k, pv, pok, nv, nok, path, droppableKeywords[k])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// resolve follows a "$ref" (only "#/..." JSON pointers into the same
// document, with no sibling constraint keywords) until it reaches a
// subschema without one. A chain of nothing but $ref that loops back
// on itself is the validator's "infinite loop" case, which accepts no
// document at all, so it resolves to false.
func (c *compatChecker) resolve(v any, root any, path string) (any, error) {
	seen := map[uintptr]bool{}
	for hop := 0; hop <= maxCompatDepth; hop++ {
		obj, ok := v.(map[string]any)
		if !ok {
			return v, nil
		}
		ref, ok := obj["$ref"]
		if !ok {
			return v, nil
		}
		if id, ok := mapID(obj); ok {
			if seen[id] {
				return false, nil
			}
			seen[id] = true
		}
		for k := range obj {
			if k == "$ref" || annotationKeywords[k] || k == "$schema" || (k == "$id" && isRootObject(obj, root)) {
				continue
			}
			return nil, fmt.Errorf("%s: $ref alongside %q is not supported by the compatibility check", at(path), k)
		}
		refStr, ok := ref.(string)
		if !ok {
			return nil, fmt.Errorf("%s: $ref must be a string", at(path))
		}
		target, err := resolvePointer(root, refStr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", at(path), err)
		}
		v = target
	}
	return nil, fmt.Errorf("%s: $ref chain longer than %d hops (cycle?)", at(path), maxCompatDepth)
}

// resolvePointer resolves an in-document JSON pointer reference
// ("#", "#/$defs/name", "#/properties/a/items").
func resolvePointer(root any, ref string) (any, error) {
	if ref == "#" {
		return root, nil
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("$ref %q is not supported; only JSON-pointer references into the same document (\"#/$defs/name\") are", ref)
	}
	frag, err := url.PathUnescape(ref[2:])
	if err != nil {
		return nil, fmt.Errorf("$ref %q: %w", ref, err)
	}
	cur := root
	for tok := range strings.SplitSeq(frag, "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[tok]
			if !ok {
				return nil, fmt.Errorf("$ref %q does not resolve", ref)
			}
			cur = next
		case []any:
			idx, err := strconv.Atoi(tok)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, fmt.Errorf("$ref %q does not resolve", ref)
			}
			cur = node[idx]
		default:
			return nil, fmt.Errorf("$ref %q does not resolve", ref)
		}
	}
	return cur, nil
}

// schemaObject normalizes a subschema: true and {} become an empty
// object, false becomes a nil map, anything else reports !ok.
func schemaObject(v any) (map[string]any, bool) {
	switch s := v.(type) {
	case map[string]any:
		return s, true
	case bool:
		if s {
			return map[string]any{}, true
		}
		return nil, true
	default:
		return nil, false
	}
}

func draftOf(v any) string {
	s, _ := v.(string)
	if s == "" {
		return defaultDraftURL
	}
	return strings.TrimSuffix(s, "#")
}

func addedOrChanged(prevPresent bool) string {
	if prevPresent {
		return "changed"
	}
	return "added"
}

// checkOpaque is the rule for keywords the check does not model: keep
// identical (following $ref, see equalResolved), or drop when the
// caller knows dropping only widens.
func (c *compatChecker) checkOpaque(k string, pv any, pok bool, nv any, nok bool, path string, droppable bool) error {
	switch {
	case !nok && !pok:
		return nil
	case !nok:
		if droppable {
			return nil
		}
		return fmt.Errorf("%s: %q removed; the compatibility check cannot prove that widens the schema here, so it must stay identical", at(path), k)
	case pok && c.equalResolved(pv, nv):
		return nil
	default:
		return fmt.Errorf("%s: %q %s; it is outside the compatibility check's vocabulary and may only stay identical (including anything it references)%s", at(path), k, addedOrChanged(pok), droppableSuffix(droppable))
	}
}

// equalResolved is jsonEqual for schema-valued keywords the check does
// not model: the same text is not enough when it contains a $ref,
// because the $defs body it points at can change while the keyword
// stays byte-identical. Every "$ref" (and dynamic/recursive variant)
// is resolved in its own document and the targets compared too; a
// reference that cannot be resolved as an in-document pointer compares
// unequal, which fails closed.
func (c *compatChecker) equalResolved(a, b any) bool {
	return c.equalResolvedAt(a, b, map[pairKey]bool{}, 0)
}

func (c *compatChecker) equalResolvedAt(a, b any, seen map[pairKey]bool, depth int) bool {
	if depth > maxCompatDepth {
		return false
	}
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		if ida, ok := mapID(av); ok {
			if idb, ok := mapID(bv); ok {
				key := pairKey{ida, idb}
				if seen[key] {
					return true
				}
				seen[key] = true
			}
		}
		for k, x := range av {
			y, ok := bv[k]
			if !ok || !c.equalResolvedAt(x, y, seen, depth+1) {
				return false
			}
		}
		for _, k := range []string{"$ref", "$dynamicRef", "$recursiveRef"} {
			ref, ok := av[k].(string)
			if !ok {
				continue
			}
			ta, err := resolvePointer(c.prevRoot, ref)
			if err != nil {
				return false
			}
			tb, err := resolvePointer(c.nextRoot, ref)
			if err != nil {
				return false
			}
			if !c.equalResolvedAt(ta, tb, seen, depth+1) {
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
			if !c.equalResolvedAt(av[i], bv[i], seen, depth+1) {
				return false
			}
		}
		return true
	default:
		return jsonEqual(a, b)
	}
}

func droppableSuffix(droppable bool) string {
	if droppable {
		return " or be removed"
	}
	return ""
}

// constrains reports whether obj carries any keyword other than an
// annotation, $schema, $id or one the draft ignores, i.e. whether it
// can reject a value.
func (c *compatChecker) constrains(obj map[string]any) bool {
	for k := range obj {
		if !annotationKeywords[k] && k != "$schema" && k != "$id" && !c.ignored[k] {
			return true
		}
	}
	return false
}

func constrainsAdditional(obj map[string]any) bool {
	ap, ok := obj["additionalProperties"]
	if !ok {
		return false
	}
	return ap != true
}

func typeSet(v any, path string) (map[string]bool, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return map[string]bool{t: true}, nil
	case []any:
		out := make(map[string]bool, len(t))
		for _, item := range t {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s: type must be a string or an array of strings", at(path))
			}
			out[s] = true
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s: type must be a string or an array of strings", at(path))
	}
}

func checkType(pv, nv any, path string) error {
	newTypes, err := typeSet(nv, path)
	if err != nil {
		return err
	}
	if newTypes == nil {
		return nil
	}
	oldTypes, err := typeSet(pv, path)
	if err != nil {
		return err
	}
	if oldTypes == nil {
		return fmt.Errorf("%s: type restricted to %v where the previous version allowed any type", at(path), sortedKeys(newTypes))
	}
	for t := range oldTypes {
		if newTypes[t] || (t == "integer" && newTypes["number"]) {
			continue
		}
		return fmt.Errorf("%s: type %q no longer allowed (was %v, now %v)", at(path), t, sortedKeys(oldTypes), sortedKeys(newTypes))
	}
	return nil
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// checkEnumConst treats enum and const as one "value set" constraint.
func (c *compatChecker) checkEnumConst(prevObj, nextObj map[string]any, path string) error {
	newValues, newKeyword, err := valueSet(nextObj, path, c.ignored["const"])
	if err != nil {
		return err
	}
	if newKeyword == "" {
		return nil
	}
	oldValues, oldKeyword, err := valueSet(prevObj, path, c.ignored["const"])
	if err != nil {
		return err
	}
	if oldKeyword == "" {
		return fmt.Errorf("%s: %s added where the previous version accepted any value", at(path), newKeyword)
	}
	// Indexed by canonical form rather than compared pairwise: a 256
	// KiB enum holds tens of thousands of values, and pairwise jsonEqual
	// (which parses every number into a big.Rat) took 38 s and 22 GiB
	// on one such schema.
	index := make(map[string]bool, len(newValues))
	for _, nv := range newValues {
		index[canonicalJSON(nv)] = true
	}
	for _, ov := range oldValues {
		if !index[canonicalJSON(ov)] {
			return fmt.Errorf("%s: value %s accepted by the previous %s is not in the new %s", at(path), jsonString(ov), oldKeyword, newKeyword)
		}
	}
	return nil
}

// canonicalJSON renders a decoded JSON value so that two values are
// jsonEqual exactly when their renderings are equal: object keys
// sorted, numbers as exact rationals (1, 1.0 and 1e0 render alike).
func canonicalJSON(v any) string {
	var b strings.Builder
	writeCanonical(&b, v)
	return b.String()
}

func writeCanonical(b *strings.Builder, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Quote(k))
			b.WriteByte(':')
			writeCanonical(b, t[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeCanonical(b, e)
		}
		b.WriteByte(']')
	case string:
		b.WriteString(strconv.Quote(t))
	case json.Number, float64, int, int64:
		if r, ok := ratOf(t); ok {
			b.WriteString(r.RatString())
		} else {
			// A literal big.Rat refuses; its text is the only identity
			// it has, which is what jsonEqual falls back to as well.
			b.WriteString("#")
			b.WriteString(numberText(t))
		}
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		b.WriteString("null")
	}
}

// valueSet returns the values a node's enum and const admit. Both on
// one node are a conjunction to the validator, so the set is const if
// enum contains it and empty otherwise. ignoreConst is set for drafts
// that predate const.
func valueSet(obj map[string]any, path string, ignoreConst bool) ([]any, string, error) {
	cv, hasConst := obj["const"]
	if ignoreConst {
		hasConst = false
	}
	ev, hasEnum := obj["enum"]
	if !hasConst && !hasEnum {
		return nil, "", nil
	}
	var values []any
	if hasEnum {
		arr, ok := ev.([]any)
		if !ok {
			return nil, "", fmt.Errorf("%s: enum must be an array", at(path))
		}
		values = arr
	}
	switch {
	case hasConst && hasEnum:
		if slices.ContainsFunc(values, func(v any) bool { return jsonEqual(v, cv) }) {
			return []any{cv}, "const and enum", nil
		}
		return nil, "const and enum", nil
	case hasConst:
		return []any{cv}, "const", nil
	default:
		return values, "enum", nil
	}
}

// ratOf reads a JSON number exactly. It fails for literals big.Rat
// refuses (exponents beyond a million), which registration rejects
// anyway; a persisted schema carrying one fails closed.
func ratOf(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case json.Number:
		return new(big.Rat).SetString(string(n))
	case float64:
		return new(big.Rat).SetFloat64(n), true
	case int:
		return big.NewRat(int64(n), 1), true
	case int64:
		return big.NewRat(n, 1), true
	default:
		return nil, false
	}
}

// numberText is the literal to show in messages.
func numberText(v any) string {
	if n, ok := v.(json.Number); ok {
		return string(n)
	}
	return fmt.Sprint(v)
}

type bound struct {
	value     *big.Rat
	text      string
	exclusive bool
	set       bool
}

// lowerBound folds minimum and exclusiveMinimum into the tightest lower
// bound the subschema imposes.
func lowerBound(obj map[string]any, path string) (bound, error) {
	return foldBound(obj, path, "minimum", "exclusiveMinimum", 1)
}

func upperBound(obj map[string]any, path string) (bound, error) {
	return foldBound(obj, path, "maximum", "exclusiveMaximum", -1)
}

// foldBound keeps the tighter of the inclusive and exclusive keyword;
// tighterSign is the Cmp result that means "tighter" (1 for lower
// bounds, -1 for upper bounds).
func foldBound(obj map[string]any, path, inclusiveKey, exclusiveKey string, tighterSign int) (bound, error) {
	var out bound
	for _, key := range []string{inclusiveKey, exclusiveKey} {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		v, ok := ratOf(raw)
		if !ok {
			return bound{}, fmt.Errorf("%s: %s must be a number (draft-04 boolean form is not supported)", at(path), key)
		}
		b := bound{value: v, text: numberText(raw), exclusive: key == exclusiveKey, set: true}
		cmp := 0
		if out.set {
			cmp = b.value.Cmp(out.value)
		}
		if !out.set || cmp == tighterSign || (cmp == 0 && b.exclusive) {
			out = b
		}
	}
	return out, nil
}

// looserOrEqual reports whether bound n admits everything bound p does;
// looserSign is the Cmp result of n against p that means "looser".
func looserOrEqual(p, n bound, looserSign int) bool {
	cmp := n.value.Cmp(p.value)
	if cmp == 0 {
		return !n.exclusive || p.exclusive
	}
	return cmp == looserSign
}

func (c *compatChecker) checkNumericBounds(prevObj, nextObj map[string]any, path string) error {
	nl, err := lowerBound(nextObj, path)
	if err != nil {
		return err
	}
	if nl.set {
		pl, err := lowerBound(prevObj, path)
		if err != nil {
			return err
		}
		if !pl.set {
			return fmt.Errorf("%s: lower bound %s added where the previous version had none", at(path), formatBound(nl))
		}
		if !looserOrEqual(pl, nl, -1) {
			return fmt.Errorf("%s: lower bound tightened from %s to %s", at(path), formatBound(pl), formatBound(nl))
		}
	}
	nu, err := upperBound(nextObj, path)
	if err != nil {
		return err
	}
	if nu.set {
		pu, err := upperBound(prevObj, path)
		if err != nil {
			return err
		}
		if !pu.set {
			return fmt.Errorf("%s: upper bound %s added where the previous version had none", at(path), formatBound(nu))
		}
		if !looserOrEqual(pu, nu, 1) {
			return fmt.Errorf("%s: upper bound tightened from %s to %s", at(path), formatBound(pu), formatBound(nu))
		}
	}
	return nil
}

func formatBound(b bound) string {
	if b.exclusive {
		return b.text + " (exclusive)"
	}
	return b.text
}

// checkMultipleOf requires the new value to divide the old one
// exactly: every multiple of p is then a multiple of n. The validator
// works in big.Rat, so a float64 tolerance here would accept 1 -> 0.9999999999
// and reject the payload 1 that the previous version accepted.
func checkMultipleOf(pv, nv any, path string) error {
	if nv == nil {
		return nil
	}
	n, ok := ratOf(nv)
	if !ok || n.Sign() <= 0 {
		return fmt.Errorf("%s: multipleOf must be a positive number", at(path))
	}
	if pv == nil {
		return fmt.Errorf("%s: multipleOf added where the previous version had none", at(path))
	}
	p, ok := ratOf(pv)
	if !ok || p.Sign() <= 0 {
		return fmt.Errorf("%s: multipleOf must be a positive number", at(path))
	}
	if !new(big.Rat).Quo(p, n).IsInt() {
		return fmt.Errorf("%s: multipleOf changed from %s to %s, which does not divide it", at(path), numberText(pv), numberText(nv))
	}
	return nil
}

// checkLengthMin handles the "at least N" family. absentDefault is the
// value the keyword takes when absent (0 for lengths, 1 for
// minContains), so "absent" can be compared like any other value.
func checkLengthMin(k string, pv any, pok bool, nv any, nok bool, path string, absentDefault float64) error {
	n := absentDefault
	if nok {
		var ok bool
		if n, ok = numberOf(nv); !ok {
			return fmt.Errorf("%s: %s must be a number", at(path), k)
		}
	}
	p := absentDefault
	if pok {
		var ok bool
		if p, ok = numberOf(pv); !ok {
			return fmt.Errorf("%s: %s must be a number", at(path), k)
		}
	}
	if n > p {
		return fmt.Errorf("%s: %s tightened from %v to %v", at(path), k, p, n)
	}
	return nil
}

func checkLengthMax(k string, pv any, pok bool, nv any, nok bool, path string) error {
	if !nok {
		return nil
	}
	n, ok := numberOf(nv)
	if !ok {
		return fmt.Errorf("%s: %s must be a number", at(path), k)
	}
	if !pok {
		return fmt.Errorf("%s: %s added where the previous version had none", at(path), k)
	}
	p, ok := numberOf(pv)
	if !ok {
		return fmt.Errorf("%s: %s must be a number", at(path), k)
	}
	if n < p {
		return fmt.Errorf("%s: %s tightened from %v to %v", at(path), k, p, n)
	}
	return nil
}

func stringSet(v any, k, path string) (map[string]bool, error) {
	if v == nil {
		return map[string]bool{}, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: %s must be an array of strings", at(path), k)
	}
	out := make(map[string]bool, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s: %s must be an array of strings", at(path), k)
		}
		out[s] = true
	}
	return out, nil
}

func checkRequired(pv, nv any, path string) error {
	newReq, err := stringSet(nv, "required", path)
	if err != nil {
		return err
	}
	oldReq, err := stringSet(pv, "required", path)
	if err != nil {
		return err
	}
	for _, name := range sortedKeys(newReq) {
		if !oldReq[name] {
			return fmt.Errorf("%s: required property %q added", at(path), name)
		}
	}
	return nil
}

func propertyMap(v any, path string) (map[string]any, error) {
	if v == nil {
		return map[string]any{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s: properties must be an object", at(path))
	}
	return m, nil
}

func (c *compatChecker) checkProperties(prevObj, nextObj map[string]any, path string, depth int) error {
	prevProps, err := propertyMap(prevObj["properties"], path)
	if err != nil {
		return err
	}
	nextProps, err := propertyMap(nextObj["properties"], path)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(prevProps)+len(nextProps))
	for name := range prevProps {
		names = append(names, name)
	}
	for name := range nextProps {
		if _, dup := prevProps[name]; !dup {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	for _, name := range names {
		childPath := path + "/properties/" + name
		pp, inPrev := prevProps[name]
		np, inNext := nextProps[name]
		switch {
		case inPrev && !inNext:
			return fmt.Errorf("%s: property %q removed", at(path), name)
		case inPrev:
			if err := c.check(pp, np, childPath, depth+1); err != nil {
				return err
			}
		default:
			// A new property. If its name matched a previous
			// patternProperties pattern, that pattern's schema is what
			// constrained it (additionalProperties does not apply to
			// matched names), so the new schema must be wider than one
			// of the matching patterns' schemas. Otherwise: under a
			// closed model the previous version rejected any document
			// carrying it, under an open one this is the documented
			// exception, and only when the previous version constrained
			// extra properties with a schema does the new property's
			// schema have something to be wider than.
			matched, err := matchingPatternSchemas(prevObj, name, path)
			if err != nil {
				return err
			}
			if len(matched) > 0 {
				var firstErr error
				for _, m := range matched {
					err := c.check(m.schema, np, childPath, depth+1)
					if err == nil {
						firstErr = nil
						break
					}
					if firstErr == nil {
						firstErr = fmt.Errorf("new property %q must accept everything the previous patternProperties %q schema did: %w", name, m.pattern, err)
					}
				}
				if firstErr != nil {
					return firstErr
				}
				continue
			}
			if prevAP, ok := prevObj["additionalProperties"].(map[string]any); ok {
				if err := c.check(prevAP, np, childPath, depth+1); err != nil {
					return fmt.Errorf("new property %q must accept everything the previous additionalProperties schema did: %w", name, err)
				}
			}
		}
	}
	return nil
}

type patternSchema struct {
	pattern string
	schema  any
}

// matchingPatternSchemas returns the patternProperties entries of obj
// whose pattern matches name, in pattern order. A pattern that does
// not compile fails closed.
func matchingPatternSchemas(obj map[string]any, name, path string) ([]patternSchema, error) {
	pp, ok := obj["patternProperties"].(map[string]any)
	if !ok {
		return nil, nil
	}
	patterns := make([]string, 0, len(pp))
	for p := range pp {
		patterns = append(patterns, p)
	}
	slices.Sort(patterns)
	var out []patternSchema
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("%s: patternProperties pattern %q cannot be evaluated by the compatibility check: %w", at(path), p, err)
		}
		if re.MatchString(name) {
			out = append(out, patternSchema{pattern: p, schema: pp[p]})
		}
	}
	return out, nil
}

func (c *compatChecker) checkAdditionalProperties(pv any, pok bool, nv any, nok bool, path string, depth int) error {
	if !nok {
		return nil
	}
	switch n := nv.(type) {
	case bool:
		if n {
			return nil
		}
		if pok && pv == false {
			return nil
		}
		return fmt.Errorf("%s: additionalProperties: false closes a content model the previous version left open", at(path))
	case map[string]any:
		switch p := pv.(type) {
		case bool:
			if !p {
				return nil
			}
		case map[string]any:
			return c.check(p, n, path+"/additionalProperties", depth+1)
		}
		return fmt.Errorf("%s: additionalProperties schema added where the previous version accepted any extra property", at(path))
	default:
		return fmt.Errorf("%s: additionalProperties must be a boolean or a schema", at(path))
	}
}

func (c *compatChecker) checkItems(pv any, pok bool, nv any, nok bool, path string, depth int) error {
	if !nok {
		return nil
	}
	if _, tuple := nv.([]any); tuple {
		return fmt.Errorf("%s: array-form items (draft-07 tuples) is not supported by the compatibility check", at(path))
	}
	if !pok {
		return fmt.Errorf("%s: items constraint added where the previous version accepted any element", at(path))
	}
	if _, tuple := pv.([]any); tuple {
		return fmt.Errorf("%s: array-form items (draft-07 tuples) is not supported by the compatibility check", at(path))
	}
	return c.check(pv, nv, path+"/items", depth+1)
}

func branches(v any, k, path string) ([]any, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: %s must be an array of schemas", at(path), k)
	}
	return arr, nil
}

// checkAnyOf: a document valid under old branch i is valid under the
// new anyOf if some new branch subsumes branch i.
func (c *compatChecker) checkAnyOf(pv any, pok bool, nv any, nok bool, path string, depth int) error {
	if !nok {
		return nil
	}
	newBranches, err := branches(nv, "anyOf", path)
	if err != nil {
		return err
	}
	if !pok {
		return fmt.Errorf("%s: anyOf added where the previous version had none", at(path))
	}
	oldBranches, err := branches(pv, "anyOf", path)
	if err != nil {
		return err
	}
	for i, ob := range oldBranches {
		covered := slices.ContainsFunc(newBranches, func(nb any) bool {
			return c.check(ob, nb, fmt.Sprintf("%s/anyOf/%d", path, i), depth+1) == nil
		})
		if !covered {
			return fmt.Errorf("%s: anyOf branch %d of the previous version is not covered by any branch of the new anyOf", at(path), i)
		}
	}
	return nil
}

// checkAllOf: every new conjunct must be implied by some old conjunct.
func (c *compatChecker) checkAllOf(pv any, pok bool, nv any, nok bool, path string, depth int) error {
	if !nok {
		return nil
	}
	newBranches, err := branches(nv, "allOf", path)
	if err != nil {
		return err
	}
	if !pok {
		return fmt.Errorf("%s: allOf added where the previous version had none", at(path))
	}
	oldBranches, err := branches(pv, "allOf", path)
	if err != nil {
		return err
	}
	for j, nb := range newBranches {
		implied := slices.ContainsFunc(oldBranches, func(ob any) bool {
			return c.check(ob, nb, fmt.Sprintf("%s/allOf/%d", path, j), depth+1) == nil
		})
		if !implied {
			return fmt.Errorf("%s: allOf branch %d of the new version is not implied by any branch of the previous allOf", at(path), j)
		}
	}
	return nil
}

// numberOf reads a JSON number decoded as json.Number or float64.
func numberOf(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// jsonEqual is JSON-value equality: numbers compare numerically, so
// 1 and 1.0 are equal, as JSON Schema requires for enum and const.
func jsonEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, ok := bv[k]
			if !ok || !jsonEqual(x, y) {
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
			if !jsonEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case json.Number:
		// Two decoded numbers compare exactly: a float64 comparison
		// would call 2^53+1 and 2^53 equal.
		if bn, ok := b.(json.Number); ok {
			ar, aok := new(big.Rat).SetString(string(av))
			br, bok := new(big.Rat).SetString(string(bn))
			return aok && bok && ar.Cmp(br) == 0
		}
		af, _ := numberOf(av)
		bf, ok := numberOf(b)
		return ok && af == bf
	case float64, int, int64:
		af, _ := numberOf(av)
		bf, ok := numberOf(b)
		return ok && af == bf
	default:
		return a == b
	}
}

func jsonString(v any) string {
	out, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(out)
}
