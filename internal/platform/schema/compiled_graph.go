package schema

import (
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// maxCompiledNodes bounds the compiled subschemas the cost analyses
// take in. A registrable document compiles to far fewer; one that
// compiles to more is refused as too intricate to analyse (fail
// closed), and a persisted one is validated under the validation limit.
const maxCompiledNodes = 1 << 16

// compiledGraph is the schema graph the cost analyses walk, built from
// what the compiler resolved rather than from the raw document: every
// reference already points at the subschema the validator will apply,
// however it was written ($anchor, $dynamicAnchor used as a plain
// anchor, an $id or draft-04 id fragment, a nested resource, a pointer
// into a member no keyword owns). Analysing the raw document instead
// missed every reference form it did not index, so a chain written
// through one of them was never counted.
type compiledGraph struct {
	*schemaGraph
	schemas []*jsonschema.Schema // node -> compiled subschema
}

// buildCompiledGraph builds the graph of root, the compiled form of
// doc registered under resource with c. Dynamic references are given
// every target the validator could pick at run time: a $dynamicRef
// that resolves dynamically goes to the root resource's anchor of that
// name when the root declares one (the root is always the outermost
// scope), and otherwise to every subschema anywhere in the document
// declaring it; a $recursiveRef that resolves dynamically goes to the
// root when the root has $recursiveAnchor, and otherwise to every
// subschema that could be the outermost one of its resource in scope.
// More targets can only raise the counts. The error is a refusal the
// caller reports as a cost error: a reference into something that is
// not part of the document, or a graph too large to analyse.
func buildCompiledGraph(c *jsonschema.Compiler, resource string, doc any, root *jsonschema.Schema) (*compiledGraph, error) {
	b := &compiledGraphBuilder{
		g:   &compiledGraph{schemaGraph: &schemaGraph{ids: map[uintptr]int{}}},
		ids: map[*jsonschema.Schema]int{},
	}
	if err := b.add(root); err != nil {
		return nil, err
	}
	dynamic, rootDynamic := dynamicAnchorTargets(c, resource, doc)
	for _, name := range slices.Sorted(maps.Keys(dynamic)) {
		for _, s := range dynamic[name] {
			if err := b.add(s); err != nil {
				return nil, err
			}
		}
	}
	for i := 0; i < len(b.g.schemas); i++ {
		for _, child := range staticChildren(b.g.schemas[i]) {
			if err := b.add(child); err != nil {
				return nil, err
			}
		}
	}

	// Every subschema a reference can jump to: the outermost schema of
	// a resource in scope is either its root, entered by descending
	// into it, or one of these.
	var jumps []int
	jumped := map[int]bool{}
	jump := func(s *jsonschema.Schema) {
		if id, ok := b.ids[s]; ok && !jumped[id] {
			jumped[id] = true
			jumps = append(jumps, id)
		}
	}
	for _, s := range b.g.schemas {
		if s.Ref != nil {
			jump(s.Ref)
		}
		if s.RecursiveRef != nil {
			jump(s.RecursiveRef)
		}
		if s.DynamicRef != nil && s.DynamicRef.Ref != nil {
			jump(s.DynamicRef.Ref)
		}
	}
	for _, list := range dynamic {
		for _, s := range list {
			jump(s)
		}
	}
	slices.Sort(jumps)
	var recursiveAnchors []int
	for id, s := range b.g.schemas {
		if s.RecursiveAnchor {
			recursiveAnchors = append(recursiveAnchors, id)
		}
	}

	for id, s := range b.g.schemas {
		b.addEdges(id, s, root, dynamic, rootDynamic, jumps, recursiveAnchors)
	}
	return b.g, nil
}

type compiledGraphBuilder struct {
	g   *compiledGraph
	ids map[*jsonschema.Schema]int
}

func (b *compiledGraphBuilder) add(s *jsonschema.Schema) error {
	if s == nil {
		return nil
	}
	if _, ok := b.ids[s]; ok {
		return nil
	}
	if isMetaschema(s) {
		return &costError{msg: fmt.Sprintf("schema reaches %s, which is not part of the document; only references within the schema are allowed", s.Location)}
	}
	if len(b.g.schemas) >= maxCompiledNodes {
		return errPathBudget
	}
	b.ids[s] = len(b.g.schemas)
	b.g.schemas = append(b.g.schemas, s)
	b.g.nodes = append(b.g.nodes, &graphNode{path: locationPath(s.Location)})
	return nil
}

func isMetaschema(s *jsonschema.Schema) bool {
	return strings.HasPrefix(s.Location, "http://json-schema.org/") || strings.HasPrefix(s.Location, "https://json-schema.org/")
}

// locationPath is the JSON pointer part of a compiled schema's location,
// for messages ("#" + path).
func locationPath(loc string) string {
	if _, frag, ok := strings.Cut(loc, "#"); ok {
		return frag
	}
	return ""
}

// staticChildren lists every subschema s applies through a keyword,
// including the static target of each reference, in a stable order.
func staticChildren(s *jsonschema.Schema) []*jsonschema.Schema {
	var out []*jsonschema.Schema
	push := func(c *jsonschema.Schema) {
		if c != nil {
			out = append(out, c)
		}
	}
	pushAny := func(v any) {
		switch t := v.(type) {
		case *jsonschema.Schema:
			push(t)
		case []*jsonschema.Schema:
			for _, c := range t {
				push(c)
			}
		}
	}
	push(s.Ref)
	push(s.RecursiveRef)
	if s.DynamicRef != nil {
		push(s.DynamicRef.Ref)
	}
	push(s.Not)
	push(s.If)
	push(s.Then)
	push(s.Else)
	for _, l := range [][]*jsonschema.Schema{s.AllOf, s.AnyOf, s.OneOf, s.PrefixItems} {
		for _, c := range l {
			push(c)
		}
	}
	push(s.PropertyNames)
	for _, name := range slices.Sorted(maps.Keys(s.Properties)) {
		push(s.Properties[name])
	}
	for _, re := range patternKeys(s.PatternProperties) {
		push(s.PatternProperties[re])
	}
	pushAny(s.AdditionalProperties)
	for _, name := range slices.Sorted(maps.Keys(s.Dependencies)) {
		pushAny(s.Dependencies[name])
	}
	for _, name := range slices.Sorted(maps.Keys(s.DependentSchemas)) {
		push(s.DependentSchemas[name])
	}
	push(s.UnevaluatedProperties)
	push(s.Contains)
	pushAny(s.Items)
	pushAny(s.AdditionalItems)
	push(s.Items2020)
	push(s.UnevaluatedItems)
	return out
}

func (b *compiledGraphBuilder) addEdges(id int, s *jsonschema.Schema, root *jsonschema.Schema, dynamic map[string][]*jsonschema.Schema, rootDynamic map[string]*jsonschema.Schema, jumps, recursiveAnchors []int) {
	n := b.g.nodes[id]
	idOf := func(c *jsonschema.Schema) (int, bool) {
		if c == nil {
			return 0, false
		}
		t, ok := b.ids[c]
		return t, ok
	}
	eps := func(c *jsonschema.Schema, kw string) {
		if t, ok := idOf(c); ok {
			n.eps = append(n.eps, epsEdge{to: t, kw: kw})
		}
	}
	epsTo := func(targets []int, kw string) {
		for _, t := range targets {
			n.eps = append(n.eps, epsEdge{to: t, kw: kw})
		}
	}
	desc := func(c *jsonschema.Schema, sel selector, kw string) {
		if t, ok := idOf(c); ok {
			n.desc = append(n.desc, descEdge{to: t, sel: sel, kw: kw})
		}
	}

	// Same value.
	eps(s.Ref, "$ref")
	if s.RecursiveRef != nil {
		switch {
		case !s.RecursiveRef.RecursiveAnchor:
			eps(s.RecursiveRef, "$recursiveRef")
		case root.RecursiveAnchor:
			eps(root, "$recursiveRef")
		default:
			eps(s.RecursiveRef, "$recursiveRef")
			epsTo(recursiveAnchors, "$recursiveRef")
			epsTo(jumps, "$recursiveRef")
		}
	}
	if d := s.DynamicRef; d != nil && d.Ref != nil {
		switch {
		case d.Anchor == "" || d.Ref.DynamicAnchor != d.Anchor:
			eps(d.Ref, "$dynamicRef")
		case rootDynamic[d.Anchor] != nil:
			eps(rootDynamic[d.Anchor], "$dynamicRef")
		default:
			eps(d.Ref, "$dynamicRef")
			for _, t := range dynamic[d.Anchor] {
				eps(t, "$dynamicRef")
			}
		}
	}
	for _, k := range []struct {
		kw   string
		list []*jsonschema.Schema
	}{{"allOf", s.AllOf}, {"anyOf", s.AnyOf}, {"oneOf", s.OneOf}} {
		for i, c := range k.list {
			eps(c, k.kw+"/"+strconv.Itoa(i))
		}
	}
	eps(s.Not, "not")
	eps(s.If, "if")
	eps(s.Then, "then")
	eps(s.Else, "else")
	for _, name := range slices.Sorted(maps.Keys(s.DependentSchemas)) {
		eps(s.DependentSchemas[name], "dependentSchemas/"+name)
	}
	for _, name := range slices.Sorted(maps.Keys(s.Dependencies)) {
		if c, ok := s.Dependencies[name].(*jsonschema.Schema); ok {
			eps(c, "dependencies/"+name)
		}
	}

	// Array items.
	for i, c := range s.PrefixItems {
		desc(c, selector{kind: selArray, idx: i, exact: true}, "prefixItems/"+strconv.Itoa(i))
	}
	desc(s.Items2020, selector{kind: selArray, from: len(s.PrefixItems)}, "items")
	switch items := s.Items.(type) {
	case *jsonschema.Schema:
		desc(items, selector{kind: selArray}, "items")
	case []*jsonschema.Schema:
		for i, c := range items {
			desc(c, selector{kind: selArray, idx: i, exact: true}, "items/"+strconv.Itoa(i))
		}
		if c, ok := s.AdditionalItems.(*jsonschema.Schema); ok {
			desc(c, selector{kind: selArray, from: len(items)}, "additionalItems")
		}
	case nil:
		// The library applies additionalItems to every item when items
		// is absent.
		if c, ok := s.AdditionalItems.(*jsonschema.Schema); ok {
			desc(c, selector{kind: selArray}, "additionalItems")
		}
	}
	desc(s.Contains, selector{kind: selArray, star: true}, "contains")
	desc(s.UnevaluatedItems, selector{kind: selArray, star: true}, "unevaluatedItems")

	// Object members. propertyNames applies to the names, strings with
	// no children: a key for the path count, no edge.
	if t, ok := idOf(s.PropertyNames); ok {
		n.keys = append(n.keys, t)
	}
	var exclNames map[string]bool
	if len(s.Properties) > 0 {
		exclNames = make(map[string]bool, len(s.Properties))
	}
	for _, name := range slices.Sorted(maps.Keys(s.Properties)) {
		exclNames[name] = true
		desc(s.Properties[name], selector{kind: selObject, name: name, hasName: true}, "properties/"+name)
	}
	var exclPatterns []*regexp.Regexp
	for _, re := range patternKeys(s.PatternProperties) {
		goRe, _ := re.(*regexp.Regexp)
		if goRe != nil {
			exclPatterns = append(exclPatterns, goRe)
		}
		// A pattern of another engine is assumed to match any name.
		desc(s.PatternProperties[re], selector{kind: selObject, pattern: goRe, hasPattern: true}, "patternProperties/"+re.String())
	}
	if c, ok := s.AdditionalProperties.(*jsonschema.Schema); ok {
		desc(c, selector{kind: selObject, additional: true, exclNames: exclNames, exclPatterns: exclPatterns}, "additionalProperties")
	}
	desc(s.UnevaluatedProperties, selector{kind: selObject, star: true}, "unevaluatedProperties")
}

// dynamicAnchorTargets compiles, with the compiler that compiled the
// document, every object anywhere in doc that declares a $dynamicAnchor
// (by name), and the root resource's own dynamic anchors. The compiler
// caches by location, so an anchor it already compiled comes back as
// the very subschema the validator would resolve to; an object that is
// not a subschema the compiler accepts is skipped, since it can never
// be a resolution target.
func dynamicAnchorTargets(c *jsonschema.Compiler, resource string, doc any) (byName map[string][]*jsonschema.Schema, root map[string]*jsonschema.Schema) {
	type pending struct {
		v   any
		ptr string
	}
	stack := []pending{{v: doc}}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch t := p.v.(type) {
		case map[string]any:
			if name, ok := t["$dynamicAnchor"].(string); ok {
				if s, err := c.Compile(resource + "#" + p.ptr); err == nil {
					if byName == nil {
						byName, root = map[string][]*jsonschema.Schema{}, map[string]*jsonschema.Schema{}
					}
					if !slices.Contains(byName[name], s) {
						byName[name] = append(byName[name], s)
					}
				}
			}
			for _, k := range slices.Sorted(maps.Keys(t)) {
				stack = append(stack, pending{v: t[k], ptr: p.ptr + "/" + url.PathEscape(pointerEscape(k))})
			}
		case []any:
			for i, v := range t {
				stack = append(stack, pending{v: v, ptr: p.ptr + "/" + strconv.Itoa(i)})
			}
		}
	}
	for name := range byName {
		if s, err := c.Compile(resource + "#" + url.PathEscape(name)); err == nil && s.DynamicAnchor == name {
			root[name] = s
		}
	}
	return byName, root
}

func pointerEscape(tok string) string {
	return strings.ReplaceAll(strings.ReplaceAll(tok, "~", "~0"), "/", "~1")
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// patternKeys returns the patternProperties keys in source order.
func patternKeys(m map[jsonschema.Regexp]*jsonschema.Schema) []jsonschema.Regexp {
	out := make([]jsonschema.Regexp, 0, len(m))
	for re := range m {
		out = append(out, re)
	}
	slices.SortFunc(out, func(a, b jsonschema.Regexp) int { return strings.Compare(a.String(), b.String()) })
	return out
}
