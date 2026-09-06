package schema

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// This file holds the generators behind the fuzz and property tests:
// a byte-driven schema generator, a schema-aware payload generator,
// and a mutator that turns one schema into a candidate next version.
// Every decision is drawn from a byteSrc so that the fuzzer's
// mutations of the input bytes translate into structural changes.

// byteSrc hands out decisions from a byte slice; once the bytes run
// out it returns zeros, which every generator maps to the smallest
// choice so generation always terminates.
type byteSrc struct {
	b []byte
	i int
}

func newByteSrc(b []byte) *byteSrc { return &byteSrc{b: b} }

func (s *byteSrc) next() byte {
	if s.i >= len(s.b) {
		s.i++
		return 0
	}
	v := s.b[s.i]
	s.i++
	return v
}

// n returns a value in [0, max).
func (s *byteSrc) n(max int) int {
	if max <= 1 {
		return 0
	}
	if max <= 256 {
		return int(s.next()) % max
	}
	v := int(s.next())<<8 | int(s.next())
	return v % max
}

// chance reports true about pct percent of the time.
func (s *byteSrc) chance(pct int) bool { return s.n(100) < pct }

func (s *byteSrc) exhausted() bool { return s.i >= len(s.b) }

// ---------------------------------------------------------------------
// Schema generator
// ---------------------------------------------------------------------

// genOptions tunes the schema generator. wellFormed keeps every
// keyword value valid against the metaschema so the result compiles
// (the payload and compatibility tests need that); adversarial mode
// also emits garbage values, external references, unknown drafts,
// oversized enums and nesting around the limits.
type genOptions struct {
	adversarial bool
	maxDepth    int
	maxNodes    int
}

type schemaGen struct {
	src   *byteSrc
	opts  genOptions
	defs  map[string]any
	nodes int
}

var (
	// patternCatalog maps RE2 patterns to strings that match them. The
	// payload generator draws from the samples, so a schema using one
	// of these patterns gets payloads that pass it.
	patternCatalog = map[string][]string{
		"^[a-z]+$":       {"a", "abc", "zzzz"},
		"^[0-9]{3}$":     {"123", "000", "999"},
		"^x-":            {"x-1", "x-tag", "x-"},
		"^[A-Z][a-z]*$":  {"A", "Abc", "Zed"},
		"^[a-z]+-[0-9]+": {"sku-1", "ab-22", "z-0"},
		"\\d":            {"1", "a2", "x9y"},
		"^\\p{L}+$":      {"héllo", "日本", "abc"},
	}
	// formatCatalog maps format names to valid samples.
	formatCatalog = map[string][]string{
		"email":     {"a@b.co", "x.y@example.com"},
		"date-time": {"2026-09-06T10:00:00Z", "2000-01-01T00:00:00+05:30"},
		"date":      {"2026-09-06", "1999-12-31"},
		"time":      {"10:00:00Z", "23:59:59+01:00"},
		"uuid":      {"123e4567-e89b-12d3-a456-426614174000"},
		"ipv4":      {"127.0.0.1", "10.0.0.1"},
		"ipv6":      {"::1", "2001:db8::1"},
		"uri":       {"https://example.com/a?b=c", "mailto:a@b.co"},
		"hostname":  {"example.com", "a-b.c"},
		"regex":     {"^a+$", "b"},
	}
	// sortedPatterns and sortedFormats give deterministic order.
	sortedPatterns = sortedKeysOf(patternCatalog)
	sortedFormats  = sortedKeysOf(formatCatalog)
	builtinDrafts  = []string{
		"", // default 2020-12
		"https://json-schema.org/draft/2020-12/schema",
		"https://json-schema.org/draft/2019-09/schema",
		"http://json-schema.org/draft-07/schema#",
		"http://json-schema.org/draft-06/schema#",
		"http://json-schema.org/draft-04/schema#",
	}
	unicodeSamples = []string{"héllo", "日本語", "😀", "\x00nul", "a b", "\ufeffbom", "Ω≈ç√", "\u200b"}
)

func sortedKeysOf(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// genSchemaDoc builds a whole schema document: a root subschema, an
// optional $defs section referenced from inside, an optional $schema.
func genSchemaDoc(src *byteSrc, opts genOptions) map[string]any {
	if opts.maxDepth == 0 {
		opts.maxDepth = 6
	}
	if opts.maxNodes == 0 {
		opts.maxNodes = 60
	}
	g := &schemaGen{src: src, opts: opts, defs: map[string]any{}}
	// Pre-declare a few $defs names so $ref can point at them (and
	// form cycles) before their bodies exist.
	ndefs := src.n(4)
	names := make([]string, ndefs)
	for i := range names {
		names[i] = fmt.Sprintf("d%d", i)
		g.defs[names[i]] = true
	}
	root := g.genNode(0)
	rootObj, ok := root.(map[string]any)
	if !ok {
		rootObj = map[string]any{}
		if root == false {
			rootObj["not"] = map[string]any{}
		}
	}
	for _, name := range names {
		g.defs[name] = g.genNode(1)
	}
	if len(g.defs) > 0 {
		rootObj["$defs"] = g.defs
	}
	if d := src.n(len(builtinDrafts)); d > 0 {
		rootObj["$schema"] = builtinDrafts[d]
	}
	if opts.adversarial && src.chance(5) {
		rootObj["$schema"] = []string{"https://example.com/my-draft", "file:///etc/passwd", "draft-99"}[src.n(3)]
	}
	if src.chance(10) {
		rootObj["$id"] = "https://example.com/" + names0(src)
	}
	return rootObj
}

func names0(src *byteSrc) string { return fmt.Sprintf("s%d", src.n(1000)) }

func (g *schemaGen) genNode(depth int) any {
	g.nodes++
	src := g.src
	if depth >= g.opts.maxDepth || g.nodes > g.opts.maxNodes {
		return g.genLeaf()
	}
	switch src.n(20) {
	case 0:
		return true
	case 1:
		if g.opts.adversarial {
			return false
		}
		return map[string]any{"type": "null"}
	case 2, 3:
		if len(g.defs) > 0 {
			return map[string]any{"$ref": g.refTo()}
		}
	}
	node := map[string]any{}
	primary := []string{"object", "array", "string", "number", "integer", "boolean", "null"}[src.n(7)]
	if src.chance(80) {
		if src.chance(20) {
			// A type union.
			other := []string{"object", "array", "string", "number", "integer", "boolean", "null"}[src.n(7)]
			node["type"] = []any{primary, other}
		} else {
			node["type"] = primary
		}
	}
	switch primary {
	case "object":
		g.objectKeywords(node, depth)
	case "array":
		g.arrayKeywords(node, depth)
	case "string":
		g.stringKeywords(node)
	case "number", "integer":
		g.numberKeywords(node)
	}
	// Value constraints.
	switch src.n(12) {
	case 0:
		node["enum"] = g.enumValues(primary)
	case 1:
		node["const"] = g.literal(primary)
	}
	// Combinators.
	switch src.n(14) {
	case 0:
		node["anyOf"] = g.branches(depth)
	case 1:
		node["allOf"] = g.branches(depth)
	case 2:
		node["oneOf"] = g.branches(depth)
	case 3:
		node["not"] = g.genNode(depth + 1)
	case 4:
		node["if"] = g.genNode(depth + 1)
		if src.chance(70) {
			node["then"] = g.genNode(depth + 1)
		}
		if src.chance(40) {
			node["else"] = g.genNode(depth + 1)
		}
	}
	// Annotations.
	if src.chance(25) {
		node["title"] = unicodeSamples[src.n(len(unicodeSamples))]
	}
	if src.chance(10) {
		node["description"] = strings.Repeat("d", src.n(40))
	}
	if src.chance(5) {
		node["default"] = g.literal(primary)
	}
	if g.opts.adversarial {
		g.adversarialKeywords(node, depth)
	}
	return node
}

func (g *schemaGen) genLeaf() any {
	switch g.src.n(6) {
	case 0:
		return true
	case 1:
		return map[string]any{}
	case 2:
		return map[string]any{"type": "integer"}
	case 3:
		return map[string]any{"type": "string"}
	case 4:
		return map[string]any{"type": []any{"string", "null"}}
	default:
		return map[string]any{"type": "boolean"}
	}
}

func (g *schemaGen) refTo() string {
	names := make([]string, 0, len(g.defs))
	for k := range g.defs {
		names = append(names, k)
	}
	sort.Strings(names)
	if len(names) == 0 || g.src.chance(10) {
		return "#"
	}
	return "#/$defs/" + names[g.src.n(len(names))]
}

func (g *schemaGen) branches(depth int) []any {
	n := 1 + g.src.n(3)
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, g.genNode(depth+1))
	}
	return out
}

func (g *schemaGen) objectKeywords(node map[string]any, depth int) {
	src := g.src
	nprops := src.n(5)
	if nprops > 0 {
		props := map[string]any{}
		var names []string
		for i := 0; i < nprops; i++ {
			name := fmt.Sprintf("p%d", i)
			if src.chance(15) {
				name = unicodeSamples[src.n(len(unicodeSamples))] + strconv.Itoa(i)
			}
			props[name] = g.genNode(depth + 1)
			names = append(names, name)
		}
		node["properties"] = props
		if src.chance(60) {
			var req []any
			for _, n := range names {
				if src.chance(50) {
					req = append(req, n)
				}
			}
			if len(req) > 0 {
				node["required"] = req
			}
		}
	}
	switch src.n(6) {
	case 0:
		node["additionalProperties"] = false
	case 1:
		node["additionalProperties"] = true
	case 2:
		node["additionalProperties"] = g.genNode(depth + 1)
	}
	if src.chance(12) {
		node["patternProperties"] = map[string]any{sortedPatterns[src.n(len(sortedPatterns))]: g.genNode(depth + 1)}
	}
	if src.chance(8) {
		node["minProperties"] = src.n(3)
	}
	if src.chance(8) {
		node["maxProperties"] = 2 + src.n(6)
	}
	if src.chance(5) {
		node["propertyNames"] = map[string]any{"maxLength": 3 + src.n(20)}
	}
	if src.chance(5) && nprops >= 2 {
		node["dependentRequired"] = map[string]any{"p0": []any{"p1"}}
	}
}

func (g *schemaGen) arrayKeywords(node map[string]any, depth int) {
	src := g.src
	if src.chance(70) {
		node["items"] = g.genNode(depth + 1)
	}
	if src.chance(10) {
		node["prefixItems"] = g.branches(depth)
	}
	if src.chance(15) {
		node["minItems"] = src.n(3)
	}
	if src.chance(15) {
		node["maxItems"] = 1 + src.n(6)
	}
	if src.chance(15) {
		node["uniqueItems"] = src.chance(70)
	}
	if src.chance(8) {
		node["contains"] = g.genNode(depth + 1)
		if src.chance(30) {
			node["minContains"] = src.n(3)
		}
		if src.chance(30) {
			node["maxContains"] = 1 + src.n(4)
		}
	}
}

func (g *schemaGen) stringKeywords(node map[string]any) {
	src := g.src
	if src.chance(25) {
		node["minLength"] = src.n(4)
	}
	if src.chance(25) {
		node["maxLength"] = 3 + src.n(30)
	}
	if src.chance(25) {
		node["pattern"] = sortedPatterns[src.n(len(sortedPatterns))]
	}
	if src.chance(20) {
		node["format"] = sortedFormats[src.n(len(sortedFormats))]
	}
}

// genNumber returns a JSON number as json.Number text so exact values
// (beyond float64) and exponent forms flow through unchanged.
func (g *schemaGen) genNumber(integer bool) json.Number {
	src := g.src
	switch src.n(10) {
	case 0:
		return json.Number("9007199254740993") // 2^53+1
	case 1:
		return json.Number("9223372036854775808") // 2^63
	case 2:
		if integer {
			return json.Number("1e2")
		}
		return json.Number("2.5e-3")
	case 3:
		if integer {
			return json.Number("-12")
		}
		return json.Number("-0.75")
	case 4:
		return json.Number("0")
	}
	v := src.n(200) - 100
	if integer || src.chance(50) {
		return json.Number(strconv.Itoa(v))
	}
	return json.Number(strconv.Itoa(v) + "." + strconv.Itoa(src.n(100)))
}

func (g *schemaGen) numberKeywords(node map[string]any) {
	src := g.src
	if src.chance(30) {
		node["minimum"] = g.genNumber(false)
	}
	if src.chance(30) {
		node["maximum"] = g.genNumber(false)
	}
	if src.chance(10) {
		node["exclusiveMinimum"] = g.genNumber(false)
	}
	if src.chance(10) {
		node["exclusiveMaximum"] = g.genNumber(false)
	}
	if src.chance(20) {
		node["multipleOf"] = []json.Number{"1", "2", "0.5", "0.1", "0.3", "10", "3", "0.25", "1e-2"}[src.n(9)]
	}
}

func (g *schemaGen) literal(typ string) any {
	src := g.src
	switch typ {
	case "string":
		if src.chance(30) {
			return unicodeSamples[src.n(len(unicodeSamples))]
		}
		return "v" + strconv.Itoa(src.n(50))
	case "integer":
		return g.genNumber(true)
	case "number":
		return g.genNumber(false)
	case "boolean":
		return src.chance(50)
	case "null":
		return nil
	case "array":
		return []any{g.literal("integer"), g.literal("string")}
	default:
		return map[string]any{"k": g.literal("string")}
	}
}

func (g *schemaGen) enumValues(typ string) []any {
	n := 1 + g.src.n(5)
	if g.opts.adversarial && g.src.chance(5) {
		n = 5000
	}
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		if n > 20 {
			out = append(out, "e"+strconv.Itoa(i))
			continue
		}
		out = append(out, g.literal(typ))
	}
	return out
}

// adversarialKeywords sprinkles what a hostile registrant would send.
func (g *schemaGen) adversarialKeywords(node map[string]any, depth int) {
	src := g.src
	switch src.n(40) {
	case 0:
		node["$ref"] = []string{"file:///etc/passwd", "http://127.0.0.1:32001/x", "other.json", "#/$defs/missing", "#/properties/../..", "#%zz", "#/$defs/0", ""}[src.n(8)]
	case 1:
		node["pattern"] = []string{"^(?=a)a$", "(a+)+$", "\\", "[", "((a{1000}){1000}){1000}", "(?P<n>a)\\1", "^" + strings.Repeat("(a|", 50) + "b" + strings.Repeat(")", 50) + "$"}[src.n(7)]
	case 2:
		node["type"] = []any{123, "nope", []any{"string", 5}, map[string]any{}}[src.n(4)]
	case 3:
		node["minimum"] = []any{"x", true, json.Number("1e999999"), json.Number("1e99999999"), json.Number("-1e-99999999"), json.Number(strings.Repeat("9", 3000))}[src.n(6)]
	case 4:
		node["properties"] = []any{[]any{}, "s", 1}[src.n(3)]
	case 5:
		node["required"] = []any{"a", 1, nil}
	case 6:
		node["enum"] = []any{}
	case 7:
		node["items"] = []any{true, false, map[string]any{"type": "string"}}
	case 8:
		node["$id"] = []string{"file:///tmp/x", "https://example.com/x#frag", "urn:uuid:x", "#bad", "", "%%%"}[src.n(6)]
	case 9:
		node["$anchor"] = []string{"ok", "bad anchor", "", "#"}[src.n(4)]
		node["$dynamicAnchor"] = "dyn"
	case 10:
		node["$dynamicRef"] = []string{"#dyn", "#missing", "http://x/#dyn"}[src.n(3)]
	case 11:
		node["multipleOf"] = []any{0, -1, "x", json.Number("1e-99999999")}[src.n(4)]
	case 12:
		node["format"] = []string{"", "no-such", strings.Repeat("f", 300), "regex"}[src.n(4)]
	case 13:
		node["$comment"] = strings.Repeat("c", 4096)
	case 14:
		node["dependencies"] = map[string]any{"a": []any{"b"}, "c": map[string]any{"type": "x"}}
	case 15:
		node["unevaluatedProperties"] = false
		node["unevaluatedItems"] = map[string]any{"type": "string"}
	case 16:
		node["$schema"] = "https://json-schema.org/draft/2019-09/schema"
	case 17:
		node["contentSchema"] = map[string]any{"type": "object"}
		node["contentMediaType"] = "application/json"
		node["contentEncoding"] = "base64"
	case 18:
		node["$vocabulary"] = map[string]any{"https://example.com/vocab": true}
	case 19:
		if depth < 2 {
			deep := map[string]any{}
			cur := deep
			for i := 0; i < 20+src.n(50); i++ {
				nxt := map[string]any{}
				cur["items"] = nxt
				cur = nxt
			}
			node["not"] = deep
		}
	case 20:
		node[unicodeSamples[src.n(len(unicodeSamples))]] = g.genLeaf()
	case 21:
		node["patternProperties"] = map[string]any{"^(?!x)": true}
	case 22:
		node["$ref"] = g.refTo()
		node["minimum"] = 0
	}
}

// ---------------------------------------------------------------------
// Schema-position walk (used by invariants)
// ---------------------------------------------------------------------

// walkSchemaPositions calls fn for every node that sits in a schema
// position of doc (applicator keywords only; enum, const, default and
// examples hold literals and are skipped).
func walkSchemaPositions(doc any, fn func(node map[string]any)) {
	var walk func(v any, depth int)
	walk = func(v any, depth int) {
		if depth > 200 {
			return
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return
		}
		fn(obj)
		for k, child := range obj {
			switch k {
			case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas":
				if m, ok := child.(map[string]any); ok {
					for _, sub := range m {
						walk(sub, depth+1)
					}
				}
			case "dependencies":
				if m, ok := child.(map[string]any); ok {
					for _, sub := range m {
						if _, isSchema := sub.(map[string]any); isSchema {
							walk(sub, depth+1)
						}
					}
				}
			case "allOf", "anyOf", "oneOf", "prefixItems":
				if arr, ok := child.([]any); ok {
					for _, sub := range arr {
						walk(sub, depth+1)
					}
				}
			case "items":
				if arr, ok := child.([]any); ok {
					for _, sub := range arr {
						walk(sub, depth+1)
					}
				} else {
					walk(child, depth+1)
				}
			case "additionalProperties", "additionalItems", "not", "if", "then", "else", "contains",
				"propertyNames", "unevaluatedProperties", "unevaluatedItems", "contentSchema":
				walk(child, depth+1)
			}
		}
	}
	walk(doc, 0)
}

// containerDepth is an independent depth measure over the decoded
// value, used to cross-check jsonNestingDepth.
func containerDepth(v any) int {
	switch t := v.(type) {
	case map[string]any:
		d := 0
		for _, c := range t {
			if cd := containerDepth(c); cd > d {
				d = cd
			}
		}
		return d + 1
	case []any:
		d := 0
		for _, c := range t {
			if cd := containerDepth(c); cd > d {
				d = cd
			}
		}
		return d + 1
	default:
		return 0
	}
}

// ---------------------------------------------------------------------
// Payload generator
// ---------------------------------------------------------------------

// payloadGen builds JSON values that are meant to satisfy a schema. It
// is best effort: the compiled schema is the oracle, and candidates it
// rejects are discarded by the caller.
type payloadGen struct {
	src   *byteSrc
	root  any
	extra int
}

func genPayload(src *byteSrc, root any) any {
	p := &payloadGen{src: src, root: root}
	return p.gen(root, 0)
}

func (p *payloadGen) gen(s any, depth int) any {
	src := p.src
	if depth > 12 {
		return nil
	}
	if s == true {
		return p.anyValue(depth)
	}
	obj, ok := s.(map[string]any)
	if !ok {
		return nil
	}
	if ref, ok := obj["$ref"].(string); ok {
		target, err := resolvePointer(p.root, ref)
		if err != nil {
			return nil
		}
		return p.gen(target, depth+1)
	}
	if c, ok := obj["const"]; ok {
		return c
	}
	if e, ok := obj["enum"].([]any); ok && len(e) > 0 {
		return e[src.n(len(e))]
	}
	if br, ok := obj["anyOf"].([]any); ok && len(br) > 0 && !hasOwnConstraints(obj) {
		return p.gen(br[src.n(len(br))], depth+1)
	}
	if br, ok := obj["oneOf"].([]any); ok && len(br) > 0 && !hasOwnConstraints(obj) {
		return p.gen(br[src.n(len(br))], depth+1)
	}
	if br, ok := obj["allOf"].([]any); ok && len(br) > 0 && !hasOwnConstraints(obj) {
		return p.gen(br[0], depth+1)
	}
	typ := pickType(src, obj)
	switch typ {
	case "null":
		return nil
	case "boolean":
		return src.chance(50)
	case "string":
		return p.genString(obj)
	case "integer", "number":
		return p.genNumber(obj, typ == "integer")
	case "array":
		return p.genArray(obj, depth)
	default:
		return p.genObject(obj, depth)
	}
}

func hasOwnConstraints(obj map[string]any) bool {
	for k := range obj {
		switch k {
		case "anyOf", "oneOf", "allOf", "title", "description", "$comment", "default", "examples", "$defs", "$id", "$schema":
		default:
			return true
		}
	}
	return false
}

func pickType(src *byteSrc, obj map[string]any) string {
	switch t := obj["type"].(type) {
	case string:
		return t
	case []any:
		if len(t) > 0 {
			if s, ok := t[src.n(len(t))].(string); ok {
				return s
			}
		}
	}
	// Infer from keywords.
	for _, k := range []string{"properties", "required", "additionalProperties", "patternProperties", "minProperties", "maxProperties"} {
		if _, ok := obj[k]; ok {
			return "object"
		}
	}
	for _, k := range []string{"items", "prefixItems", "minItems", "maxItems", "uniqueItems", "contains"} {
		if _, ok := obj[k]; ok {
			return "array"
		}
	}
	for _, k := range []string{"minLength", "maxLength", "pattern", "format"} {
		if _, ok := obj[k]; ok {
			return "string"
		}
	}
	for _, k := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"} {
		if _, ok := obj[k]; ok {
			return "number"
		}
	}
	return []string{"object", "array", "string", "number", "integer", "boolean", "null"}[src.n(7)]
}

func (p *payloadGen) anyValue(depth int) any {
	switch p.src.n(8) {
	case 0:
		return nil
	case 1:
		return true
	case 2:
		return json.Number("42")
	case 3:
		return json.Number("-1.5e3")
	case 4:
		return unicodeSamples[p.src.n(len(unicodeSamples))]
	case 5:
		if depth < 6 {
			return []any{p.anyValue(depth + 1)}
		}
		return []any{}
	case 6:
		if depth < 6 {
			return map[string]any{"k": p.anyValue(depth + 1)}
		}
		return map[string]any{}
	default:
		return "s"
	}
}

func (p *payloadGen) genString(obj map[string]any) any {
	src := p.src
	var s string
	if pat, ok := obj["pattern"].(string); ok {
		if samples := patternCatalog[pat]; len(samples) > 0 {
			s = samples[src.n(len(samples))]
		}
	} else if f, ok := obj["format"].(string); ok {
		if samples := formatCatalog[f]; len(samples) > 0 {
			s = samples[src.n(len(samples))]
		}
	} else if src.chance(30) {
		s = unicodeSamples[src.n(len(unicodeSamples))]
	} else {
		s = strings.Repeat("s", src.n(6))
	}
	minL, _ := intOf(obj["minLength"])
	maxL, hasMax := intOf(obj["maxLength"])
	runes := []rune(s)
	for len(runes) < minL {
		runes = append(runes, 'a')
	}
	if hasMax && len(runes) > maxL {
		runes = runes[:maxL]
	}
	return string(runes)
}

func intOf(v any) (int, bool) {
	f, ok := numberOf(v)
	if !ok {
		return 0, false
	}
	return int(f), true
}

// genNumber picks a value inside the bounds and on the multipleOf
// grid using exact arithmetic, then emits it in one of several
// spellings (plain, exponent, trailing .0) so integer detection is
// exercised in every form.
func (p *payloadGen) genNumber(obj map[string]any, integer bool) any {
	src := p.src
	step := big.NewRat(1, 1)
	if m, ok := ratOfAny(obj["multipleOf"]); ok && m.Sign() > 0 {
		step = m
		if integer && !step.IsInt() {
			// The grid must land on integers: use the smallest integer
			// multiple of the step.
			k := new(big.Int).Set(step.Denom())
			step = new(big.Rat).Mul(step, new(big.Rat).SetInt(k))
		}
	}
	lo, hasLo := ratOfAny(obj["minimum"])
	if ex, ok := ratOfAny(obj["exclusiveMinimum"]); ok && (!hasLo || ex.Cmp(lo) >= 0) {
		lo, hasLo = new(big.Rat).Add(ex, step), true
	}
	hi, hasHi := ratOfAny(obj["maximum"])
	if ex, ok := ratOfAny(obj["exclusiveMaximum"]); ok && (!hasHi || ex.Cmp(hi) <= 0) {
		hi, hasHi = new(big.Rat).Sub(ex, step), true
	}
	// Start from a grid point at or above lo (or a random small one).
	var v *big.Rat
	switch {
	case hasLo:
		q := new(big.Rat).Quo(lo, step)
		k := ratCeil(q)
		v = new(big.Rat).Mul(new(big.Rat).SetInt(k), step)
	case hasHi:
		q := new(big.Rat).Quo(hi, step)
		k := ratFloor(q)
		v = new(big.Rat).Mul(new(big.Rat).SetInt(k), step)
	default:
		k := int64(src.n(41) - 20)
		if src.chance(10) {
			k = 9007199254740993 // 2^53+1
		}
		v = new(big.Rat).Mul(big.NewRat(k, 1), step)
	}
	// Walk a few grid steps up while staying under hi.
	for i := src.n(4); i > 0; i-- {
		cand := new(big.Rat).Add(v, step)
		if hasHi && cand.Cmp(hi) > 0 {
			break
		}
		v = cand
	}
	if hasHi && v.Cmp(hi) > 0 {
		return json.Number("0") // no grid point fits; oracle will reject
	}
	if integer && !v.IsInt() {
		return json.Number("0")
	}
	return json.Number(spellRat(src, v))
}

func ratCeil(q *big.Rat) *big.Int {
	z := new(big.Int).Quo(q.Num(), q.Denom())
	if !q.IsInt() && q.Sign() > 0 {
		z.Add(z, big.NewInt(1))
	}
	return z
}

func ratFloor(q *big.Rat) *big.Int {
	z := new(big.Int).Quo(q.Num(), q.Denom())
	if !q.IsInt() && q.Sign() < 0 {
		z.Sub(z, big.NewInt(1))
	}
	return z
}

// spellRat renders an exact rational as a JSON number literal in a
// randomly chosen but value-preserving spelling.
func spellRat(src *byteSrc, v *big.Rat) string {
	if v.IsInt() {
		s := v.Num().String()
		switch src.n(4) {
		case 0:
			return s + ".0"
		case 1:
			// Exponent form: strip trailing zeros into the exponent.
			zeros := 0
			for strings.HasSuffix(s, "0") && len(s) > 1 && zeros < 6 {
				s = s[:len(s)-1]
				zeros++
			}
			if s == "-" {
				return "0"
			}
			return s + "e" + strconv.Itoa(zeros)
		case 2:
			return s + "E0"
		}
		return s
	}
	// Non-integer: a finite decimal if the denominator allows it.
	den := new(big.Int).Set(v.Denom())
	twos, fives := 0, 0
	two, five := big.NewInt(2), big.NewInt(5)
	for new(big.Int).Mod(den, two).Sign() == 0 {
		den.Div(den, two)
		twos++
	}
	for new(big.Int).Mod(den, five).Sign() == 0 {
		den.Div(den, five)
		fives++
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		return v.FloatString(20) // inexact; the oracle decides
	}
	prec := twos
	if fives > prec {
		prec = fives
	}
	s := v.FloatString(prec)
	if src.chance(30) {
		// Shift the point into an exponent: 2.5 -> 25e-1.
		neg := strings.HasPrefix(s, "-")
		digits := strings.TrimPrefix(s, "-")
		if i := strings.IndexByte(digits, '.'); i >= 0 {
			frac := len(digits) - i - 1
			digits = strings.TrimLeft(digits[:i]+digits[i+1:], "0")
			if digits == "" {
				digits = "0"
			}
			s = digits + "e-" + strconv.Itoa(frac)
			if neg {
				s = "-" + s
			}
		}
	}
	return s
}

func ratOfAny(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case json.Number:
		return new(big.Rat).SetString(string(n))
	case float64:
		return new(big.Rat).SetFloat64(n), true
	case int:
		return big.NewRat(int64(n), 1), true
	default:
		return nil, false
	}
}

func (p *payloadGen) genArray(obj map[string]any, depth int) any {
	src := p.src
	minI, _ := intOf(obj["minItems"])
	maxI, hasMax := intOf(obj["maxItems"])
	n := minI + src.n(3)
	if hasMax && n > maxI {
		n = maxI
	}
	out := make([]any, 0, n)
	prefix, _ := obj["prefixItems"].([]any)
	for i := 0; i < n; i++ {
		var item any
		switch {
		case i < len(prefix):
			item = p.gen(prefix[i], depth+1)
		case obj["items"] != nil:
			item = p.gen(obj["items"], depth+1)
		default:
			item = json.Number(strconv.Itoa(i))
		}
		out = append(out, item)
	}
	if obj["uniqueItems"] == true {
		// Make items distinct when they are scalars.
		seen := map[string]bool{}
		for i, it := range out {
			key := fmt.Sprint(it)
			if seen[key] {
				out[i] = json.Number(strconv.Itoa(1000 + i))
			}
			seen[key] = true
		}
	}
	return out
}

func (p *payloadGen) genObject(obj map[string]any, depth int) any {
	src := p.src
	out := map[string]any{}
	props, _ := obj["properties"].(map[string]any)
	required := map[string]bool{}
	if req, ok := obj["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				required[s] = true
			}
		}
	}
	names := make([]string, 0, len(props))
	for k := range props {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		if required[name] || src.chance(60) {
			out[name] = p.gen(props[name], depth+1)
		}
	}
	for name := range required {
		if _, ok := out[name]; !ok {
			out[name] = p.anyValue(depth + 1)
		}
	}
	// Extra keys. Names come from a pool the mutator never uses for
	// new properties, so the documented open-model exception cannot be
	// mistaken for a soundness violation.
	ap := obj["additionalProperties"]
	if pp, ok := obj["patternProperties"].(map[string]any); ok && src.chance(50) {
		for pat, sub := range pp {
			if samples := patternCatalog[pat]; len(samples) > 0 {
				out[samples[src.n(len(samples))]] = p.gen(sub, depth+1)
			}
		}
	}
	if ap != false && src.chance(40) {
		p.extra++
		name := "extra_" + strconv.Itoa(p.extra)
		if apSchema, ok := ap.(map[string]any); ok {
			out[name] = p.gen(apSchema, depth+1)
		} else {
			out[name] = p.anyValue(depth + 1)
		}
	}
	return out
}

// ---------------------------------------------------------------------
// Mutator
// ---------------------------------------------------------------------

// mutation describes one change applied to a schema. expectAccept is
// set only for changes the docs table lists as allowed, applied at a
// position the compatibility check follows (properties, items,
// additionalProperties schemas, anyOf/allOf branches, $ref targets)
// and not also reachable through a keyword the check keeps opaque:
// a node that "then" or "not" points at (directly or through $ref)
// may only stay identical, so widening it is a documented rejection.
type mutation struct {
	name         string
	expectAccept bool
}

// mutateSchema returns a deep copy of doc with one mutation applied,
// or ok=false when no mutation fit the chosen position.
func mutateSchema(src *byteSrc, doc map[string]any) (map[string]any, mutation, bool) {
	next := deepCopy(doc).(map[string]any)
	node, ok := descend(src, next, next, 0)
	if !ok {
		return nil, mutation{}, false
	}
	opaque := opaqueReachable(next)
	id, _ := mapID(node)
	m := &mutator{src: src, root: next, newProps: 0}
	var mut mutation
	switch {
	case src.chance(60) && m.tryWiden(node, &mut):
	case m.tryNarrow(node, &mut):
	case m.tryWiden(node, &mut):
	default:
		return nil, mutation{}, false
	}
	if mut.expectAccept && opaque[id] {
		mut.expectAccept = false
		mut.name += " (node reachable through an opaque keyword)"
	}
	// Under draft-04 const is not a keyword: turning it into an enum
	// adds a constraint where there was none, and the check must say so.
	if mut.expectAccept && strings.Contains(mut.name, "const") && next["$schema"] == "http://json-schema.org/draft-04/schema#" {
		mut.expectAccept = false
		mut.name += " (draft-04 ignores const)"
	}
	return next, mut, true
}

func (m *mutator) tryWiden(node map[string]any, out *mutation) bool {
	mut, ok := m.widen(node)
	if ok {
		*out = mut
	}
	return ok
}

func (m *mutator) tryNarrow(node map[string]any, out *mutation) bool {
	mut, ok := m.narrow(node)
	if ok {
		*out = mut
	}
	return ok
}

// opaqueKeywords are the schema-valued keywords the compatibility
// check does not model: whatever they point at (following $ref) must
// stay identical.
var opaqueKeywords = []string{
	"not", "if", "then", "else", "oneOf", "contains", "propertyNames",
	"dependentSchemas", "dependencies", "patternProperties", "prefixItems",
	"unevaluatedProperties", "unevaluatedItems", "contentSchema",
}

// opaqueReachable returns the identities of every object reachable
// from an opaque keyword anywhere in root, following $ref and every
// schema position underneath.
func opaqueReachable(root map[string]any) map[uintptr]bool {
	out := map[uintptr]bool{}
	var mark func(v any, depth int)
	mark = func(v any, depth int) {
		if depth > 200 {
			return
		}
		switch t := v.(type) {
		case map[string]any:
			id, _ := mapID(t)
			if out[id] {
				return
			}
			out[id] = true
			if ref, ok := t["$ref"].(string); ok {
				if target, err := resolvePointer(root, ref); err == nil {
					mark(target, depth+1)
				}
			}
			for _, c := range t {
				mark(c, depth+1)
			}
		case []any:
			for _, c := range t {
				mark(c, depth+1)
			}
		}
	}
	walkSchemaPositions(root, func(node map[string]any) {
		for _, k := range opaqueKeywords {
			if v, ok := node[k]; ok {
				mark(v, 0)
			}
		}
	})
	return out
}

// descend walks from root along supported paths and returns the node
// to mutate. It resolves $ref so that a $defs body can be mutated.
func descend(src *byteSrc, root map[string]any, node any, depth int) (map[string]any, bool) {
	obj, ok := node.(map[string]any)
	if !ok {
		return nil, false
	}
	if ref, ok := obj["$ref"].(string); ok {
		target, err := resolvePointer(root, ref)
		if err != nil || depth > 10 {
			return nil, false
		}
		return descend(src, root, target, depth+1)
	}
	if depth > 8 || src.chance(35) {
		return obj, true
	}
	var paths []any
	if props, ok := obj["properties"].(map[string]any); ok {
		names := make([]string, 0, len(props))
		for k := range props {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, n := range names {
			paths = append(paths, props[n])
		}
	}
	if items, ok := obj["items"].(map[string]any); ok {
		paths = append(paths, items)
	}
	if ap, ok := obj["additionalProperties"].(map[string]any); ok {
		paths = append(paths, ap)
	}
	for _, k := range []string{"anyOf", "allOf"} {
		if br, ok := obj[k].([]any); ok {
			paths = append(paths, br...)
		}
	}
	if len(paths) == 0 {
		return obj, true
	}
	child, ok := descend(src, root, paths[src.n(len(paths))], depth+1)
	if !ok {
		return obj, true
	}
	return child, true
}

type mutator struct {
	src      *byteSrc
	root     map[string]any
	newProps int
}

// widen applies one documented widening. Every case here must be
// accepted by the compatibility check.
func (m *mutator) widen(node map[string]any) (mutation, bool) {
	src := m.src
	ops := []func() (string, bool){
		func() (string, bool) {
			for _, k := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "pattern", "format", "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "enum", "const", "required", "uniqueItems", "items", "oneOf", "not", "if", "then", "else", "contains", "maxContains", "propertyNames", "dependentRequired", "dependentSchemas", "dependencies", "type", "additionalProperties", "allOf", "anyOf"} {
				if _, ok := node[k]; ok && src.chance(50) {
					delete(node, k)
					return "drop " + k, true
				}
			}
			return "", false
		},
		func() (string, bool) {
			if _, ok := node["patternProperties"]; ok && !constrainsAdditional(node) {
				delete(node, "patternProperties")
				return "drop patternProperties (open model)", true
			}
			return "", false
		},
		func() (string, bool) {
			if _, ok := node["prefixItems"]; ok {
				if _, hasItems := node["items"]; !hasItems {
					delete(node, "prefixItems")
					return "drop prefixItems (no items)", true
				}
			}
			return "", false
		},
		func() (string, bool) {
			switch t := node["type"].(type) {
			case string:
				if t == "integer" {
					node["type"] = "number"
					return "integer -> number", true
				}
				node["type"] = []any{t, "null"}
				return "type union add null", true
			case []any:
				node["type"] = append(append([]any{}, t...), "boolean")
				return "type union add boolean", true
			}
			return "", false
		},
		func() (string, bool) {
			for _, k := range []string{"minimum", "exclusiveMinimum"} {
				if r, ok := ratOfAny(node[k]); ok {
					node[k] = json.Number(spellRat(src, new(big.Rat).Sub(r, big.NewRat(int64(1+src.n(5)), 1))))
					return "loosen " + k, true
				}
			}
			for _, k := range []string{"maximum", "exclusiveMaximum"} {
				if r, ok := ratOfAny(node[k]); ok {
					node[k] = json.Number(spellRat(src, new(big.Rat).Add(r, big.NewRat(int64(1+src.n(5)), 1))))
					return "loosen " + k, true
				}
			}
			return "", false
		},
		func() (string, bool) {
			if v, ok := node["exclusiveMinimum"]; ok {
				delete(node, "exclusiveMinimum")
				node["minimum"] = v
				return "exclusiveMinimum -> minimum", true
			}
			if v, ok := node["exclusiveMaximum"]; ok {
				delete(node, "exclusiveMaximum")
				node["maximum"] = v
				return "exclusiveMaximum -> maximum", true
			}
			return "", false
		},
		func() (string, bool) {
			if r, ok := ratOfAny(node["multipleOf"]); ok {
				k := []int64{2, 4, 5, 10}[src.n(4)]
				node["multipleOf"] = json.Number(spellRat(src, new(big.Rat).Quo(r, big.NewRat(k, 1))))
				return "multipleOf -> divisor", true
			}
			return "", false
		},
		func() (string, bool) {
			for _, k := range []string{"minLength", "minItems", "minProperties"} {
				if v, ok := intOf(node[k]); ok && v > 0 {
					node[k] = v - 1
					return "decrease " + k, true
				}
			}
			for _, k := range []string{"maxLength", "maxItems", "maxProperties", "maxContains"} {
				if v, ok := intOf(node[k]); ok {
					node[k] = v + 1 + src.n(3)
					return "increase " + k, true
				}
			}
			return "", false
		},
		func() (string, bool) {
			if e, ok := node["enum"].([]any); ok {
				node["enum"] = append(append([]any{}, e...), "added-"+strconv.Itoa(src.n(100)))
				return "enum add value", true
			}
			if c, ok := node["const"]; ok {
				delete(node, "const")
				node["enum"] = []any{c, "added"}
				return "const -> enum", true
			}
			return "", false
		},
		func() (string, bool) {
			if req, ok := node["required"].([]any); ok && len(req) > 0 {
				i := src.n(len(req))
				node["required"] = append(append([]any{}, req[:i]...), req[i+1:]...)
				return "required drop name", true
			}
			return "", false
		},
		func() (string, bool) {
			if _, isObj := node["properties"]; !isObj && node["type"] != "object" {
				return "", false
			}
			props, _ := node["properties"].(map[string]any)
			if props == nil {
				props = map[string]any{}
				node["properties"] = props
			}
			m.newProps++
			name := "new_" + strconv.Itoa(m.newProps)
			// A name the previous version already constrained (through
			// an additionalProperties schema or a matching
			// patternProperties pattern) may only get a wider schema;
			// true is wider than anything.
			_, apIsSchema := node["additionalProperties"].(map[string]any)
			matched, _ := matchingPatternSchemas(node, name, "")
			if apIsSchema || len(matched) > 0 {
				props[name] = true
			} else {
				props[name] = map[string]any{"type": []string{"string", "integer", "boolean"}[src.n(3)]}
			}
			return "add optional property", true
		},
		func() (string, bool) {
			for _, k := range []string{"minLength", "minItems", "minProperties"} {
				if _, ok := node[k]; !ok && src.chance(50) {
					node[k] = 0
					return k + " appears as 0", true
				}
			}
			return "", false
		},
		func() (string, bool) {
			if node["additionalProperties"] == false {
				switch src.n(3) {
				case 0:
					delete(node, "additionalProperties")
				case 1:
					node["additionalProperties"] = true
				default:
					node["additionalProperties"] = map[string]any{"type": []any{"string", "number", "null", "boolean", "object", "array"}}
				}
				return "additionalProperties false opens", true
			}
			return "", false
		},
		func() (string, bool) {
			if node["uniqueItems"] == true {
				node["uniqueItems"] = false
				return "uniqueItems true -> false", true
			}
			return "", false
		},
		func() (string, bool) {
			if br, ok := node["anyOf"].([]any); ok {
				node["anyOf"] = append(append([]any{}, br...), map[string]any{"type": "null"})
				return "anyOf add branch", true
			}
			return "", false
		},
		func() (string, bool) {
			if br, ok := node["allOf"].([]any); ok && len(br) > 1 {
				node["allOf"] = br[1:]
				return "allOf drop branch", true
			}
			return "", false
		},
		func() (string, bool) {
			// Inline a $ref found in properties.
			props, _ := node["properties"].(map[string]any)
			for _, name := range sortedKeysAny(props) {
				if sub, ok := props[name].(map[string]any); ok {
					// The root is never inlined: its $id would become a
					// nested $id, which the check does not support.
					if ref, ok := sub["$ref"].(string); ok && len(sub) == 1 && ref != "#" {
						target, err := resolvePointer(m.root, ref)
						if err == nil {
							props[name] = deepCopy(target)
							return "inline $ref", true
						}
					}
				}
			}
			return "", false
		},
	}
	start := src.n(len(ops))
	for i := 0; i < len(ops); i++ {
		if name, ok := ops[(start+i)%len(ops)](); ok {
			return mutation{name: "widen: " + name, expectAccept: true}, true
		}
	}
	return mutation{}, false
}

// narrow applies a change that is not in the allowed table. The check
// may accept it only when it is semantically vacuous; the payload
// oracle decides.
func (m *mutator) narrow(node map[string]any) (mutation, bool) {
	src := m.src
	ops := []func() (string, bool){
		func() (string, bool) {
			k := []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"}[src.n(4)]
			node[k] = json.Number(strconv.Itoa(src.n(20) - 10))
			return "set " + k, true
		},
		func() (string, bool) {
			for _, k := range []string{"minimum", "exclusiveMinimum"} {
				if r, ok := ratOfAny(node[k]); ok {
					node[k] = json.Number(spellRat(src, new(big.Rat).Add(r, big.NewRat(1, 1))))
					return "tighten " + k, true
				}
			}
			for _, k := range []string{"maximum", "exclusiveMaximum"} {
				if r, ok := ratOfAny(node[k]); ok {
					node[k] = json.Number(spellRat(src, new(big.Rat).Sub(r, big.NewRat(1, 1))))
					return "tighten " + k, true
				}
			}
			return "", false
		},
		func() (string, bool) {
			// Precision traps: changes that float64 cannot see.
			switch src.n(4) {
			case 0:
				node["minimum"] = json.Number("9007199254740993")
				delete(node, "exclusiveMinimum")
				return "minimum 2^53+1", true
			case 1:
				node["maximum"] = json.Number("9007199254740991")
				delete(node, "exclusiveMaximum")
				return "maximum 2^53-1", true
			case 2:
				node["multipleOf"] = json.Number("0.9999999999")
				return "multipleOf 0.9999999999", true
			default:
				node["multipleOf"] = json.Number("1.0000000001")
				return "multipleOf 1.0000000001", true
			}
		},
		func() (string, bool) {
			switch t := node["type"].(type) {
			case string:
				if t == "number" {
					node["type"] = "integer"
					return "number -> integer", true
				}
				node["type"] = []string{"string", "integer", "object", "array", "null", "boolean"}[src.n(6)]
				return "type change", true
			case []any:
				if len(t) > 1 {
					node["type"] = t[:len(t)-1]
					return "type union drop", true
				}
			case nil:
				node["type"] = []string{"string", "integer", "object", "array", "number"}[src.n(5)]
				return "type added", true
			}
			return "", false
		},
		func() (string, bool) {
			if e, ok := node["enum"].([]any); ok && len(e) > 1 {
				node["enum"] = e[1:]
				return "enum drop value", true
			}
			node["enum"] = []any{"only", json.Number("1"), nil}
			return "enum added", true
		},
		func() (string, bool) {
			props, _ := node["properties"].(map[string]any)
			names := sortedKeysAny(props)
			if len(names) == 0 {
				return "", false
			}
			name := names[src.n(len(names))]
			if src.chance(50) {
				delete(props, name)
				return "property removed", true
			}
			req, _ := node["required"].([]any)
			node["required"] = append(append([]any{}, req...), name)
			return "required added", true
		},
		func() (string, bool) {
			node["additionalProperties"] = []any{false, map[string]any{"type": "string"}}[src.n(2)]
			return "additionalProperties closed", true
		},
		func() (string, bool) {
			node[[]string{"pattern", "format"}[src.n(2)]] = []string{"^[a-z]+$", "email", "^x-", "uuid"}[src.n(4)]
			return "pattern/format set", true
		},
		func() (string, bool) {
			k := []string{"minLength", "minItems", "minProperties", "maxLength", "maxItems", "maxProperties"}[src.n(6)]
			v, _ := intOf(node[k])
			if strings.HasPrefix(k, "min") {
				node[k] = v + 1
			} else {
				node[k] = 1
			}
			return "tighten " + k, true
		},
		func() (string, bool) {
			node["items"] = map[string]any{"type": []string{"string", "integer"}[src.n(2)]}
			return "items set", true
		},
		func() (string, bool) {
			node["uniqueItems"] = true
			return "uniqueItems added", true
		},
		func() (string, bool) {
			if r, ok := ratOfAny(node["multipleOf"]); ok {
				node["multipleOf"] = json.Number(spellRat(src, new(big.Rat).Mul(r, big.NewRat(3, 1))))
				return "multipleOf x3", true
			}
			node["multipleOf"] = json.Number([]string{"2", "0.3", "7"}[src.n(3)])
			return "multipleOf added", true
		},
		func() (string, bool) {
			switch src.n(4) {
			case 0:
				node["not"] = map[string]any{"type": "string"}
				return "not added", true
			case 1:
				node["oneOf"] = []any{map[string]any{"type": "string"}, map[string]any{"type": "integer"}}
				return "oneOf added", true
			case 2:
				node["if"] = map[string]any{"type": "string"}
				node["then"] = map[string]any{"minLength": 1}
				return "if/then added", true
			default:
				node["propertyNames"] = map[string]any{"maxLength": 2}
				return "propertyNames added", true
			}
		},
		func() (string, bool) {
			// Change a $defs body: a keyword that references it through
			// an opaque keyword keeps its text but not its meaning.
			defs, _ := m.root["$defs"].(map[string]any)
			names := sortedKeysAny(defs)
			if len(names) == 0 {
				return "", false
			}
			name := names[src.n(len(names))]
			switch src.n(3) {
			case 0:
				defs[name] = map[string]any{}
			case 1:
				defs[name] = map[string]any{"type": "string"}
			default:
				defs[name] = false
			}
			return "$defs body changed", true
		},
		func() (string, bool) {
			node["patternProperties"] = map[string]any{sortedPatterns[src.n(len(sortedPatterns))]: map[string]any{"type": "integer"}}
			return "patternProperties added", true
		},
		func() (string, bool) {
			// A new property whose name matches a previous pattern.
			pp, _ := node["patternProperties"].(map[string]any)
			for _, pat := range sortedKeysAny(pp) {
				if samples := patternCatalog[pat]; len(samples) > 0 {
					props, _ := node["properties"].(map[string]any)
					if props == nil {
						props = map[string]any{}
						node["properties"] = props
					}
					props[samples[0]] = map[string]any{"type": "integer", "minimum": 100}
					return "new property matching a pattern", true
				}
			}
			return "", false
		},
		func() (string, bool) {
			// Replace a keyword value with an arbitrary JSON value.
			keys := sortedKeysAny(node)
			if len(keys) == 0 {
				return "", false
			}
			k := keys[src.n(len(keys))]
			node[k] = []any{nil, true, json.Number("1"), "s", []any{}, map[string]any{}}[src.n(6)]
			return "garbage value for " + k, true
		},
	}
	start := src.n(len(ops))
	for i := 0; i < len(ops); i++ {
		if name, ok := ops[(start+i)%len(ops)](); ok {
			return mutation{name: "narrow: " + name}, true
		}
	}
	return mutation{}, false
}

func sortedKeysAny(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, c := range t {
			out[k] = deepCopy(c)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, c := range t {
			out[i] = deepCopy(c)
		}
		return out
	default:
		return v
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
