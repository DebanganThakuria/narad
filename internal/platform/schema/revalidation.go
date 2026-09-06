package schema

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// checkRevalidation refuses, at registration, a schema whose validation
// time is exponential in payload nesting depth.
//
// The validator has no memoisation: a value is validated once per
// applicator path that reaches it. A recursive schema in which some
// subschema S reaches itself through two different paths that select
// the same child (allOf of two branches that each carry items:
// {"$ref": "#"}, or anyOf listing the same recursive $ref twice) then
// validates every nested value twice per level, four times two levels
// down, and so on: a 21-byte payload nested ten deep against four such
// branches took 1.2 s and 1.7 GiB, and every extra level multiplies
// that by four. Ordinary recursive schemas (a tree whose children sit
// under different property names, a JSON-value schema that recurses
// through items for arrays and additionalProperties for objects) reach
// each value once and are unaffected.
//
// The analysis builds the schema graph (subschema objects as nodes;
// "epsilon" edges for keywords that apply a subschema to the same value
// and "descending" edges, labelled with the child they select, for the
// ones that apply it to a child), then walks pairs of nodes inside each
// strongly connected component: a pair walk from (S, S) back to (S, S)
// that at some step takes two different edges with compatible labels
// is two distinct validation paths for one value per lap, which is the
// exponential case. Labels are compared conservatively (a pattern is
// assumed to match anything it is not provably disjoint from), so the
// check can refuse a schema that would in fact be fine, never the
// reverse; unresolvable references are ignored because the compiler
// has already rejected them.
func checkRevalidation(doc any) error {
	g := buildSchemaGraph(doc)
	if len(g.nodes) == 0 {
		return nil
	}
	steps := 0
	for _, comp := range g.components() {
		if len(comp) == 1 && !g.hasSelfEdge(comp[0]) {
			continue
		}
		if err := g.checkComponent(comp, &steps); err != nil {
			return err
		}
	}
	return nil
}

// maxRevalidationSteps bounds the analysis itself: pair states and
// transitions generated across all components. Real schemas stay far
// below it; a document that exhausts it is refused as too complex to
// analyse, which fails closed.
const maxRevalidationSteps = 2_000_000

var errRevalidationBudget = fmt.Errorf("schema is too intricate for the compatibility of its recursive references to be analysed; simplify the recursion")

type selectorKind byte

const (
	selArray  selectorKind = 'a'
	selObject selectorKind = 'o'
)

// selector describes which child of a value a descending edge applies
// to. For arrays: every index (star), one index, or every index from a
// bound. For objects: every name (star), one name, names matching a
// pattern, or every name not claimed by the node's properties and
// patternProperties (additional).
type selector struct {
	kind selectorKind
	star bool
	// arrays
	idx, from int
	exact     bool
	// objects
	name         string
	hasName      bool
	pattern      *regexp.Regexp // nil with hasPattern set: assume it matches anything
	hasPattern   bool
	additional   bool
	exclNames    map[string]bool
	exclPatterns []*regexp.Regexp
}

func (s selector) compatible(o selector) bool {
	if s.kind != o.kind {
		return false
	}
	if s.star || o.star {
		return true
	}
	if s.kind == selArray {
		switch {
		case s.exact && o.exact:
			return s.idx == o.idx
		case s.exact:
			return s.idx >= o.from
		case o.exact:
			return o.idx >= s.from
		default:
			return true
		}
	}
	// Objects: a name is disjoint from a pattern it does not match and
	// from an "additional" that excludes it; everything else may overlap.
	nameVs := func(name string, other selector) bool {
		switch {
		case other.hasName:
			return name == other.name
		case other.hasPattern:
			return other.pattern == nil || other.pattern.MatchString(name)
		case other.additional:
			if other.exclNames[name] {
				return false
			}
			for _, re := range other.exclPatterns {
				if re.MatchString(name) {
					return false
				}
			}
			return true
		}
		return true
	}
	switch {
	case s.hasName:
		return nameVs(s.name, o)
	case o.hasName:
		return nameVs(o.name, s)
	default:
		return true
	}
}

type descEdge struct {
	to  int
	sel selector
	kw  string // keyword path of the edge within the source node, for messages
}

type epsEdge struct {
	to int
	kw string
}

type graphNode struct {
	obj  map[string]any
	path string
	eps  []epsEdge
	desc []descEdge
}

type schemaGraph struct {
	nodes         []*graphNode
	ids           map[uintptr]int
	anchors       map[string][]int
	dynAnchors    map[string][]int
	resourceRoots []int
	recAnchors    []int
	root          any
}

// buildSchemaGraph collects every subschema object in a schema
// position (pass one, which also indexes anchors and resource roots)
// and then adds the edges (pass two, so references can point forward).
func buildSchemaGraph(doc any) *schemaGraph {
	g := &schemaGraph{ids: map[uintptr]int{}, anchors: map[string][]int{}, dynAnchors: map[string][]int{}, root: doc}
	g.collect(doc, "", 0)
	for id, n := range g.nodes {
		g.addEdges(id, n)
	}
	return g
}

func (g *schemaGraph) collect(v any, path string, depth int) {
	obj, ok := v.(map[string]any)
	if !ok || depth > MaxSchemaDepth {
		return
	}
	mid, _ := mapID(obj)
	if _, seen := g.ids[mid]; seen {
		return
	}
	id := len(g.nodes)
	g.ids[mid] = id
	g.nodes = append(g.nodes, &graphNode{obj: obj, path: path})
	if _, ok := obj["$id"].(string); ok || path == "" {
		g.resourceRoots = append(g.resourceRoots, id)
	}
	if a, ok := obj["$anchor"].(string); ok {
		g.anchors[a] = append(g.anchors[a], id)
	}
	if a, ok := obj["$dynamicAnchor"].(string); ok {
		g.dynAnchors[a] = append(g.dynAnchors[a], id)
	}
	if obj["$recursiveAnchor"] == true {
		g.recAnchors = append(g.recAnchors, id)
	}
	for k, child := range obj {
		switch k {
		case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas", "dependencies":
			if m, ok := child.(map[string]any); ok {
				for name, sub := range m {
					g.collect(sub, path+"/"+k+"/"+name, depth+1)
				}
			}
		case "allOf", "anyOf", "oneOf", "prefixItems", "items":
			if arr, ok := child.([]any); ok {
				for i, sub := range arr {
					g.collect(sub, path+"/"+k+"/"+strconv.Itoa(i), depth+1)
				}
			} else {
				g.collect(child, path+"/"+k, depth+1)
			}
		case "additionalProperties", "additionalItems", "not", "if", "then", "else", "contains",
			"propertyNames", "unevaluatedProperties", "unevaluatedItems":
			g.collect(child, path+"/"+k, depth+1)
		}
	}
}

func (g *schemaGraph) idOf(v any) (int, bool) {
	obj, ok := v.(map[string]any)
	if !ok {
		return 0, false
	}
	mid, _ := mapID(obj)
	id, ok := g.ids[mid]
	return id, ok
}

func (g *schemaGraph) addEdges(id int, n *graphNode) {
	obj := n.obj
	eps := func(v any, kw string) {
		if t, ok := g.idOf(v); ok {
			n.eps = append(n.eps, epsEdge{to: t, kw: kw})
		}
	}
	desc := func(v any, sel selector, kw string) {
		if t, ok := g.idOf(v); ok {
			n.desc = append(n.desc, descEdge{to: t, sel: sel, kw: kw})
		}
	}
	for _, k := range []string{"allOf", "anyOf", "oneOf"} {
		if arr, ok := obj[k].([]any); ok {
			for i, sub := range arr {
				eps(sub, k+"/"+strconv.Itoa(i))
			}
		}
	}
	for _, k := range []string{"not", "if", "then", "else"} {
		if sub, ok := obj[k]; ok {
			eps(sub, k)
		}
	}
	for _, k := range []string{"dependentSchemas", "dependencies"} {
		if m, ok := obj[k].(map[string]any); ok {
			for name, sub := range m {
				eps(sub, k+"/"+name)
			}
		}
	}
	if ref, ok := obj["$ref"].(string); ok {
		for _, t := range g.refTargets(ref) {
			n.eps = append(n.eps, epsEdge{to: t, kw: "$ref"})
		}
	}
	if ref, ok := obj["$dynamicRef"].(string); ok {
		name := strings.TrimPrefix(ref, "#")
		for _, t := range g.dynAnchors[name] {
			n.eps = append(n.eps, epsEdge{to: t, kw: "$dynamicRef"})
		}
		for _, t := range g.refTargets(ref) {
			n.eps = append(n.eps, epsEdge{to: t, kw: "$dynamicRef"})
		}
	}
	if _, ok := obj["$recursiveRef"].(string); ok {
		for _, t := range g.resourceRoots {
			n.eps = append(n.eps, epsEdge{to: t, kw: "$recursiveRef"})
		}
		for _, t := range g.recAnchors {
			n.eps = append(n.eps, epsEdge{to: t, kw: "$recursiveRef"})
		}
	}

	// Arrays.
	prefixLen := 0
	if arr, ok := obj["prefixItems"].([]any); ok {
		prefixLen = len(arr)
		for i, sub := range arr {
			desc(sub, selector{kind: selArray, idx: i, exact: true}, "prefixItems/"+strconv.Itoa(i))
		}
	}
	switch items := obj["items"].(type) {
	case []any: // draft-07 tuple form
		for i, sub := range items {
			desc(sub, selector{kind: selArray, idx: i, exact: true}, "items/"+strconv.Itoa(i))
		}
		desc(obj["additionalItems"], selector{kind: selArray, from: len(items)}, "additionalItems")
	case map[string]any:
		desc(items, selector{kind: selArray, from: prefixLen}, "items")
	}
	for _, k := range []string{"contains", "unevaluatedItems"} {
		desc(obj[k], selector{kind: selArray, star: true}, k)
	}

	// Objects. propertyNames applies to key strings, which have no
	// children, so no cycle can pass through it; it adds no edge.
	var exclNames map[string]bool
	var exclPatterns []*regexp.Regexp
	if props, ok := obj["properties"].(map[string]any); ok {
		exclNames = make(map[string]bool, len(props))
		names := make([]string, 0, len(props))
		for name := range props {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			exclNames[name] = true
			desc(props[name], selector{kind: selObject, name: name, hasName: true}, "properties/"+name)
		}
	}
	if pp, ok := obj["patternProperties"].(map[string]any); ok {
		pats := make([]string, 0, len(pp))
		for p := range pp {
			pats = append(pats, p)
		}
		slices.Sort(pats)
		for _, p := range pats {
			re, err := regexp.Compile(p)
			if err != nil {
				re = nil
			} else {
				exclPatterns = append(exclPatterns, re)
			}
			desc(pp[p], selector{kind: selObject, pattern: re, hasPattern: true}, "patternProperties/"+p)
		}
	}
	desc(obj["additionalProperties"], selector{kind: selObject, additional: true, exclNames: exclNames, exclPatterns: exclPatterns}, "additionalProperties")
	desc(obj["unevaluatedProperties"], selector{kind: selObject, star: true}, "unevaluatedProperties")
}

// refTargets resolves an in-document reference to every node it could
// mean: "#" is the enclosing resource root (any of them, since the
// analysis does not track resource scope), "#name" an anchor, and
// "#/pointer" a pointer resolved against the document and against
// every nested resource root.
func (g *schemaGraph) refTargets(ref string) []int {
	var out []int
	switch {
	case ref == "#":
		out = append(out, g.resourceRoots...)
	case strings.HasPrefix(ref, "#/"):
		if t, err := resolvePointer(g.root, ref); err == nil {
			if id, ok := g.idOf(t); ok {
				out = append(out, id)
			}
		}
		for _, r := range g.resourceRoots {
			if r == 0 {
				continue
			}
			if t, err := resolvePointer(g.nodes[r].obj, ref); err == nil {
				if id, ok := g.idOf(t); ok {
					out = append(out, id)
				}
			}
		}
	default:
		out = append(out, g.anchors[strings.TrimPrefix(ref, "#")]...)
	}
	return out
}

func (g *schemaGraph) hasSelfEdge(id int) bool {
	n := g.nodes[id]
	for _, e := range n.eps {
		if e.to == id {
			return true
		}
	}
	for _, e := range n.desc {
		if e.to == id {
			return true
		}
	}
	return false
}

// components returns the strongly connected components of the schema
// graph (Tarjan, iterative).
func (g *schemaGraph) components() [][]int {
	n := len(g.nodes)
	index := make([]int, n)
	low := make([]int, n)
	onStack := make([]bool, n)
	for i := range index {
		index[i] = -1
	}
	var stack []int
	var comps [][]int
	next := 0
	succ := func(id int) []int {
		var out []int
		for _, e := range g.nodes[id].eps {
			out = append(out, e.to)
		}
		for _, e := range g.nodes[id].desc {
			out = append(out, e.to)
		}
		return out
	}
	type frame struct {
		id   int
		succ []int
		i    int
	}
	for start := range g.nodes {
		if index[start] >= 0 {
			continue
		}
		frames := []frame{{id: start, succ: succ(start)}}
		index[start], low[start] = next, next
		next++
		stack = append(stack, start)
		onStack[start] = true
		for len(frames) > 0 {
			f := &frames[len(frames)-1]
			if f.i < len(f.succ) {
				w := f.succ[f.i]
				f.i++
				if index[w] < 0 {
					index[w], low[w] = next, next
					next++
					stack = append(stack, w)
					onStack[w] = true
					frames = append(frames, frame{id: w, succ: succ(w)})
				} else if onStack[w] && index[w] < low[f.id] {
					low[f.id] = index[w]
				}
				continue
			}
			if low[f.id] == index[f.id] {
				var comp []int
				for {
					w := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[w] = false
					comp = append(comp, w)
					if w == f.id {
						break
					}
				}
				comps = append(comps, comp)
			}
			done := *f
			frames = frames[:len(frames)-1]
			if len(frames) > 0 {
				p := &frames[len(frames)-1]
				if low[done.id] < low[p.id] {
					low[p.id] = low[done.id]
				}
			}
		}
	}
	return comps
}

// nonMonotone are the applicators whose outcome can flip when the
// subschema they apply fails: a reference-cycle failure under them
// becomes a pass.
var nonMonotone = map[string]bool{"not": true, "if": true, "oneOf": true}

// checkEpsilonCycles refuses a cycle that applies a subschema to the
// value already being validated (no descent into a child) when the
// cycle runs through not, if or oneOf. The validator fails such a
// cycle at the node it visits twice, so where the failure lands
// depends on where the cycle was entered; under a monotone applicator
// (allOf, anyOf, then, else, dependentSchemas) that never changes the
// outcome, but "not" turns the failure into a pass, and the same
// subschema then accepts different values depending on whether it is
// reached directly or through a copy. The compatibility check treats
// a $ref and its inlined target as the same schema, which is only
// true without such cycles.
func (g *schemaGraph) checkEpsilonCycles(comp []int, inComp map[int]bool, steps *int) error {
	reaches := func(from, to int) (bool, error) {
		seen := map[int]bool{from: true}
		stack := []int{from}
		for len(stack) > 0 {
			v := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, e := range g.nodes[v].eps {
				*steps++
				if *steps > maxRevalidationSteps {
					return false, errRevalidationBudget
				}
				if e.to == to {
					return true, nil
				}
				if inComp[e.to] && !seen[e.to] {
					seen[e.to] = true
					stack = append(stack, e.to)
				}
			}
		}
		return false, nil
	}
	for _, u := range comp {
		for _, e := range g.nodes[u].eps {
			kw := e.kw
			if i := strings.IndexByte(kw, '/'); i >= 0 {
				kw = kw[:i]
			}
			if !nonMonotone[kw] || !inComp[e.to] {
				continue
			}
			back, err := reaches(e.to, u)
			if err != nil {
				return err
			}
			if back {
				return fmt.Errorf("at %s/%s: %q applies a subschema that refers back to the value already being validated; the validator reports a reference cycle there and its outcome would depend on where the cycle is entered", g.nodes[u].path, e.kw, kw)
			}
		}
	}
	return nil
}

// macroEdge is one way of getting from a node to a child value: an
// epsilon path (possibly empty) to a node that carries a descending
// edge, then that edge. count is how many distinct epsilon paths lead
// to the same edge, capped at 2.
type macroEdge struct {
	via   int // node carrying the descending edge
	edge  int // index into that node's desc
	to    int
	sel   selector
	count int
}

// macroEdges enumerates the macro edges from u that stay inside comp.
// Epsilon paths that revisit a node are the validator's reference
// cycle (a failure), so only simple paths count.
func (g *schemaGraph) macroEdges(u int, inComp map[int]bool, steps *int) ([]macroEdge, error) {
	counts := map[int]int{}
	onPath := map[int]bool{}
	var dfs func(v int) error
	dfs = func(v int) error {
		*steps++
		if *steps > maxRevalidationSteps {
			return errRevalidationBudget
		}
		if counts[v] < 2 {
			counts[v]++
		}
		onPath[v] = true
		for _, e := range g.nodes[v].eps {
			if !inComp[e.to] || onPath[e.to] {
				continue
			}
			if err := dfs(e.to); err != nil {
				return err
			}
		}
		delete(onPath, v)
		return nil
	}
	if err := dfs(u); err != nil {
		return nil, err
	}
	var out []macroEdge
	vias := make([]int, 0, len(counts))
	for v := range counts {
		vias = append(vias, v)
	}
	slices.Sort(vias)
	for _, via := range vias {
		for i, e := range g.nodes[via].desc {
			if inComp[e.to] {
				out = append(out, macroEdge{via: via, edge: i, to: e.to, sel: e.sel, count: counts[via]})
			}
		}
	}
	return out, nil
}

// checkComponent runs the pair analysis on one strongly connected
// component.
func (g *schemaGraph) checkComponent(comp []int, steps *int) error {
	inComp := make(map[int]bool, len(comp))
	for _, id := range comp {
		inComp[id] = true
	}
	if err := g.checkEpsilonCycles(comp, inComp, steps); err != nil {
		return err
	}
	macros := map[int][]macroEdge{}
	for _, id := range comp {
		m, err := g.macroEdges(id, inComp, steps)
		if err != nil {
			return err
		}
		macros[id] = m
	}

	type pair struct{ u, v int }
	norm := func(u, v int) pair {
		if u > v {
			u, v = v, u
		}
		return pair{u, v}
	}
	type transition struct {
		to       pair
		distinct bool
		m1, m2   macroEdge
	}
	successors := func(p pair) ([]transition, error) {
		var out []transition
		for _, m1 := range macros[p.u] {
			for _, m2 := range macros[p.v] {
				*steps++
				if *steps > maxRevalidationSteps {
					return nil, errRevalidationBudget
				}
				if !m1.sel.compatible(m2.sel) {
					continue
				}
				same := m1.via == m2.via && m1.edge == m2.edge
				distinct := p.u != p.v || !same || m1.count >= 2
				out = append(out, transition{to: norm(m1.to, m2.to), distinct: distinct, m1: m1, m2: m2})
			}
		}
		return out, nil
	}

	// Tarjan over the pair graph, reached from the diagonal states.
	index := map[pair]int{}
	low := map[pair]int{}
	onStack := map[pair]bool{}
	comp2 := map[pair]int{} // pair -> pair-component number
	var stack []pair
	next, ncomp := 0, 0
	type frame struct {
		p    pair
		succ []transition
		i    int
	}
	var edges []struct {
		from pair
		t    transition
	}
	for _, s := range comp {
		start := pair{s, s}
		if _, seen := index[start]; seen {
			continue
		}
		succ, err := successors(start)
		if err != nil {
			return err
		}
		frames := []frame{{p: start, succ: succ}}
		index[start], low[start] = next, next
		next++
		stack = append(stack, start)
		onStack[start] = true
		for len(frames) > 0 {
			f := &frames[len(frames)-1]
			if f.i < len(f.succ) {
				t := f.succ[f.i]
				f.i++
				if t.distinct {
					edges = append(edges, struct {
						from pair
						t    transition
					}{f.p, t})
				}
				if _, seen := index[t.to]; !seen {
					index[t.to], low[t.to] = next, next
					next++
					stack = append(stack, t.to)
					onStack[t.to] = true
					succ, err := successors(t.to)
					if err != nil {
						return err
					}
					frames = append(frames, frame{p: t.to, succ: succ})
				} else if onStack[t.to] && index[t.to] < low[f.p] {
					low[f.p] = index[t.to]
				}
				continue
			}
			if low[f.p] == index[f.p] {
				for {
					w := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[w] = false
					comp2[w] = ncomp
					if w == f.p {
						break
					}
				}
				ncomp++
			}
			done := *f
			frames = frames[:len(frames)-1]
			if len(frames) > 0 {
				p := &frames[len(frames)-1]
				if low[done.p] < low[p.p] {
					low[p.p] = low[done.p]
				}
			}
		}
	}
	// A distinct transition inside a pair component that also holds a
	// diagonal state closes two different validation paths per lap.
	diagonal := map[int]bool{}
	for _, s := range comp {
		if c, ok := comp2[pair{s, s}]; ok {
			diagonal[c] = true
		}
	}
	for _, e := range edges {
		c, ok := comp2[e.from]
		if !ok || comp2[e.t.to] != c || !diagonal[c] {
			continue
		}
		a := g.nodes[e.t.m1.via].path + "/" + g.nodes[e.t.m1.via].desc[e.t.m1.edge].kw
		b := g.nodes[e.t.m2.via].path + "/" + g.nodes[e.t.m2.via].desc[e.t.m2.edge].kw
		if a == b {
			return fmt.Errorf("schema validates the same value through more than one path per nesting level (%s is reached through two references); validation time would grow exponentially with payload depth", a)
		}
		return fmt.Errorf("schema validates the same value through more than one path per nesting level (%s and %s); validation time would grow exponentially with payload depth", a, b)
	}
	return nil
}
