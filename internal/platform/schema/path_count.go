package schema

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// maxValidationPaths is the most validation paths through which a
// schema may apply one subschema to the same value. The validator does
// not memoise, so every path is paid in full: an acyclic chain of
// $defs where each level applies the next one twice (allOf, anyOf and
// oneOf all qualify) reaches its leaf through 2^levels paths, and a
// 22-level, 1.4 KB chain took 2 s to validate the string "x". 64 still
// admits a union of 64 branches that each reference one shared base.
const maxValidationPaths = 64

// pathCountCap saturates path counts and per-value work, far above any
// bound they are compared with.
const pathCountCap = 1 << 20

// costError is a registration refusal for a schema that would be
// too costly to validate against. It is the request's to fix: the
// topic manager reports it as an invalid request (400).
type costError struct{ msg string }

func (e *costError) Error() string { return e.msg }

var errPathBudget = &costError{msg: "schema is too intricate for its validation cost to be analysed; simplify how its subschemas reference each other"}

// pathSeed is a subschema and the number of validation paths that
// apply it to the current value.
type pathSeed struct {
	node int
	n    int
}

// pathCounter counts, for every value a payload can hold, how many
// validation paths apply each subschema to it. It walks the schema
// graph the way the validator walks a payload: from the set of
// subschemas applied to a value (with their path counts), the
// epsilon edges (allOf, anyOf, oneOf, not, if, then, else,
// dependentSchemas, $ref and its dynamic forms) add the subschemas that
// apply to the same value, and each class of child (a property name,
// an array index, a key) selects the descending edges whose selectors
// can match it. Selectors are matched conservatively (a pattern is
// assumed to match any name it is not tested against, and every
// "other" name to meet every pattern and additionalProperties), so the
// count can only be too high.
//
// Two paths into the same subschema only add up when they apply it to
// the same value, which is what keeps a shared definition used under
// many property names at one path per value. Within a cycle of epsilon
// edges only simple paths count: the validator stops a path that
// revisits a subschema for the same value (a reference cycle).
type pathCounter struct {
	g     *schemaGraph
	steps int

	// Strongly connected components of the epsilon edges alone.
	comp    []int   // node -> component
	rank    []int   // component -> topological rank, sources first
	members [][]int // component -> nodes
	cyclic  []bool  // component has more than one node
	within  map[[2]int]map[int]int

	fanout int // most subschema applications one value receives
}

// countValidationPaths returns the most subschema applications any one
// value receives (saturated), or a refusal when some subschema is
// applied to one value through more than maxValidationPaths paths, or
// when the analysis exceeds its budget.
func countValidationPaths(g *schemaGraph) (int, error) {
	if len(g.nodes) == 0 {
		return 1, nil
	}
	c := &pathCounter{g: g, within: map[[2]int]map[int]int{}}
	c.epsComponents()
	if err := c.walk(); err != nil {
		return pathCountCap, err
	}
	return max(c.fanout, 1), nil
}

func (c *pathCounter) spend(n int) error {
	c.steps += n
	if c.steps > maxRevalidationSteps {
		return errPathBudget
	}
	return nil
}

// epsComponents runs Tarjan's algorithm (iteratively) over the epsilon
// edges and ranks the components topologically.
func (c *pathCounter) epsComponents() {
	n := len(c.g.nodes)
	index := make([]int, n)
	low := make([]int, n)
	onStack := make([]bool, n)
	for i := range index {
		index[i] = -1
	}
	c.comp = make([]int, n)
	var stack []int
	next := 0
	type frame struct{ id, i int }
	for start := range c.g.nodes {
		if index[start] >= 0 {
			continue
		}
		frames := []frame{{id: start}}
		index[start], low[start] = next, next
		next++
		stack = append(stack, start)
		onStack[start] = true
		for len(frames) > 0 {
			f := &frames[len(frames)-1]
			if eps := c.g.nodes[f.id].eps; f.i < len(eps) {
				w := eps[f.i].to
				f.i++
				if index[w] < 0 {
					index[w], low[w] = next, next
					next++
					stack = append(stack, w)
					onStack[w] = true
					frames = append(frames, frame{id: w})
				} else if onStack[w] && index[w] < low[f.id] {
					low[f.id] = index[w]
				}
				continue
			}
			if low[f.id] == index[f.id] {
				id := len(c.members)
				var members []int
				for {
					w := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[w] = false
					c.comp[w] = id
					members = append(members, w)
					if w == f.id {
						break
					}
				}
				c.members = append(c.members, members)
				c.cyclic = append(c.cyclic, len(members) > 1)
			}
			done := frames[len(frames)-1].id
			frames = frames[:len(frames)-1]
			if len(frames) > 0 {
				p := &frames[len(frames)-1]
				low[p.id] = min(low[p.id], low[done])
			}
		}
	}
	// Tarjan emits a component after every component it reaches.
	c.rank = make([]int, len(c.members))
	for i := range c.members {
		c.rank[i] = len(c.members) - 1 - i
	}
}

func satAdd(a, b int) int { return min(a+b, pathCountCap) }

func satMul(a, b int) int {
	if a == 0 || b == 0 {
		return 0
	}
	if a > pathCountCap/b {
		return pathCountCap
	}
	return min(a*b, pathCountCap)
}

func (c *pathCounter) tooManyPaths(node, n int) error {
	where := "#" + c.g.nodes[node].path
	return &costError{msg: fmt.Sprintf("schema applies %s to the same value through more than %d validation paths (%s); validation work multiplies with every path, so reference it from fewer places, for example one allOf next to a oneOf instead of one reference per branch",
		where, maxValidationPaths, pathCountText(n))}
}

func pathCountText(n int) string {
	if n >= pathCountCap {
		return "over a million"
	}
	return strconv.Itoa(n)
}

// simplePaths counts the simple epsilon paths from entry to every node
// of entry's (cyclic) component, staying inside it.
func (c *pathCounter) simplePaths(entry int) (map[int]int, error) {
	id := c.comp[entry]
	key := [2]int{id, entry}
	if p, ok := c.within[key]; ok {
		return p, nil
	}
	p := map[int]int{}
	onPath := map[int]bool{}
	var dfs func(v int) error
	dfs = func(v int) error {
		if err := c.spend(1); err != nil {
			return err
		}
		p[v]++
		if p[v] > maxValidationPaths {
			return c.tooManyPaths(v, p[v])
		}
		onPath[v] = true
		for _, e := range c.g.nodes[v].eps {
			if c.comp[e.to] == id && !onPath[e.to] {
				if err := dfs(e.to); err != nil {
					return err
				}
			}
		}
		delete(onPath, v)
		return nil
	}
	if err := dfs(entry); err != nil {
		return nil, err
	}
	c.within[key] = p
	return p, nil
}

// closure applies the epsilon edges to seeds and returns the path
// count of every subschema applied to the value.
func (c *pathCounter) closure(seeds []pathSeed) (map[int]int, error) {
	arrivals := map[int]int{}
	seen := map[int]bool{}
	var stack []int
	for _, s := range seeds {
		arrivals[s.node] = satAdd(arrivals[s.node], s.n)
		if !seen[s.node] {
			seen[s.node] = true
			stack = append(stack, s.node)
		}
	}
	compSeen := map[int]bool{}
	var comps []int
	for len(stack) > 0 {
		v := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if !compSeen[c.comp[v]] {
			compSeen[c.comp[v]] = true
			comps = append(comps, c.comp[v])
		}
		eps := c.g.nodes[v].eps
		if err := c.spend(1 + len(eps)); err != nil {
			return nil, err
		}
		for _, e := range eps {
			if !seen[e.to] {
				seen[e.to] = true
				stack = append(stack, e.to)
			}
		}
	}
	slices.SortFunc(comps, func(a, b int) int { return c.rank[a] - c.rank[b] })

	counts := make(map[int]int, len(seen))
	for _, id := range comps {
		members := c.members[id]
		if c.cyclic[id] {
			for _, entry := range members {
				in := arrivals[entry]
				if in == 0 {
					continue
				}
				paths, err := c.simplePaths(entry)
				if err != nil {
					return nil, err
				}
				for m, k := range paths {
					counts[m] = satAdd(counts[m], satMul(in, k))
				}
			}
		} else {
			counts[members[0]] = arrivals[members[0]]
		}
		for _, v := range members {
			n := counts[v]
			if n > maxValidationPaths {
				return nil, c.tooManyPaths(v, n)
			}
			for _, e := range c.g.nodes[v].eps {
				if c.comp[e.to] != id {
					arrivals[e.to] = satAdd(arrivals[e.to], n)
				}
			}
		}
	}
	return counts, nil
}

// pathState is a set of seeds waiting for its closure. terminal is
// set for a property name (propertyNames applies to the key, a string,
// which has no children).
type pathState struct {
	seeds    []pathSeed
	terminal bool
}

func pathStateKey(s pathState) string {
	var b strings.Builder
	if s.terminal {
		b.WriteByte('k')
	}
	for _, sd := range s.seeds {
		b.WriteString(strconv.Itoa(sd.node))
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(sd.n))
		b.WriteByte(',')
	}
	return b.String()
}

func seedsOf(m map[int]int) []pathSeed {
	out := make([]pathSeed, 0, len(m))
	for node, n := range m {
		if n > 0 {
			out = append(out, pathSeed{node, n})
		}
	}
	slices.SortFunc(out, func(a, b pathSeed) int { return a.node - b.node })
	return out
}

// walk explores every class of value reachable from the root.
func (c *pathCounter) walk() error {
	start := pathState{seeds: []pathSeed{{node: 0, n: 1}}}
	seen := map[string]bool{pathStateKey(start): true}
	queue := []pathState{start}
	for len(queue) > 0 {
		st := queue[0]
		queue = queue[1:]
		counts, err := c.closure(st.seeds)
		if err != nil {
			return err
		}
		work := 0
		for _, n := range counts {
			work = satAdd(work, n)
		}
		c.fanout = max(c.fanout, work)
		if st.terminal {
			continue
		}
		next, err := c.children(counts)
		if err != nil {
			return err
		}
		for _, ns := range next {
			if k := pathStateKey(ns); !seen[k] {
				seen[k] = true
				queue = append(queue, ns)
			}
		}
	}
	return nil
}

type pathEdge struct {
	sel selector
	to  int
	n   int
}

// children returns the states of the children of a value to which
// counts apply: one per property name some subschema names, one for
// any other name, one per distinct array position class, and one for
// the property names themselves.
func (c *pathCounter) children(counts map[int]int) ([]pathState, error) {
	named := map[string][]pathEdge{}
	var otherObj, arr []pathEdge
	keys := map[int]int{}
	nodes := make([]int, 0, len(counts))
	for v := range counts {
		nodes = append(nodes, v)
	}
	slices.Sort(nodes)
	for _, v := range nodes {
		n := counts[v]
		node := c.g.nodes[v]
		if err := c.spend(1 + len(node.desc) + len(node.keys)); err != nil {
			return nil, err
		}
		for _, e := range node.desc {
			pe := pathEdge{sel: e.sel, to: e.to, n: n}
			switch {
			case e.sel.kind == selArray:
				arr = append(arr, pe)
			case e.sel.hasName:
				named[e.sel.name] = append(named[e.sel.name], pe)
			default:
				otherObj = append(otherObj, pe)
			}
		}
		for _, to := range node.keys {
			keys[to] = satAdd(keys[to], n)
		}
	}

	var out []pathState
	add := func(m map[int]int, terminal bool) {
		if len(m) > 0 {
			out = append(out, pathState{seeds: seedsOf(m), terminal: terminal})
		}
	}
	names := make([]string, 0, len(named))
	for name := range named {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		m := map[int]int{}
		for _, e := range named[name] {
			m[e.to] = satAdd(m[e.to], e.n)
		}
		probe := selector{kind: selObject, name: name, hasName: true}
		if err := c.spend(len(otherObj)); err != nil {
			return nil, err
		}
		for _, e := range otherObj {
			if probe.compatible(e.sel) {
				m[e.to] = satAdd(m[e.to], e.n)
			}
		}
		add(m, false)
	}
	// A name no properties keyword lists: every pattern, star and
	// additionalProperties edge is assumed to apply.
	other := map[int]int{}
	for _, e := range otherObj {
		other[e.to] = satAdd(other[e.to], e.n)
	}
	add(other, false)

	// Array positions: the classes change only at an exact index or at
	// the first index an items keyword covers.
	bounds := []int{0}
	exact := map[int][]pathEdge{}
	var spans []pathEdge // star, or every index from a bound
	for _, e := range arr {
		switch {
		case e.sel.exact:
			exact[e.sel.idx] = append(exact[e.sel.idx], e)
			bounds = append(bounds, e.sel.idx, e.sel.idx+1)
		case e.sel.star:
			spans = append(spans, e)
		default:
			spans = append(spans, e)
			bounds = append(bounds, e.sel.from)
		}
	}
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)
	if len(arr) > 0 {
		if err := c.spend(len(bounds) * (1 + len(spans))); err != nil {
			return nil, err
		}
		for _, j := range bounds {
			m := map[int]int{}
			for _, e := range exact[j] {
				m[e.to] = satAdd(m[e.to], e.n)
			}
			for _, e := range spans {
				if e.sel.star || e.sel.from <= j {
					m[e.to] = satAdd(m[e.to], e.n)
				}
			}
			add(m, false)
		}
	}
	add(keys, true)
	return out, nil
}
