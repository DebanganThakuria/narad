package schema

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// dagSchema is an acyclic chain: d_i applies d_{i+1} twice through kw,
// so the leaf is reached by 2^levels validation paths while no
// reference is recursive.
func dagSchema(levels int, kw string) string {
	var b strings.Builder
	b.WriteString(`{"$ref":"#/$defs/d0","$defs":{`)
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&b, `"d%d":{"%s":[{"$ref":"#/$defs/d%d"},{"$ref":"#/$defs/d%d"}]},`, i, kw, i+1, i+1)
	}
	fmt.Fprintf(&b, `"d%d":{"type":"string"}}}`, levels)
	return b.String()
}

// rerooted swaps the root of a dagSchema document ("$ref" to d0) for
// root, which must end in a comma before "$defs".
func rerooted(dag, root string) string {
	return strings.Replace(dag, `{"$ref":"#/$defs/d0",`, root, 1)
}

// TestAcyclicRefChainIsRefused: a 22-level chain (1.4 KB) registered in
// under a millisecond and then took 2 s to validate "x", doubling per
// level, because nothing counted validation paths outside recursive
// components. Registration now counts them over the whole graph and
// refuses any subschema one value reaches through more than 64.
func TestAcyclicRefChainIsRefused(t *testing.T) {
	r := NewJSONSchema()
	for _, kw := range []string{"allOf", "anyOf", "oneOf"} {
		for _, levels := range []int{7, 22, 40} {
			s := dagSchema(levels, kw)
			start := time.Now()
			err := r.ValidateDefinition(context.Background(), "t", []byte(s))
			took := time.Since(start)
			if err == nil || !strings.Contains(err.Error(), "more than 64") {
				t.Errorf("%s chain of %d levels (%d bytes): err = %v, want the path-count refusal", kw, levels, len(s), err)
			}
			if took > time.Second {
				t.Errorf("%s chain of %d levels: registration check took %v", kw, levels, took)
			}
		}
		// 2^6 = 64 paths is the most a subschema may be reached by.
		if err := r.ValidateDefinition(context.Background(), "t", []byte(dagSchema(6, kw))); err != nil {
			t.Errorf("%s chain of 6 levels (64 paths) refused: %v", kw, err)
		}
	}

	union65 := func() string {
		var b strings.Builder
		b.WriteString(`{"oneOf":[`)
		for i := range 65 {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"allOf":[{"$ref":"#/$defs/base"},{"properties":{"kind":{"const":"k%d"}}}]}`, i)
		}
		b.WriteString(`],"$defs":{"base":{"type":"object","required":["kind"]}}}`)
		return b.String()
	}
	// A cluster of definitions that all reference each other through
	// anyOf: the validator walks every simple path through it, (n-1)!
	// steps per failing value.
	cluster := func(n int) string {
		var b strings.Builder
		b.WriteString(`{"$ref":"#/$defs/c0","$defs":{`)
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"c%d":{"anyOf":[{"type":"null"}`, i)
			for j := range n {
				if j != i {
					fmt.Fprintf(&b, `,{"$ref":"#/$defs/c%d"}`, j)
				}
			}
			b.WriteString(`]}`)
		}
		b.WriteString(`}}`)
		return b.String()
	}
	propertiesAndPatterns := func() string {
		var b strings.Builder
		b.WriteString(`{"$ref":"#/$defs/o0","$defs":{`)
		for i := range 8 {
			fmt.Fprintf(&b, `"o%d":{"properties":{"a":{"$ref":"#/$defs/o%d"}},"patternProperties":{"^a$":{"$ref":"#/$defs/o%d"}}},`, i, i+1, i+1)
		}
		b.WriteString(`"o8":{"type":"integer"}}}`)
		return b.String()
	}
	for name, s := range map[string]string{
		"propertyNames through an acyclic chain":    rerooted(dagSchema(8, "allOf"), `{"propertyNames":{"$ref":"#/$defs/d0"},`),
		"union of 65 branches sharing one base":     union65(),
		"anyOf cluster of 7 definitions":            cluster(7),
		"two applicators doubling under items":      rerooted(dagSchema(8, "anyOf"), `{"type":"array","items":{"$ref":"#/$defs/d0"},`),
		"patternProperties and properties doubling": propertiesAndPatterns(),
	} {
		if err := r.ValidateDefinition(context.Background(), "t", []byte(s)); err == nil || !strings.Contains(err.Error(), "paths") {
			t.Errorf("%s: err = %v, want the path-count refusal\n%.300s", name, err, s)
		}
	}
}

// anchorChain is dagSchema written with another reference form: the
// root applies d0, and each d_i applies d_{i+1} twice through allOf.
// header opens the root, defs names the member holding the levels,
// anchor(i) declares level i's name and ref(i) references it.
func anchorChain(levels int, header, defs string, anchor, ref func(i int) string) string {
	var b strings.Builder
	b.WriteString(`{` + header + `"allOf":[{"$ref":"` + ref(0) + `"}],"` + defs + `":{`)
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&b, `"d%d":{%s"allOf":[{"$ref":"%s"},{"$ref":"%s"}]},`, i, anchor(i), ref(i+1), ref(i+1))
	}
	fmt.Fprintf(&b, `"d%d":{%s"type":"string"}}}`, levels, anchor(levels))
	return b.String()
}

// TestRefChainsThroughAnchorsAreRefused covers reference forms a count
// over the raw document misses: the chain registered, the leaf was
// reached by 2^20 paths, and validating "x" took about half a second,
// doubling per level, with no validation limit taken. The count now
// walks what the compiler resolved, so every form is refused.
func TestRefChainsThroughAnchorsAreRefused(t *testing.T) {
	const levels = 20
	name := func(i int) string { return fmt.Sprintf("#d%d", i) }
	cases := map[string]string{
		"$anchor": anchorChain(levels, ``, "$defs",
			func(i int) string { return fmt.Sprintf(`"$anchor":"d%d",`, i) }, name),
		"$dynamicAnchor used as a plain anchor": anchorChain(levels, ``, "$defs",
			func(i int) string { return fmt.Sprintf(`"$dynamicAnchor":"d%d",`, i) }, name),
		"draft-07 $id anchor": anchorChain(levels, `"$schema":"http://json-schema.org/draft-07/schema#",`, "definitions",
			func(i int) string { return fmt.Sprintf(`"$id":"#d%d",`, i) }, name),
		"draft-06 $id anchor": anchorChain(levels, `"$schema":"http://json-schema.org/draft-06/schema#",`, "definitions",
			func(i int) string { return fmt.Sprintf(`"$id":"#d%d",`, i) }, name),
		"draft-04 id anchor": anchorChain(levels, `"$schema":"http://json-schema.org/draft-04/schema#",`, "definitions",
			func(i int) string { return fmt.Sprintf(`"id":"#d%d",`, i) }, name),
		"chain under a member no keyword owns": anchorChain(levels, ``, "x",
			func(int) string { return "" }, func(i int) string { return fmt.Sprintf("#/x/d%d", i) }),
	}

	// A draft-04 resource nested with id: its pointers resolve against
	// it, not the document root.
	var nestedID strings.Builder
	nestedID.WriteString(`{"$schema":"http://json-schema.org/draft-04/schema#","allOf":[{"$ref":"#/definitions/sub"}],` +
		`"definitions":{"sub":{"id":"http://example.com/sub","allOf":[{"$ref":"#/definitions/d0"}],"definitions":{`)
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&nestedID, `"d%d":{"allOf":[{"$ref":"#/definitions/d%d"},{"$ref":"#/definitions/d%d"}]},`, i, i+1, i+1)
	}
	fmt.Fprintf(&nestedID, `"d%d":{"type":"string"}}}}}`, levels)
	cases["draft-04 nested id resource"] = nestedID.String()

	// The doubling chain is reached only by dynamic resolution: the
	// nested resource's levels each apply the next once, but a
	// $dynamicRef resolves to the outermost resource in scope that
	// declares the anchor, the root, whose levels apply it twice.
	var dynamic strings.Builder
	dynamic.WriteString(`{"$ref":"#/$defs/inner","$defs":{"inner":{"$id":"inner","$ref":"#/$defs/d0","$defs":{`)
	for i := 0; i < levels; i++ {
		fmt.Fprintf(&dynamic, `"d%d":{"$dynamicAnchor":"a%d","$dynamicRef":"#a%d"},`, i, i, i+1)
	}
	fmt.Fprintf(&dynamic, `"d%d":{"$dynamicAnchor":"a%d","type":"string"}}}`, levels, levels)
	for i := 1; i < levels; i++ {
		fmt.Fprintf(&dynamic, `,"r%d":{"$dynamicAnchor":"a%d","allOf":[{"$dynamicRef":"#a%d"},{"$dynamicRef":"#a%d"}]}`, i, i, i+1, i+1)
	}
	fmt.Fprintf(&dynamic, `,"r%d":{"$dynamicAnchor":"a%d","type":"string"}}}`, levels, levels)
	cases["$dynamicRef resolved to the root resource's anchors"] = dynamic.String()

	r := NewJSONSchema()
	for name, s := range cases {
		start := time.Now()
		err := r.ValidateDefinition(context.Background(), "t", []byte(s))
		if err == nil || !strings.Contains(err.Error(), "more than 64") {
			t.Errorf("%s (%d bytes): err = %v, want the path-count refusal", name, len(s), err)
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: registration check took %v", name, took)
		}
		// One persisted before the count existed loads and is validated
		// under the validation limit, however small the payload.
		cs, err := compileTopicBytes("t", 1, []byte(s))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !cs.expensive() {
			t.Errorf("%s: a persisted copy is not validated under the validation limit", name)
		}
	}
}

// TestPathMultiplicityAcceptsOrdinaryRecursion pins what the count
// leaves alone. Paths only add up when they apply a subschema to the
// same value: a shared definition used under many property names is
// reached once per value, and ordinary recursion (trees, JSON values,
// the 2020-12 extensible tree through $dynamicRef) reaches each value
// a bounded number of times however deep it nests.
func TestPathMultiplicityAcceptsOrdinaryRecursion(t *testing.T) {
	var sharedProps, union64, wideDistinct strings.Builder
	sharedProps.WriteString(`{"type":"object","properties":{`)
	for i := range 200 {
		if i > 0 {
			sharedProps.WriteByte(',')
		}
		fmt.Fprintf(&sharedProps, `"p%d":{"$ref":"#/$defs/addr"}`, i)
	}
	sharedProps.WriteString(`},"$defs":{"addr":{"type":"object","properties":{"zip":{"$ref":"#/$defs/zip"}}},"zip":{"type":"string"}}}`)
	union64.WriteString(`{"oneOf":[`)
	for i := range 64 {
		if i > 0 {
			union64.WriteByte(',')
		}
		fmt.Fprintf(&union64, `{"allOf":[{"$ref":"#/$defs/base"},{"properties":{"kind":{"const":"k%d"}}}]}`, i)
	}
	union64.WriteString(`],"$defs":{"base":{"type":"object","required":["kind"]}}}`)
	wideDistinct.WriteString(`{"oneOf":[`)
	for i := range 500 {
		if i > 0 {
			wideDistinct.WriteByte(',')
		}
		fmt.Fprintf(&wideDistinct, `{"properties":{"kind":{"const":"k%d"}},"required":["kind"]}`, i)
	}
	wideDistinct.WriteString(`]}`)
	cluster5 := `{"$ref":"#/$defs/c0","$defs":{`
	for i := range 5 {
		if i > 0 {
			cluster5 += ","
		}
		cluster5 += fmt.Sprintf(`"c%d":{"anyOf":[{"type":"null"}`, i)
		for j := range 5 {
			if j != i {
				cluster5 += fmt.Sprintf(`,{"$ref":"#/$defs/c%d"}`, j)
			}
		}
		cluster5 += `]}`
	}
	cluster5 += `}}`

	accepted := map[string]string{
		"shared definition under 200 property names": sharedProps.String(),
		"union of 64 branches sharing one base":      union64.String(),
		"union of 500 distinct branches":             wideDistinct.String(),
		"anyOf cluster of 5 definitions":             cluster5,
		"JSON value": `{"$ref":"#/$defs/v","$defs":{"v":{"anyOf":[{"type":["null","boolean","number","string"]},` +
			`{"type":"array","items":{"$ref":"#/$defs/v"}},{"type":"object","additionalProperties":{"$ref":"#/$defs/v"}}]}}}`,
		"tree with shared node definition": `{"$ref":"#/$defs/node","$defs":{"node":{"type":"object","properties":{"id":{"$ref":"#/$defs/id"},` +
			`"parent":{"$ref":"#/$defs/id"},"children":{"type":"array","items":{"$ref":"#/$defs/node"}}}},"id":{"type":"integer"}}}`,
		"one diamond per level of a recursion": `{"allOf":[{"$ref":"#/$defs/x"},{"$ref":"#/$defs/x"}],"items":{"$ref":"#"},"$defs":{"x":{"type":"array"}}}`,
		"propertyNames referencing the root":   `{"propertyNames":{"$ref":"#"},"items":{"$ref":"#"},"type":["array","string"]}`,
		"anchored tree": `{"$ref":"#node","$defs":{"n":{"$anchor":"node","type":"object",` +
			`"properties":{"children":{"type":"array","items":{"$ref":"#node"}}}}}}`,
	}
	r := NewJSONSchema()
	for name, s := range accepted {
		if err := r.ValidateDefinition(context.Background(), "t", []byte(s)); err != nil {
			t.Errorf("%s: unexpected refusal: %v", name, err)
		}
	}

	// Recursion through dynamic references: the path count follows
	// every target dynamic resolution can pick and still finds one path
	// per value. (The older recursion check, which reads the raw
	// document, refuses both at registration on its own.)
	for name, s := range map[string]string{
		"extensible tree through $dynamicRef": `{"$dynamicAnchor":"node","$ref":"#/$defs/tree","unevaluatedProperties":false,` +
			`"$defs":{"tree":{"$id":"tree","$dynamicAnchor":"node","type":"object",` +
			`"properties":{"data":true,"children":{"type":"array","items":{"$dynamicRef":"#node"}}}}}}`,
		"draft 2019-09 tree through $recursiveRef": `{"$schema":"https://json-schema.org/draft/2019-09/schema","$recursiveAnchor":true,` +
			`"type":"object","properties":{"children":{"type":"array","items":{"$recursiveRef":"#"}}}}`,
	} {
		cs, err := compileTopicBytes("t", 1, []byte(s))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if cs.cost.paths != nil || cs.expensive() {
			t.Errorf("%s: path count %v, expensive=%v; want one path per value", name, cs.cost.paths, cs.expensive())
		}
	}
}
