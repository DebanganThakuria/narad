package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Directed worst cases for the time and memory bounds: schemas at the
// 256 KiB registration limit built to maximise compile work, and
// payloads at the 1 MiB produce limit built to maximise validation
// work. Each case logs its cost; the assertion is the bug threshold
// (slowBug), and NARAD_FUZZ_TIMING=1 also fails on slowFinding.

// fill pads a JSON document generator up to about MaxSchemaBytes.
func fill(build func(n int) string) string {
	lo, hi := 1, 1<<20
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if len(build(mid)) <= MaxSchemaBytes {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return build(lo)
}

func measure(t *testing.T, what string, fn func()) (time.Duration, uint64) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	fn()
	d := time.Since(start)
	runtime.ReadMemStats(&after)
	alloc := after.TotalAlloc - before.TotalAlloc
	t.Logf("%-52s %8.1f ms  %7.1f MiB allocated", what, float64(d)/float64(time.Millisecond), float64(alloc)/(1<<20))
	if d > slowBug {
		t.Errorf("%s took %v (bug threshold %v)", what, d, slowBug)
	} else if fuzzTiming && d > slowFinding {
		t.Errorf("%s took %v (finding threshold %v)", what, d, slowFinding)
	}
	return d, alloc
}

func skipBounds(t *testing.T) {
	t.Helper()
	if testing.Short() || raceEnabled {
		t.Skip("worst-case shapes run without -short and without the race detector")
	}
}

func TestBoundsCompile(t *testing.T) {
	skipBounds(t)
	strs := func(n int, f func(i int) string) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = f(i)
		}
		return strings.Join(parts, ",")
	}
	shapes := map[string]string{
		"huge enum of strings": fill(func(n int) string {
			return `{"enum":[` + strs(n, func(i int) string { return fmt.Sprintf(`"value-%d"`, i) }) + `]}`
		}),
		"huge enum of objects": fill(func(n int) string {
			return `{"enum":[` + strs(n, func(i int) string { return fmt.Sprintf(`{"k":%d,"a":[1,2]}`, i) }) + `]}`
		}),
		"many properties": fill(func(n int) string {
			return `{"type":"object","properties":{` + strs(n, func(i int) string { return fmt.Sprintf(`"p%d":{"type":"string"}`, i) }) + `}}`
		}),
		"many required names": fill(func(n int) string {
			return `{"required":[` + strs(n, func(i int) string { return fmt.Sprintf(`"p%d"`, i) }) + `]}`
		}),
		"many distinct patternProperties": fill(func(n int) string {
			return `{"patternProperties":{` + strs(n, func(i int) string { return fmt.Sprintf(`"^k%d-[a-z]+$":{"type":"string"}`, i) }) + `}}`
		}),
		"many $refs in anyOf": fill(func(n int) string {
			return `{"$defs":{"x":{"type":"integer"}},"anyOf":[` + strs(n, func(int) string { return `{"$ref":"#/$defs/x"}` }) + `]}`
		}),
		"many $defs each referenced": fill(func(n int) string {
			return `{"$defs":{` + strs(n, func(i int) string { return fmt.Sprintf(`"d%d":{"type":"integer"}`, i) }) + `},"anyOf":[` +
				strs(n, func(i int) string { return fmt.Sprintf(`{"$ref":"#/$defs/d%d"}`, i) }) + `]}`
		}),
		"many formats": fill(func(n int) string {
			return `{"anyOf":[` + strs(n, func(i int) string {
				return fmt.Sprintf(`{"type":"string","format":"%s"}`, []string{"email", "date-time", "uuid", "ipv4", "uri", "hostname", "regex"}[i%7])
			}) + `]}`
		}),
		"64-level properties nesting, wide": fill(func(n int) string {
			var b strings.Builder
			for range 63 {
				b.WriteString(`{"type":"object","properties":{`)
				for j := range n {
					fmt.Fprintf(&b, `"w%d":{"type":"integer"},`, j)
				}
				b.WriteString(`"n":`)
			}
			b.WriteString(`{"type":"integer"}`)
			for range 63 {
				b.WriteString(`}}`)
			}
			return b.String()
		}),
		"64-deep $ref chain": func() string {
			var b strings.Builder
			b.WriteString(`{"$ref":"#/$defs/d0","$defs":{`)
			for i := range 62 {
				fmt.Fprintf(&b, `"d%d":{"$ref":"#/$defs/d%d"},`, i, i+1)
			}
			b.WriteString(`"d62":{"type":"integer"}}}`)
			return b.String()
		}(),
		"binary anyOf tree": fill(func(n int) string {
			var rec func(d int) string
			rec = func(d int) string {
				if d == 0 {
					return `{"type":"integer"}`
				}
				return `{"anyOf":[` + rec(d-1) + `,` + rec(d-1) + `]}`
			}
			depth := 0
			for (1 << depth) < n {
				depth++
			}
			if depth > 60 {
				depth = 60
			}
			return rec(depth)
		}),
		"allOf/anyOf/oneOf alternating 60 deep": func() string {
			var b strings.Builder
			for i := range 60 {
				b.WriteString(`{"` + []string{"allOf", "anyOf", "oneOf"}[i%3] + `":[`)
			}
			b.WriteString(`{"type":"integer"}`)
			for range 60 {
				b.WriteString(`]}`)
			}
			return b.String()
		}(),
		"one 256 KiB integer literal":         `{"minimum":` + strings.Repeat("9", MaxSchemaBytes-16) + `}`,
		"one 256 KiB decimal literal":         `{"multipleOf":0.` + strings.Repeat("3", MaxSchemaBytes-20) + `1}`,
		"one 256 KiB pattern":                 `{"pattern":"` + strings.Repeat("a", MaxSchemaBytes-20) + `"}`,
		"one 256 KiB pattern of alternations": `{"pattern":"^(` + strings.Repeat("ab|", (MaxSchemaBytes-40)/3) + `c)$"}`,
		"one 256 KiB unicode string enum":     `{"enum":["` + strings.Repeat("日", (MaxSchemaBytes-16)/3) + `"]}`,
		"many unicode property names": fill(func(n int) string {
			return `{"properties":{` + strs(n, func(i int) string { return fmt.Sprintf(`"日本%d":true`, i) }) + `}}`
		}),
		"many dependentRequired": fill(func(n int) string {
			return `{"dependentRequired":{` + strs(n, func(i int) string { return fmt.Sprintf(`"p%d":["p%d"]`, i, i+1) }) + `}}`
		}),
		"many prefixItems": fill(func(n int) string {
			return `{"prefixItems":[` + strs(n, func(int) string { return `{"type":"integer"}` }) + `]}`
		}),
		"many nested $id": fill(func(n int) string {
			return `{"$defs":{` + strs(n, func(i int) string {
				return fmt.Sprintf(`"d%d":{"$id":"https://example.com/s%d","type":"integer"}`, i, i)
			}) + `}}`
		}),
		"many $anchor": fill(func(n int) string {
			return `{"$defs":{` + strs(n, func(i int) string { return fmt.Sprintf(`"d%d":{"$anchor":"a%d","type":"integer"}`, i, i) }) + `}}`
		}),
	}
	names := make([]string, 0, len(shapes))
	for k := range shapes {
		names = append(names, k)
	}
	sortStrings(names)
	for _, name := range names {
		doc := shapes[name]
		if len(doc) > MaxSchemaBytes {
			t.Fatalf("%s: shape is %d bytes", name, len(doc))
		}
		r := NewJSONSchema()
		var err error
		measure(t, fmt.Sprintf("compile %s (%d KiB)", name, len(doc)>>10), func() {
			err = r.ValidateDefinition(context.Background(), "t", []byte(doc))
		})
		if err != nil {
			t.Logf("  refused: %.120s", err.Error())
		}
		// The compatibility check against itself is part of registering
		// a new version; it must stay within the same bound.
		if err == nil {
			measure(t, fmt.Sprintf("self-compat %s", name), func() {
				if err := checkCompatible([]byte(doc), []byte(doc)); err != nil {
					t.Errorf("%s: incompatible with itself: %.200s", name, err)
				}
			})
		}
	}
}

func TestBoundsValidate(t *testing.T) {
	skipBounds(t)
	const maxPayload = 1 << 20
	intArray := func(n int) string {
		var b strings.Builder
		b.WriteByte('[')
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%d", i)
		}
		b.WriteByte(']')
		return b.String()
	}
	strArray := func(n int) string {
		var b strings.Builder
		b.WriteByte('[')
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"s%d"`, i)
		}
		b.WriteByte(']')
		return b.String()
	}
	objKeys := func(n int) string {
		var b strings.Builder
		b.WriteByte('{')
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"key-%d":1`, i)
		}
		b.WriteByte('}')
		return b.String()
	}
	enumSchema := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"e%d"`, i)
		}
		return `{"items":{"enum":[` + strings.Join(parts, ",") + `]}}`
	}
	patternSchema := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"^pat%d-[a-z]+$":{"type":"string"}`, i)
		}
		return `{"patternProperties":{` + strings.Join(parts, ",") + `}}`
	}
	cases := []struct {
		name, schema, payload string
	}{
		{"uniqueItems over 1 MiB of integers", `{"uniqueItems":true}`, intArray(150_000)},
		{"uniqueItems over 1 MiB of strings", `{"uniqueItems":true}`, strArray(110_000)},
		{"uniqueItems over 1 MiB of objects", `{"uniqueItems":true}`, "[" + strings.TrimSuffix(strings.Repeat(`{"a":[1,{"b":2}]},`, 55_000), ",") + "]"},
		{"uniqueItems over equal objects", `{"uniqueItems":true}`, "[" + strings.TrimSuffix(strings.Repeat(`{"a":[1,{"b":2}]},`, 55_000), ",") + "]"},
		{"5k-value enum against 110k items (linear scan)", enumSchema(5_000), strArray(110_000)},
		{"1k patternProperties against 50k keys (every key tries every pattern)", patternSchema(1_000), objKeys(50_000)},
		{"propertyNames pattern against 50k keys", `{"propertyNames":{"pattern":"^key-[0-9]+$"}}`, objKeys(50_000)},
		{"multipleOf on 1e1000 literals", `{"items":{"multipleOf":1e-1000}}`, "[" + strings.TrimSuffix(strings.Repeat("1e1000,", 100_000), ",") + "]"},
		{"multipleOf 0.1 on 100k decimals", `{"items":{"multipleOf":0.1}}`, "[" + strings.TrimSuffix(strings.Repeat("123456.7,", 100_000), ",") + "]"},
		{"integer check on 100k exponent forms", `{"items":{"type":"integer"}}`, "[" + strings.TrimSuffix(strings.Repeat("1.5e300,", 100_000), ",") + "]"},
		{"one 1 MiB integer literal, bounds", `{"minimum":0,"maximum":1e1000,"multipleOf":3}`, strings.Repeat("9", maxPayload-8)},
		{"one 1 MiB string, maxLength", `{"type":"string","maxLength":10}`, `"` + strings.Repeat("日", (maxPayload-8)/3) + `"`},
		{"one 1 MiB string, pattern", `{"type":"string","pattern":"^(a|b)*c$"}`, `"` + strings.Repeat("ab", (maxPayload-8)/2) + `"`},
		{"one 1 MiB string, format email", `{"type":"string","format":"email"}`, `"` + strings.Repeat("a", maxPayload-20) + `@x.io"`},
		{"one 1 MiB string, format uri", `{"type":"string","format":"uri"}`, `"https://x.io/` + strings.Repeat("a", maxPayload-30) + `"`},
		{"contains with minContains over 150k items", `{"contains":{"type":"string"},"minContains":150000}`, intArray(150_000)},
		{"deep recursion 9000 levels", `{"properties":{"a":{"$ref":"#"}},"additionalProperties":false}`, strings.Repeat(`{"a":`, 9000) + `{}` + strings.Repeat(`}`, 9000)},
		{"deep anyOf recursion 9000 levels", `{"anyOf":[{"type":"integer"},{"type":"array","items":{"$ref":"#"}}]}`, strings.Repeat(`[`, 9000) + `1` + strings.Repeat(`]`, 9000)},
		{"20k-value enum error message on 100k failing items", enumSchema(20_000), intArray(100_000)},
	}
	for _, tc := range cases {
		if len(tc.payload) > maxPayload+64 {
			t.Fatalf("%s: payload is %d bytes", tc.name, len(tc.payload))
		}
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
			t.Fatalf("%s: Load: %v", tc.name, err)
		}
		var err error
		measure(t, fmt.Sprintf("validate %s (%d KiB)", tc.name, len(tc.payload)>>10), func() {
			err = r.Validate(context.Background(), "t", []byte(tc.payload))
		})
		if err != nil {
			if n := len(err.Error()); n > maxValidationErrorBytes+64 {
				t.Errorf("%s: error message is %d bytes", tc.name, n)
			}
		}
	}
}

// TestBoundsRevalidation: a schema that validates the same sub-instance
// through several applicators multiplies the work per nesting level.
// This measures how the cost grows with payload depth for a fixed
// small fan-in.
func TestBoundsRevalidation(t *testing.T) {
	skipBounds(t)
	fanIn := 4
	branches := make([]string, fanIn)
	for i := range branches {
		branches[i] = `{"items":{"$ref":"#"}}`
	}
	schema := `{"allOf":[` + strings.Join(branches, ",") + `]}`
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(schema)); err != nil {
		t.Fatal(err)
	}
	for depth := 4; depth <= 10; depth += 2 {
		payload := strings.Repeat("[", depth) + "1" + strings.Repeat("]", depth)
		d, _ := measure(t, fmt.Sprintf("validate fan-in %d, %d-byte payload nested %d", fanIn, len(payload), depth), func() {
			_ = r.Validate(context.Background(), "t", []byte(payload))
		})
		if d > slowFinding {
			break
		}
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

var _ = json.Valid
