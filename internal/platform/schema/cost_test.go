package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestCostlyPatternsAreRefused: registration refuses costly patterns.
// Go's regexp is linear in the input, but each byte costs every
// instruction the matcher visits for it: one per partial match still
// alive (an unanchored counted repetition keeps one per starting
// position) plus every alternation, capture and empty-width assertion
// on the way to them. The 39-byte {"type":"string","pattern":
// "a.{1000}b"} took 5 s per 1 MiB string, and "(?:\B){1000}x" keeps one
// partial match yet walks a thousand assertions per byte (3.4 s).
// Registration refuses a pattern that visits more than 32 instructions
// per byte, wherever the validator can apply it.
func TestCostlyPatternsAreRefused(t *testing.T) {
	refused := []string{
		`{"type":"string","pattern":"a.{1000}b"}`,
		`{"type":"string","pattern":"a{1000}b"}`,
		`{"type":"string","pattern":"^.*a.{200}b"}`,
		`{"type":"string","pattern":"\\d{1,64}x"}`,
		`{"type":"string","pattern":"(?:\\B){1000}x"}`,
		`{"type":"string","pattern":"(?:(?:\\B)(?:)){999}x"}`,
		`{"type":"string","pattern":"(?:\\b|\\B){40}x"}`,
		`{"type":"object","patternProperties":{"a.{1000}b":{"type":"integer"}}}`,
		`{"type":"object","propertyNames":{"pattern":"a.{1000}b"}}`,
		`{"type":"array","items":{"anyOf":[{"type":"integer"},{"pattern":"a.{500}b"}]}}`,
		`{"$defs":{"s":{"pattern":"(a|b){100}c"}},"$ref":"#/$defs/s"}`,
		// Applied through a reference into a member no keyword owns.
		`{"allOf":[{"$ref":"#/x/s"}],"x":{"s":{"type":"string","pattern":"a.{1000}b"}}}`,
		`{"allOf":[{"$ref":"#/x/s"}],"x":{"s":{"pattern":"(?:\\B){1000}x"}}}`,
	}
	accepted := []string{
		`{"type":"string","pattern":"^a.{1000}b"}`,
		`{"type":"string","pattern":"^[a-z]{1,255}$"}`,
		`{"type":"string","pattern":"^[A-Z]+-[0-9]+$"}`,
		`{"type":"string","pattern":"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"}`,
		`{"type":"string","pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"}`,
		`{"type":"string","pattern":"^(?:[a-z0-9-]{1,63}\\.)+[a-z]{2,63}$"}`,
		`{"type":"string","pattern":"^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$"}`,
		`{"type":"string","pattern":"^\\+?[1-9]\\d{1,14}$"}`,
		`{"type":"string","pattern":"^.{0,280}$"}`,
		`{"type":"string","pattern":"error|warn|fatal"}`,
		`{"type":"string","pattern":"\\bword\\b"}`,
		// Counted, unanchored, yet cheap: a new partial match needs an
		// x, which ends every partial match still reading digits.
		`{"type":"string","pattern":"x[0-9]{1,64}y"}`,
		`{"type":"object","patternProperties":{"^x-":{"type":"string"},"^[a-z]{1,32}$":{"type":"integer"}}}`,
	}
	r := NewJSONSchema()
	for _, s := range refused {
		start := time.Now()
		err := r.ValidateDefinition(context.Background(), "t", []byte(s))
		if err == nil || !strings.Contains(err.Error(), "on every byte") {
			t.Errorf("%s: err = %v, want the pattern-cost refusal", s, err)
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: registration check took %v", s, took)
		}
	}
	for _, s := range accepted {
		if err := r.ValidateDefinition(context.Background(), "t", []byte(s)); err != nil {
			t.Errorf("%s: unexpected refusal: %v", s, err)
		}
	}
	// A persisted schema is never re-checked: it still loads and
	// validates.
	if err := r.Load(context.Background(), "t", 1, []byte(refused[0])); err != nil {
		t.Fatalf("persisted schema with a costly pattern no longer loads: %v", err)
	}
	if err := r.Validate(context.Background(), "t", []byte(`"a`+strings.Repeat("x", 1000)+`b"`)); err != nil {
		t.Fatalf("persisted pattern schema: %v", err)
	}
}

// TestPatternBoundCountsEveryVisitedInstruction pins the measure
// itself against Go's own matcher: patterns that keep one partial match
// but walk long chains of empty-width or empty instructions per byte
// count those, not one.
func TestPatternBoundCountsEveryVisitedInstruction(t *testing.T) {
	for _, p := range []string{`(?:\B){1000}x`, `(?:(?:\B)(?:)){999}x`, `(?:\b|\B){40}x`} {
		if _, err := regexp.Compile(p); err != nil {
			t.Fatalf("%q: %v", p, err)
		}
		if n := patternThreads(p); n <= maxPatternThreads {
			t.Errorf("%q: bound %d, want more than %d: every assertion is visited on every byte", p, n, maxPatternThreads)
		}
	}
	if n := patternThreads(`x[0-9]{1,64}y`); n > maxPatternThreads {
		t.Errorf("x[0-9]{1,64}y: bound %d, want at most %d", n, maxPatternThreads)
	}
}

// wideRequiredSchema is the largest registrable {"type":"array",
// "items":{kw: [names]}} and how many names it lists. For
// dependentRequired the list hangs off the property "k".
func wideRequiredSchema(kw string) (string, int) {
	var b strings.Builder
	switch kw {
	case "required":
		b.WriteString(`{"type":"array","items":{"required":[`)
	case "dependentRequired":
		b.WriteString(`{"type":"array","items":{"dependentRequired":{"k":[`)
	}
	names := 0
	for b.Len() < MaxSchemaBytes-64 {
		if names > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"%x"`, names)
		names++
	}
	b.WriteString(`]}}`)
	if kw == "dependentRequired" {
		b.WriteString(`}`)
	}
	return b.String(), names
}

// itemsPayload is an array of item, as many as fit under n bytes.
func itemsPayload(item string, n int) []byte {
	var p strings.Builder
	p.WriteByte('[')
	for p.Len() < n-len(item)-2 {
		if p.Len() > 1 {
			p.WriteByte(',')
		}
		p.WriteString(item)
	}
	p.WriteByte(']')
	return []byte(p.String())
}

// TestWideRequiredListRejectionStaysBounded covers the other way a
// detailed report grows: a required (or dependentRequired) error lists
// every missing name. Against a registrable schema whose items require
// 38k names, a payload of 16381 bytes of {} items fits under the size
// at which validation takes a slot of the node's limit, yet its report
// held 38k names per item: 3.5 s, 17 GiB allocated and a 6.6 GiB heap
// peak for one produce. The report's cost now counts the names, so the
// payload is checked without one, and the schema's lists alone put its
// validations under the limit.
func TestWideRequiredListRejectionStaysBounded(t *testing.T) {
	for _, tc := range []struct{ kw, item string }{
		{"required", `{}`},
		{"dependentRequired", `{"k":1}`},
	} {
		t.Run(tc.kw, func(t *testing.T) {
			ctx := context.Background()
			doc, names := wideRequiredSchema(tc.kw)
			r := NewJSONSchema()
			if err := r.ValidateDefinition(ctx, "t", []byte(doc)); err != nil {
				t.Fatalf("schema of %d bytes listing %d names refused: %v", len(doc), names, err)
			}
			if err := r.Load(ctx, "t", 1, []byte(doc)); err != nil {
				t.Fatal(err)
			}
			payload := itemsPayload(tc.item, limitedPayloadBytes)
			if len(payload) > limitedPayloadBytes {
				t.Fatalf("payload is %d bytes", len(payload))
			}

			// Validations against the schema wait for a slot, however
			// small the payload.
			r.SetValidationLimit(1, 20*time.Millisecond)
			if err := r.limiter.acquire(ctx); err != nil {
				t.Fatal(err)
			}
			if err := r.Validate(ctx, "t", []byte(`[]`)); !IsCapacityError(err) {
				t.Errorf("small payload with the only slot taken: err = %v, want it to wait for a slot", err)
			}
			r.limiter.release()

			skipBounds(t)
			use, err := measureHeap(func() error { return r.Validate(ctx, "t", payload) })
			t.Logf("%d-byte payload against %d names: took %v, allocated %.1f MiB, peak %.1f MiB, err=%.160v",
				len(payload), names, use.took, use.allocMiB, use.peakMiB, err)
			if err == nil {
				t.Fatal("payload missing every required name accepted")
			}
			if len(err.Error()) > maxValidationErrorBytes+256 {
				t.Errorf("error message is %d bytes", len(err.Error()))
			}
			if use.allocMiB > 64 {
				t.Errorf("validation allocated %.1f MiB; the bound is 64 MiB", use.allocMiB)
			}
			if use.peakMiB > 64 {
				t.Errorf("validation peaked at %.1f MiB of heap; the bound is 64 MiB", use.peakMiB)
			}
		})
	}
}

// TestManyPatternPropertiesValidateUnderTheLimit: every member name is
// matched against every patternProperties key, so a schema with
// thousands of keys makes one 16 KiB object cost seconds. Such a schema
// is flagged and its validations wait for a slot.
func TestManyPatternPropertiesValidateUnderTheLimit(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"type":"object","patternProperties":{`)
	for i := range expensivePatternProperties + 1 {
		if i > 0 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(fmt.Sprintf("^q%x$", i))
		fmt.Fprintf(&b, `%s:{}`, key)
	}
	b.WriteString(`}}`)
	ctx := context.Background()
	r := NewJSONSchema()
	r.SetValidationLimit(1, 20*time.Millisecond)
	if err := r.Load(ctx, "t", 1, []byte(b.String())); err != nil {
		t.Fatal(err)
	}
	if err := r.limiter.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(ctx, "t", []byte(`{"a":1}`)); !IsCapacityError(err) {
		t.Errorf("small payload with the only slot taken: err = %v, want it to wait for a slot", err)
	}
	r.limiter.release()
	if err := r.Validate(ctx, "t", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
}
