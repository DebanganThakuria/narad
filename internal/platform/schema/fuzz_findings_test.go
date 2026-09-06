package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Regression tests for what fuzzing and property testing found. Each
// case is the minimal input that reproduced the finding on the code
// before the fix; the comment says what went wrong.

// TestFindingBigExponentPayloadPanics: a payload number with an
// exponent beyond a million made big.Rat refuse the literal and the
// validator dereferenced the nil result (minimum, maximum,
// multipleOf); smaller exponents (1e999999) validated but expanded to
// a million digits per literal. Both are now refused before
// validation, in payloads and in schemas.
func TestFindingBigExponentPayloadPanics(t *testing.T) {
	for _, tc := range []struct {
		schema, payload, wantErr string
	}{
		{`{"minimum":0}`, `1e9999999`, "outside ±1000"},
		{`{"multipleOf":2}`, `1e9999999`, "outside ±1000"},
		{`{"maximum":0}`, `-1e9999999`, "outside ±1000"},
		{`{"type":"integer"}`, `1e999999`, "outside ±1000"},
		{`{"type":"number"}`, `1e-1001`, "outside ±1000"},
		{`{"type":"number"}`, `1E+1001`, "outside ±1000"},
		{`{"type":"number"}`, `[1, 2, 3e0001001]`, "outside ±1000"},
		{`{"type":"number"}`, `1e1000`, ""},
		{`{"type":"number"}`, `1e-1000`, ""},
		{`{"type":"number"}`, `1e0000000001000`, ""},
		{`{"type":"number"}`, `1e400`, ""},
		{`{"type":"string"}`, `"1e9999999 inside a string is fine"`, ""},
		{`{"type":"integer"}`, `"1e9999999"`, "got string, want integer"},
		{`{"type":"string"}`, `"escaped quote \" then 1e9999999"`, ""},
		{`{"type":"boolean"}`, `true`, ""},
	} {
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
			t.Fatalf("Load(%s): %v", tc.schema, err)
		}
		err := r.Validate(context.Background(), "t", []byte(tc.payload))
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s / %s: unexpected error %v", tc.schema, tc.payload, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s / %s: got %v, want an error containing %q", tc.schema, tc.payload, err, tc.wantErr)
		}
	}
	// The same literal in a schema is refused at registration, and a
	// malformed document is still reported as malformed first.
	r := NewJSONSchema()
	for _, tc := range []struct{ schema, wantErr string }{
		{`{"minimum":1e9999999}`, "outside ±1000"},
		{`{"enum":[1e999999]}`, "outside ±1000"},
		{`{"minimum":1e1000}`, ""},
		{`{"minimum":1e9999999`, "invalid JSON"},
	} {
		err := r.ValidateDefinition(context.Background(), "t", []byte(tc.schema))
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("ValidateDefinition(%s): unexpected error %v", tc.schema, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("ValidateDefinition(%s): got %v, want an error containing %q", tc.schema, err, tc.wantErr)
		}
	}
}

// TestFindingUnreferencedExternalRefRegisters: the compiler only
// compiles subschemas the root reaches, so an external $ref in a $defs
// entry nobody points at (or a $ref of "" / the schema's own absolute
// $id, which the compiler resolves in-document) registered although
// the contract says such references are refused. Registration now
// walks every schema position.
func TestFindingUnreferencedExternalRefRegisters(t *testing.T) {
	r := NewJSONSchema()
	for _, tc := range []struct{ schema, wantErr string }{
		{`{"$defs":{"d0":{"$ref":"file:///etc/passwd","const":null}}}`, "external $ref"},
		{`{"$ref":""}`, "external $ref"},
		{`{"definitions":{"x":{"anyOf":[{"$ref":"other.json"}]}},"type":"integer"}`, "external $ref"},
		{`{"$id":"https://example.com/s","$ref":"https://example.com/s#/$defs/a","$defs":{"a":{"type":"integer"}}}`, "external $ref"},
		{`{"$defs":{"d0":{"items":[{"$dynamicRef":"http://x/#dyn"}]}}}`, "external $ref"},
		{`{"$defs":{"d0":{"$ref":"#/$defs/d1"},"d1":{"type":"integer"}},"type":"integer"}`, ""},
		// $ref as a property name, or inside a literal, is not a reference.
		{`{"properties":{"$ref":{"type":"string"}},"const":{"$ref":"http://x"}}`, ""},
		{`{"enum":[{"$ref":"file:///etc/passwd"}]}`, ""},
	} {
		err := r.ValidateDefinition(context.Background(), "t", []byte(tc.schema))
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("ValidateDefinition(%s): unexpected error %v", tc.schema, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("ValidateDefinition(%s): got %v, want an error containing %q", tc.schema, err, tc.wantErr)
		case tc.wantErr != "" && strings.Contains(err.Error(), "passwd"):
			t.Errorf("ValidateDefinition(%s): error echoes the reference: %v", tc.schema, err)
		}
	}
}

// TestFindingErrorRenderingUnbounded: the 2 KiB cap was applied to
// the library's fully rendered message, which lists every enum value
// once per failing item. 20k values against 100k failing items built
// a multi-gigabyte string over minutes. The message is now rendered
// under the cap; for small errors it must stay byte-identical to the
// library's, since clients read it.
func TestFindingErrorRenderingUnbounded(t *testing.T) {
	values := make([]string, 5000)
	for i := range values {
		values[i] = fmt.Sprintf(`"e%d"`, i)
	}
	schema := `{"items":{"enum":[` + strings.Join(values, ",") + `]}}`
	var b strings.Builder
	b.WriteByte('[')
	for i := range 20000 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", i)
	}
	b.WriteByte(']')
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(schema)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := r.Validate(context.Background(), "t", []byte(b.String()))
	if d := time.Since(start); d > slowBug {
		t.Fatalf("validation plus message took %v", d)
	}
	if err == nil {
		t.Fatal("expected a validation error")
	}
	msg := err.Error()
	if len(msg) > maxValidationErrorBytes+64 {
		t.Fatalf("message is %d bytes", len(msg))
	}
	if !strings.Contains(msg, "at '/0'") || !strings.Contains(msg, "more") {
		t.Fatalf("message lost its first error or its truncation marker: %.300s", msg)
	}
	if _, ok := errors.AsType[*jsonschema.ValidationError](err); !ok {
		t.Fatalf("the library error is no longer reachable through Unwrap")
	}

	// Small errors render exactly as the library renders them.
	for _, tc := range []struct{ schema, payload string }{
		{`{"type":"object","properties":{"id":{"type":"integer"},"tags":{"type":"array","items":{"type":"string"}}},"required":["id","name"],"additionalProperties":false}`, `{"id":"x","tags":[1,"a",2],"zzz":1}`},
		{`{"anyOf":[{"type":"string"},{"type":"integer","minimum":3}]}`, `1.5`},
		{`{"$ref":"#/$defs/a","$defs":{"a":{"oneOf":[{"type":"string"},{"type":"number"}]}}}`, `true`},
		{`{"properties":{"a~b/c":{"const":1}}}`, `{"a~b/c":2}`},
		{`{"items":{"enum":["a","b"]}}`, `["c"]`},
		{`{"not":{}}`, `1`},
		{`{"$ref":"#"}`, `1`},
	} {
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
			t.Fatal(err)
		}
		got := r.Validate(context.Background(), "t", []byte(tc.payload))
		if got == nil {
			t.Fatalf("%s / %s: expected an error", tc.schema, tc.payload)
		}
		var lib *jsonschema.ValidationError
		if !errors.As(got, &lib) {
			t.Fatalf("%s: no ValidationError in chain", tc.schema)
		}
		want := "schema: " + lib.Error()
		if got.Error() != want {
			t.Fatalf("rendering differs from the library's\n got: %s\nwant: %s", got.Error(), want)
		}
	}
}

// TestFindingExponentialRevalidation: the validator memoises nothing,
// so a schema that reaches a value through two paths per nesting
// level validates it 2^depth times (four branches: 1.2 s for a
// 21-byte payload nested ten deep, times four per extra level). Such
// schemas are now refused at registration; ordinary recursive shapes
// are not.
func TestFindingExponentialRevalidation(t *testing.T) {
	r := NewJSONSchema()
	refused := []string{
		`{"allOf":[{"items":{"$ref":"#"}},{"items":{"$ref":"#"}}]}`,
		`{"anyOf":[{"$ref":"#/$defs/a"},{"$ref":"#/$defs/a"}],"$defs":{"a":{"items":{"$ref":"#"}}}}`,
		`{"items":{"anyOf":[{"$ref":"#"},{"$ref":"#"}]}}`,
		`{"items":{"$ref":"#"},"contains":{"$ref":"#"}}`,
		`{"properties":{"a":{"$ref":"#"}},"patternProperties":{"^a$":{"$ref":"#"}}}`,
		`{"properties":{"a":{"$ref":"#"}},"then":{"properties":{"a":{"$ref":"#"}}}}`,
		`{"properties":{"a":{"$ref":"#"}},"not":{"properties":{"a":{"$ref":"#"}}}}`,
		`{"allOf":[{"$ref":"#/$defs/base"},{"properties":{"c":{"items":{"$ref":"#"}}}}],"$defs":{"base":{"properties":{"c":{"items":{"$ref":"#"}}}}}}`,
		// Two laps of different length that meet: S -> S in one level and S -> T -> S in two.
		`{"items":{"anyOf":[{"$ref":"#"},{"$ref":"#/$defs/t"}]},"$defs":{"t":{"items":{"$ref":"#"}}}}`,
		`{"$defs":{"x":{"$anchor":"x","allOf":[{"items":{"$ref":"#x"}},{"items":{"$ref":"#x"}}]}},"$ref":"#x"}`,
		`{"$defs":{"x":{"$dynamicAnchor":"x","allOf":[{"items":{"$dynamicRef":"#x"}},{"items":{"$dynamicRef":"#x"}}]}},"$dynamicRef":"#x"}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"$ref":"#"}],"additionalItems":{"$ref":"#"},"contains":{"$ref":"#"}}`,
	}
	for _, s := range refused {
		err := r.ValidateDefinition(context.Background(), "t", []byte(s))
		if err == nil || !strings.Contains(err.Error(), "more than one path per nesting level") {
			t.Errorf("%s: got %v, want the revalidation refusal", s, err)
		}
	}
	accepted := []string{
		`{"properties":{"left":{"$ref":"#"},"right":{"$ref":"#"}}}`,
		`{"properties":{"a":{"$ref":"#"}},"additionalProperties":{"$ref":"#"}}`,
		`{"properties":{"a":{"$ref":"#"}},"patternProperties":{"^b":{"$ref":"#"}}}`,
		`{"anyOf":[{"type":"null"},{"type":"array","items":{"$ref":"#"}},{"type":"object","additionalProperties":{"$ref":"#"}}]}`,
		`{"$defs":{"a":{"properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"properties":{"a":{"$ref":"#/$defs/a"}}}},"$ref":"#/$defs/a"}`,
		`{"allOf":[{"$ref":"#/$defs/base"}],"properties":{"children":{"items":{"$ref":"#"}}},"$defs":{"base":{"properties":{"id":{"type":"integer"}}}}}`,
		`{"prefixItems":[{"$ref":"#"}],"items":{"$ref":"#"}}`,
		`{"prefixItems":[{"$ref":"#"},{"$ref":"#"}]}`,
		`{"propertyNames":{"$ref":"#"},"items":{"$ref":"#"}}`,
		`{"$ref":"#"}`,
		`{"allOf":[{"$ref":"#/$defs/x"},{"$ref":"#/$defs/x"}],"$defs":{"x":{"type":"integer"}}}`,
		`{"type":"object","properties":{"a":{"$ref":"#"}},"additionalProperties":false}`,
		`{"items":{"$ref":"#"},"if":{"type":"array"},"then":{"minItems":1}}`,
		// Two ways into a cycle that itself has one path per lap is a
		// constant factor, not exponential growth.
		`{"$defs":{"x":{"$anchor":"x","items":{"$ref":"#x"}}},"allOf":[{"$ref":"#x"},{"$ref":"#/$defs/x"}]}`,
	}
	for _, s := range accepted {
		if err := r.ValidateDefinition(context.Background(), "t", []byte(s)); err != nil {
			t.Errorf("%s: unexpected refusal: %v", s, err)
		}
	}
	// Persisted schemas keep loading whatever the analysis says.
	if err := r.Load(context.Background(), "t", 1, []byte(refused[0])); err != nil {
		t.Fatalf("Load of a persisted schema refused: %v", err)
	}
}

// TestFindingCompatibilitySoundness pins the compatibility-check
// acceptances the property test found unsound: each "prev -> next"
// was accepted, and the payload was valid under prev and invalid
// under next. Every case must now be rejected, and the payload must
// really be accepted by prev and refused by next (so the case stays
// a soundness case if the validator changes).
func TestFindingCompatibilitySoundness(t *testing.T) {
	cases := []struct {
		name, prev, next, payload, wantErr string
	}{
		{
			// Bounds were compared as float64, where 2^53 == 2^53+1.
			name:    "minimum tightened by one beyond 2^53",
			prev:    `{"minimum":9007199254740992}`,
			next:    `{"minimum":9007199254740993}`,
			payload: `9007199254740992`,
			wantErr: "lower bound tightened",
		},
		{
			name:    "maximum tightened by one beyond 2^53",
			prev:    `{"maximum":9007199254740993}`,
			next:    `{"maximum":9007199254740992}`,
			payload: `9007199254740993`,
			wantErr: "upper bound tightened",
		},
		{
			name:    "exclusiveMinimum in exponent form is compared exactly",
			prev:    `{"exclusiveMinimum":1e2}`,
			next:    `{"exclusiveMinimum":100.0000000000000001}`,
			payload: `100.00000000000000005`,
			wantErr: "lower bound tightened",
		},
		{
			// multipleOf used a 1e-9 relative tolerance; the validator
			// divides exactly.
			name:    "multipleOf changed within float tolerance",
			prev:    `{"multipleOf":1}`,
			next:    `{"multipleOf":0.9999999999}`,
			payload: `1`,
			wantErr: "does not divide",
		},
		{
			name:    "multipleOf 0.1 to 0.3 (float ratio 0.333...)",
			prev:    `{"multipleOf":0.3}`,
			next:    `{"multipleOf":0.1000000000000000000001}`,
			payload: `0.3`,
			wantErr: "does not divide",
		},
		{
			// const beside enum is a conjunction; the check read const
			// alone and called {const:1, enum:[2]} a superset of {const:1}.
			name:    "enum added beside an unchanged const",
			prev:    `{"const":1}`,
			next:    `{"const":1,"enum":[2]}`,
			payload: `1`,
			wantErr: "not in the new const and enum",
		},
		{
			name:    "enum narrowed beside an unchanged const",
			prev:    `{"enum":[1,2],"const":1}`,
			next:    `{"enum":[2],"const":1}`,
			payload: `1`,
			wantErr: "not in the new const and enum",
		},
		{
			// Opaque keywords were compared as text; the $defs body a
			// $ref inside them points at could change freely.
			name:    "not through $ref whose target widened",
			prev:    `{"not":{"$ref":"#/$defs/x"},"$defs":{"x":{"type":"string"}}}`,
			next:    `{"not":{"$ref":"#/$defs/x"},"$defs":{"x":{}}}`,
			payload: `1`,
			wantErr: `"not" changed`,
		},
		{
			name:    "then through $ref whose target tightened",
			prev:    `{"if":{"type":"integer"},"then":{"$ref":"#/$defs/x"},"$defs":{"x":{}}}`,
			next:    `{"if":{"type":"integer"},"then":{"$ref":"#/$defs/x"},"$defs":{"x":{"minimum":10}}}`,
			payload: `1`,
			wantErr: `"then" changed`,
		},
		{
			name:    "oneOf branch through $ref whose target tightened",
			prev:    `{"oneOf":[{"$ref":"#/$defs/x"}],"$defs":{"x":{"type":"integer"}}}`,
			next:    `{"oneOf":[{"$ref":"#/$defs/x"}],"$defs":{"x":{"type":"integer","minimum":10}}}`,
			payload: `1`,
			wantErr: `"oneOf" changed`,
		},
		{
			name:    "patternProperties schema through $ref whose target tightened",
			prev:    `{"patternProperties":{"^x-":{"$ref":"#/$defs/x"}},"$defs":{"x":{}}}`,
			next:    `{"patternProperties":{"^x-":{"$ref":"#/$defs/x"}},"$defs":{"x":{"type":"string"}}}`,
			payload: `{"x-1":1}`,
			wantErr: `"patternProperties" changed`,
		},
		{
			// A new property whose name matched a previous
			// patternProperties pattern was constrained by that pattern's
			// schema, not by additionalProperties; the check applied the
			// "new optional property" exception to it.
			name:    "new property matching a previous pattern with a narrower schema",
			prev:    `{"patternProperties":{"^x-":{"type":"integer"}}}`,
			next:    `{"patternProperties":{"^x-":{"type":"integer"}},"properties":{"x-1":{"type":"string"}}}`,
			payload: `{"x-1":5}`,
			wantErr: `new property "x-1" must accept everything the previous patternProperties "^x-" schema did`,
		},
		{
			name:    "new property matching a previous pattern under a closed model",
			prev:    `{"patternProperties":{"^x-":{"type":"integer"}},"additionalProperties":false}`,
			next:    `{"patternProperties":{"^x-":{"type":"integer"}},"additionalProperties":false,"properties":{"x-1":{"type":"string"}}}`,
			payload: `{"x-1":5}`,
			wantErr: `new property "x-1" must accept everything`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCompatible([]byte(tc.prev), []byte(tc.next))
			if err == nil {
				t.Fatalf("SOUNDNESS: checkCompatible accepted\nprev: %s\nnext: %s", tc.prev, tc.next)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("checkCompatible() error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
			// The payload really distinguishes the two versions.
			if got := validateWith(t, tc.prev, tc.payload); got != nil {
				t.Fatalf("payload %s is not valid under prev: %v", tc.payload, got)
			}
			if got := validateWith(t, tc.next, tc.payload); got == nil {
				t.Fatalf("payload %s is valid under next; the case no longer shows a narrowing", tc.payload)
			}
		})
	}
}

// TestFindingDraftVocabulary: draft-04 has no const (it arrived in
// draft-06), so the validator ignores it there; the check read it as
// a constraint and accepted {const: -0.75} -> {enum: [-0.75, "added"]},
// which turned an ignored keyword into an enforced one. Keywords the
// draft does not apply are annotations to the check as well.
func TestFindingDraftVocabulary(t *testing.T) {
	d4 := `"$schema":"http://json-schema.org/draft-04/schema#",`
	d6 := `"$schema":"http://json-schema.org/draft-06/schema#",`
	if err := validateWith(t, `{`+d4+`"allOf":[{"const":-0.75}],"type":"integer"}`, `5.0`); err != nil {
		t.Fatalf("draft-04 const is enforced by the validator now: %v", err)
	}
	for _, tc := range []struct{ name, prev, next, wantErr string }{
		{"const to enum under draft-04", `{` + d4 + `"allOf":[{"const":-0.75}],"type":"integer"}`, `{` + d4 + `"allOf":[{"enum":[-0.75,"added"]}],"type":"integer"}`, "allOf branch 0 of the new version is not implied"},
		{"const to enum under draft-04 at the root", `{` + d4 + `"const":-0.75,"type":"integer"}`, `{` + d4 + `"enum":[-0.75,"added"],"type":"integer"}`, "enum added where the previous version accepted any value"},
		{"const to const under draft-04 is a no-op", `{` + d4 + `"const":1}`, `{` + d4 + `"const":2}`, ""},
		{"const to enum under draft-06 is a widening", `{` + d6 + `"const":1}`, `{` + d6 + `"enum":[1,2]}`, ""},
		{"if/then under draft-06 are annotations", `{` + d6 + `"if":{"type":"string"},"then":{"minLength":1}}`, `{` + d6 + `"if":{"type":"integer"},"then":{"minimum":5}}`, ""},
		{"if/then under draft-07 are opaque", `{"$schema":"http://json-schema.org/draft-07/schema#","if":{"type":"string"},"then":{"minLength":1}}`, `{"$schema":"http://json-schema.org/draft-07/schema#","if":{"type":"integer"},"then":{"minimum":5}}`, `"if" changed`},
		{"prefixItems under 2019-09 is an annotation", `{"$schema":"https://json-schema.org/draft/2019-09/schema","items":{"type":"integer"}}`, `{"$schema":"https://json-schema.org/draft/2019-09/schema","items":{"type":"integer"},"prefixItems":[{"type":"string"}]}`, ""},
		{"additionalItems under 2020-12 is an annotation", `{"items":{"type":"integer"}}`, `{"items":{"type":"integer"},"additionalItems":false}`, ""},
		{"additionalItems under draft-07 is opaque", `{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"type":"integer"}]}`, `{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"type":"integer"}],"additionalItems":false}`, "additionalItems"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCompatible([]byte(tc.prev), []byte(tc.next))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("rejected: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("SOUNDNESS: accepted\nprev: %s\nnext: %s", tc.prev, tc.next)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestFindingReferenceCycleThroughNot: the validator fails a $ref
// cycle at the node it visits twice with the same value, so an
// inlined copy of a self-referencing subschema fails one lap later;
// under "not" that failure is a pass, and {"p0": [-19]} was valid
// against {"$ref": "#/$defs/d0"} but not against d0's inlined body,
// which the check (rightly, in every other respect) treats as the
// same schema. Such cycles are now refused at registration; cycles
// through allOf/anyOf/then/else are entry-independent and still
// register.
func TestFindingReferenceCycleThroughNot(t *testing.T) {
	r := NewJSONSchema()
	d0 := `{"items":{"type":"number"},"maxItems":3,"not":{"$ref":"#/$defs/d0"},"type":["array","number"]}`
	old := `{"$defs":{"d0":` + d0 + `},"properties":{"p0":{"$ref":"#/$defs/d0"}},"required":["p0"],"type":"object"}`
	next := `{"$defs":{"d0":` + d0 + `},"properties":{"p0":` + d0 + `},"required":["p0"],"type":"object"}`
	if validateWith(t, old, `{"p0":[-19]}`) != nil || validateWith(t, next, `{"p0":[-19]}`) == nil {
		t.Fatalf("the validator no longer distinguishes a $ref from its inlined copy here; the finding is moot")
	}
	for _, s := range []string{
		old,
		`{"not":{"$ref":"#"}}`,
		`{"$defs":{"a":{"if":{"$ref":"#/$defs/a"},"then":{"type":"string"}}},"$ref":"#/$defs/a"}`,
		`{"oneOf":[{"$ref":"#"},{"type":"integer"}]}`,
		`{"$defs":{"a":{"anyOf":[{"not":{"allOf":[{"$ref":"#/$defs/a"}]}}]}},"$ref":"#/$defs/a"}`,
	} {
		err := r.ValidateDefinition(context.Background(), "t", []byte(s))
		if err == nil || !strings.Contains(err.Error(), "refers back to the value already being validated") {
			t.Errorf("%s: got %v, want the reference-cycle refusal", s, err)
		}
	}
	for _, s := range []string{
		`{"anyOf":[{"$ref":"#"},{"type":"integer"}]}`,
		`{"allOf":[{"$ref":"#"}]}`,
		`{"$ref":"#"}`,
		`{"then":{"$ref":"#"},"if":{"type":"string"}}`,
		`{"not":{"items":{"$ref":"#"}}}`,
		`{"not":{"type":"string"},"properties":{"a":{"$ref":"#"}}}`,
		`{"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}},"properties":{"q":{"$ref":"#/$defs/a"}}}`,
	} {
		if err := r.ValidateDefinition(context.Background(), "t", []byte(s)); err != nil {
			t.Errorf("%s: unexpected refusal: %v", s, err)
		}
	}
}

// TestFindingCompatibilityCompleteness pins the documented widenings
// the check used to reject.
func TestFindingCompatibilityCompleteness(t *testing.T) {
	cases := []struct {
		name, prev, next string
	}{
		{
			// A recursive schema hit the depth limit against itself, so
			// no recursive schema could ever be evolved.
			name: "recursive schema identical to itself",
			prev: `{"properties":{"c":{"$ref":"#"}}}`,
			next: `{"properties":{"c":{"$ref":"#"}}}`,
		},
		{
			name: "recursive schema widened",
			prev: `{"type":"object","properties":{"c":{"$ref":"#"},"n":{"type":"integer"}},"required":["n"]}`,
			next: `{"type":"object","properties":{"c":{"$ref":"#"},"n":{"type":"number"}}}`,
		},
		{
			name: "mutually recursive $defs widened",
			prev: `{"$ref":"#/$defs/a","$defs":{"a":{"type":"object","properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}},"required":["a"]}}}`,
			next: `{"$ref":"#/$defs/a","$defs":{"a":{"type":"object","properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}}}}}`,
		},
		{
			name: "recursion through anyOf and items",
			prev: `{"anyOf":[{"type":"integer"},{"type":"array","items":{"$ref":"#"}}]}`,
			next: `{"anyOf":[{"type":"number"},{"type":"array","items":{"$ref":"#"}}]}`,
		},
		{
			// The root $id and $schema are not "sibling keywords" of a
			// root $ref.
			name: "root $ref beside $id and $schema",
			prev: `{"$id":"https://example.com/s","$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"#/$defs/a","$defs":{"a":{"type":"integer"}}}`,
			next: `{"$id":"https://example.com/s","$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"#/$defs/a","$defs":{"a":{"type":"number"}}}`,
		},
		{
			name: "new property matching a previous pattern with a wider schema",
			prev: `{"patternProperties":{"^x-":{"type":"integer"}}}`,
			next: `{"patternProperties":{"^x-":{"type":"integer"}},"properties":{"x-1":{"type":"number"}}}`,
		},
		{
			// Comparing the previous additionalProperties schema with the
			// new property's schema true walked keyword by keyword and
			// called the vanished "properties" a removal; a nested
			// subschema that accepts everything is wider than anything.
			name: "new property true under a previous additionalProperties schema with properties",
			prev: `{"type":"object","additionalProperties":{"type":"object","properties":{"h":{"type":"object"}},"required":["h"],"additionalProperties":false}}`,
			next: `{"type":"object","additionalProperties":{"type":"object","properties":{"h":{"type":"object"}},"required":["h"],"additionalProperties":false},"properties":{"new_1":true}}`,
		},
		{
			name: "property schema widened to true",
			prev: `{"properties":{"p":{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}}}`,
			next: `{"properties":{"p":{"title":"anything"}}}`,
		},
		{
			name: "items widened to true",
			prev: `{"items":{"properties":{"q":{"type":"string"}}}}`,
			next: `{"items":true}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkCompatible([]byte(tc.prev), []byte(tc.next)); err != nil {
				t.Fatalf("documented widening rejected: %v\nprev: %s\nnext: %s", err, tc.prev, tc.next)
			}
		})
	}
	// And the recursive narrowings are still caught.
	for _, tc := range []struct{ name, prev, next, wantErr string }{
		{
			"recursive schema narrowed",
			`{"type":"object","properties":{"c":{"$ref":"#"},"n":{"type":"number"}}}`,
			`{"type":"object","properties":{"c":{"$ref":"#"},"n":{"type":"integer"}}}`,
			`type "number" no longer allowed`,
		},
		{
			"mutually recursive $defs narrowed",
			`{"$ref":"#/$defs/a","$defs":{"a":{"type":"object","properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}}}}}`,
			`{"$ref":"#/$defs/a","$defs":{"a":{"type":"object","properties":{"b":{"$ref":"#/$defs/b"}}},"b":{"type":"object","properties":{"a":{"$ref":"#/$defs/a"}},"required":["a"]}}}`,
			`required property "a" added`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCompatible([]byte(tc.prev), []byte(tc.next))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("checkCompatible() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func validateWith(t *testing.T, schema, payload string) error {
	t.Helper()
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(schema)); err != nil {
		t.Fatalf("Load(%s): %v", schema, err)
	}
	return r.Validate(context.Background(), "t", []byte(payload))
}
