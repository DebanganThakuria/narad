package schema

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// loadOne registers sch as v1 of topic "t" in a fresh registry.
func loadOne(t *testing.T, sch string) *JSONSchema {
	t.Helper()
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(sch)); err != nil {
		t.Fatalf("Load(%s): %v", sch, err)
	}
	return r
}

func repeat(s string, n int) string { return strings.Repeat(s, n) }

// TestValidatePayloadEdgeCases pins the produce-time contract for every
// payload shape a client can send to a schema topic.
func TestValidatePayloadEdgeCases(t *testing.T) {
	object := `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}},"meta":{"type":"object","properties":{"x":{"type":"number"}}}},"required":["id"],"additionalProperties":false}`
	tests := []struct {
		name    string
		schema  string
		payload string
		wantErr string // substring of the error; empty means valid
	}{
		{"valid object", object, `{"id":1,"name":"a","tags":["x"],"meta":{"x":1.5}}`, ""},
		{"nested type violation names the path", object, `{"id":1,"meta":{"x":"no"}}`, "/meta/x"},
		{"array item violation", object, `{"id":1,"tags":[1]}`, "/tags/0"},
		{"missing required", object, `{"name":"a"}`, "missing property 'id'"},
		{"extra property under closed model", object, `{"id":1,"zzz":1}`, "additional properties 'zzz'"},
		{"non-JSON text", object, `hello world`, "invalid JSON payload"},
		{"binary bytes", object, "\x00\x01\x02\xff", "invalid JSON payload"},
		{"empty body", object, ``, "invalid JSON payload"},
		{"whitespace only", object, ` `, "invalid JSON payload"},
		{"trailing garbage after value", object, `{"id":1} {"id":2}`, "invalid JSON payload"},
		{"BOM is not JSON", object, "\xef\xbb\xbf{\"id\":1}", "invalid JSON payload"},
		{"raw NUL inside a string", object, "{\"id\":1,\"name\":\"a\x00b\"}", "invalid JSON payload"},
		{"invalid UTF-8 inside a string", object, "{\"id\":1,\"name\":\"\xff\xfe\"}", "not valid UTF-8"},
		{"escaped unicode and surrogate pair", object, `{"id":1,"name":"é😀"}`, ""},
		{"raw emoji counts as one character", `{"type":"string","maxLength":1}`, `"😀"`, ""},
		{"maxLength counts runes not bytes", `{"type":"string","maxLength":2}`, `"éé"`, ""},
		{"duplicate keys: last one wins", `{"properties":{"a":{"type":"integer"}}}`, `{"a":"s","a":1}`, ""},
		{"duplicate keys: last one loses", `{"properties":{"a":{"type":"integer"}}}`, `{"a":1,"a":"s"}`, "/a"},
		{"scalar payload against object schema", object, `123`, "got number, want object"},
		{"null payload", `{"type":"object"}`, `null`, "got null, want object"},
		{"true schema accepts any JSON", `true`, `[1,"two",null]`, ""},
		{"true schema still needs JSON", `true`, `not json`, "invalid JSON payload"},
		{"empty object schema accepts any JSON", `{}`, `"x"`, ""},
		{"false schema rejects everything", `false`, `1`, "validation failed"},
		{"1.0 is an integer", `{"type":"integer"}`, `1.0`, ""},
		{"1e2 is an integer", `{"type":"integer"}`, `1e2`, ""},
		{"1.5 is not an integer", `{"type":"integer"}`, `1.5`, "want integer"},
		{"2^53 exactly", `{"type":"integer","multipleOf":2}`, `9007199254740992`, ""},
		{"2^53+1 is odd and exact", `{"type":"integer","multipleOf":2}`, `9007199254740993`, "multipleOf"},
		{"2^63 maximum is exact", `{"type":"integer","maximum":9223372036854775807}`, `9223372036854775808`, "maximum"},
		{"2^63-1 within maximum", `{"type":"integer","maximum":9223372036854775807}`, `9223372036854775807`, ""},
		{"30-digit integer is an integer", `{"type":"integer"}`, `123456789012345678901234567890`, ""},
		{"1e400 exceeds float64 but is a number", `{"type":"number"}`, `1e400`, ""},
		{"1e400 above a maximum", `{"type":"number","maximum":1e308}`, `1e400`, "maximum"},
		{"negative zero", `{"type":"integer","minimum":0}`, `-0`, ""},
		{"minimum exclusive boundary", `{"exclusiveMinimum":5}`, `5`, "exclusiveMinimum"},
		{"deeply nested valid", `{"type":"object","properties":{"a":{"$ref":"#"}}}`, repeat(`{"a":`, 9000) + `{}` + repeat(`}`, 9000), ""},
		{"nesting beyond the decoder limit", `{"type":"array"}`, repeat(`[`, 10001) + repeat(`]`, 10001), "exceeded max depth"},
		{"self-referencing root schema on a scalar", `{"$ref":"#"}`, `1`, "validation failed"},
		{"mutually recursive $defs", `{"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}},"$ref":"#/$defs/a"}`, `1`, "validation failed"},
		{"$anchor reference", `{"$defs":{"x":{"$anchor":"xa","type":"integer"}},"properties":{"a":{"$ref":"#xa"}}}`, `{"a":"s"}`, "/a"},
		{"$dynamicRef reference", `{"$defs":{"x":{"$dynamicAnchor":"dyn","type":"integer"}},"properties":{"b":{"$dynamicRef":"#dyn"}}}`, `{"b":"t"}`, "/b"},
		{"$id does not load anything", `{"$id":"file:///etc/passwd","type":"integer"}`, `1`, ""},
		{"pattern uses RE2 (linear time)", `{"type":"string","pattern":"^(a+)+$"}`, `"` + repeat("a", 50000) + `!"`, "validation failed"},
		{"pattern is unanchored by default", `{"type":"string","pattern":"b"}`, `"abc"`, ""},
		{"format asserted on the default 2020-12 draft", `{"type":"string","format":"email"}`, `"not-an-email"`, "email"},
		{"format asserted on 2019-09", `{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"string","format":"email"}`, `"not-an-email"`, "email"},
		{"format asserted on draft-07", `{"$schema":"http://json-schema.org/draft-07/schema#","type":"string","format":"email"}`, `"not-an-email"`, "email"},
		{"format passes a valid value", `{"type":"string","format":"date-time"}`, `"2026-09-06T10:00:00Z"`, ""},
		{"unknown format is ignored", `{"type":"string","format":"no-such-format"}`, `"anything"`, ""},
		{"format only applies to strings", `{"format":"email"}`, `42`, ""},
		{"2020-12 prefixItems", `{"type":"array","prefixItems":[{"type":"string"}]}`, `[1]`, "/0"},
		{"2019-09 array-form items", `{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"array","items":[{"type":"string"}]}`, `[1]`, "/0"},
		{"draft-04 boolean exclusiveMinimum", `{"$schema":"http://json-schema.org/draft-04/schema#","type":"integer","minimum":1,"exclusiveMinimum":true}`, `1`, "validation failed"},
		{"draft-07 const", `{"$schema":"http://json-schema.org/draft-07/schema#","const":"a"}`, `"b"`, "value must be"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := loadOne(t, tc.schema)
			err := r.Validate(context.Background(), "t", []byte(tc.payload))
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("Validate(%q) = %v, want valid", tc.payload, err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("Validate(%q) accepted, want error containing %q", tc.payload, tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("Validate(%q) = %v, want error containing %q", tc.payload, err, tc.wantErr)
			}
		})
	}
}

// A payload at the produce size limit validates in bounded time.
func TestValidateOneMiBPayload(t *testing.T) {
	r := loadOne(t, `{"type":"array","items":{"type":"integer","minimum":0}}`)
	payload := "[" + repeat("1,", 524287) + "1]" // 1 MiB
	start := time.Now()
	if err := r.Validate(context.Background(), "t", []byte(payload)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// A hang guard, not a benchmark: CI runs this under -race on a busy
	// runner, where 1 MiB of JSON decoding legitimately takes seconds.
	if d := time.Since(start); d > 60*time.Second {
		t.Fatalf("1 MiB payload took %v", d)
	}
}

// A very large enum validates and its rejection message is capped.
func TestValidateLargeEnumBoundsTheErrorMessage(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"enum":[`)
	for i := 0; i < 12000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"value-`)
		sb.WriteString(strings.Repeat("x", 8))
		sb.WriteString(string(rune('a' + i%26)))
		sb.WriteString(`"`)
	}
	sb.WriteString(`]}`)
	if len(sb.String()) > MaxSchemaBytes {
		t.Fatalf("test schema is %d bytes, over MaxSchemaBytes", sb.Len())
	}
	r := NewJSONSchema()
	if err := r.ValidateDefinition(context.Background(), "t", []byte(sb.String())); err != nil {
		t.Fatalf("ValidateDefinition: %v", err)
	}
	if err := r.Load(context.Background(), "t", 1, []byte(sb.String())); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := r.Validate(context.Background(), "t", []byte(`"value-xxxxxxxxa"`)); err != nil {
		t.Fatalf("member rejected: %v", err)
	}
	err := r.Validate(context.Background(), "t", []byte(`"nope"`))
	if err == nil {
		t.Fatal("non-member accepted")
	}
	if n := len(err.Error()); n > maxValidationErrorBytes+64 {
		t.Fatalf("error message is %d bytes, want at most about %d", n, maxValidationErrorBytes)
	}
	if !strings.Contains(err.Error(), "more bytes)") {
		t.Fatalf("truncated error should say how much was cut: %q", err.Error())
	}
	var truncated *truncatedError
	if !errors.As(err, &truncated) {
		t.Fatal("truncated error should be reachable through the chain")
	}
}

// TestValidateDefinitionLimits pins what a topic owner may register.
func TestValidateDefinitionLimits(t *testing.T) {
	r := NewJSONSchema()
	check := func(name, sch, wantErr string) {
		t.Helper()
		err := r.ValidateDefinition(context.Background(), "t", []byte(sch))
		switch {
		case wantErr == "" && err != nil:
			t.Errorf("%s: %v, want accepted", name, err)
		case wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)):
			t.Errorf("%s: %v, want error containing %q", name, err, wantErr)
		}
	}
	check("object", `{"type":"object"}`, "")
	check("true", `true`, "")
	check("leading whitespace", "\n  {\"type\":\"object\"}", "")
	check("false", `false`, "would reject every message")
	check("null", `null`, "must be a JSON object")
	check("string", `"x"`, "must be a JSON object")
	check("number", `1`, "must be a JSON object")
	check("array", `[]`, "must be a JSON object")
	check("empty", ``, "is empty")
	check("whitespace", "  ", "is empty")
	check("malformed", `{"type":`, "invalid JSON")
	check("bad type value", `{"type":123}`, "metaschema")
	check("at the depth limit", repeat(`{"properties":{"a":`, MaxSchemaDepth/2-1)+`{"items":{}}`+repeat(`}}`, MaxSchemaDepth/2-1), "")
	check("one past the depth limit", repeat(`{"properties":{"a":`, MaxSchemaDepth/2-1)+`{"items":{"items":{}}}`+repeat(`}}`, MaxSchemaDepth/2-1), "nests")
	check("10k deep", repeat(`{"properties":{"a":`, 5000)+`{}`+repeat(`}}`, 5000), "nests")
	check("brackets inside strings do not count", `{"description":"`+repeat(`[{`, 500)+`","type":"object"}`, "")
	check("escaped quote inside string", `{"description":"a\"`+repeat(`[`, 500)+`","type":"object"}`, "")
	check("over the size limit", `{"description":"`+repeat("x", MaxSchemaBytes)+`"}`, "bytes; the maximum")
	check("wide schema under the limit", func() string {
		var sb strings.Builder
		sb.WriteString(`{"type":"object","properties":{`)
		for i := 0; i < 5000; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(`"p` + strings.Repeat("0", 4) + string(rune('a'+i%26)) + strings.Repeat("y", i%7) + `":{"type":"integer"}`)
		}
		sb.WriteString(`}}`)
		return sb.String()
	}(), "")
	check("lookahead is not RE2", `{"type":"string","pattern":"^(?=a)a$"}`, "metaschema")
	check("external $ref", `{"$ref":"file:///etc/passwd"}`, "external $ref")
	check("relative $ref against $id", `{"$id":"https://example.com/root","$ref":"other.json"}`, "external $ref")
	check("unknown $schema", `{"$schema":"https://example.com/my-draft","type":"integer"}`, "external $ref")
	check("$id, $anchor and $dynamicRef compile", `{"$id":"https://example.com/root","$defs":{"x":{"$anchor":"xa","$dynamicAnchor":"dyn","type":"integer"}},"properties":{"a":{"$ref":"#xa"},"b":{"$dynamicRef":"#dyn"}}}`, "")
	check("duplicate keys in a schema: last wins", `{"type":"string","type":"integer"}`, "")
}

// Registration limits never apply to persisted history: a schema that
// predates a limit must still hydrate.
func TestReplaceTopicIgnoresRegistrationLimits(t *testing.T) {
	r := NewJSONSchema()
	deep := repeat(`{"properties":{"a":`, 100) + `{}` + repeat(`}}`, 100)
	if err := r.ValidateDefinition(context.Background(), "t", []byte(deep)); err == nil {
		t.Fatal("deep schema should be refused at registration")
	}
	if err := r.ReplaceTopic(context.Background(), "t", []Version{{Number: 1, Raw: []byte(deep)}}); err != nil {
		t.Fatalf("ReplaceTopic of a persisted deep schema: %v", err)
	}
	if err := r.Validate(context.Background(), "t", []byte(`{}`)); err != nil {
		t.Fatalf("Validate after hydrate: %v", err)
	}
}

// ReplaceTopic compiles only the latest version; an older version that
// no longer compiles must not block a hydrate.
func TestReplaceTopicCompilesOnlyTheLatestVersion(t *testing.T) {
	r := NewJSONSchema()
	history := []Version{
		{Number: 1, Raw: []byte(`{"type":`)}, // corrupt, never consulted
		{Number: 2, Raw: []byte(`{"type":"integer"}`)},
	}
	if err := r.ReplaceTopic(context.Background(), "t", history); err != nil {
		t.Fatalf("ReplaceTopic: %v", err)
	}
	if err := r.Validate(context.Background(), "t", []byte(`"s"`)); err == nil {
		t.Fatal("latest version not enforced")
	}
	if err := r.ReplaceTopic(context.Background(), "t", []Version{{Number: 1, Raw: []byte(`{"type":`)}}); err == nil {
		t.Fatal("a corrupt latest version must fail the hydrate")
	}
	if err := r.Validate(context.Background(), "t", []byte(`"s"`)); err == nil {
		t.Fatal("failed hydrate must leave the previous history in place")
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{`{"type":"object","properties":{"a":{"type":"integer"}}}`, `{ "properties": {"a": {"type": "integer"}}, "type": "object" }`, true},
		{`{"minimum":1}`, `{"minimum":1.0}`, true},
		{`{"minimum":9007199254740993}`, `{"minimum":9007199254740992}`, false},
		{`{"enum":[1,2]}`, `{"enum":[2,1]}`, false},
		{`{"type":"object"}`, `{"type":"object","title":"x"}`, false},
		{`true`, `true`, true},
		{`{}`, `true`, false},
		{`{bad`, `{bad`, false},
	}
	for _, tc := range tests {
		if got := Equal([]byte(tc.a), []byte(tc.b)); got != tc.want {
			t.Errorf("Equal(%s, %s) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestJSONNestingDepth(t *testing.T) {
	tests := map[string]int{
		`1`:                      0,
		`{}`:                     1,
		`{"a":[1,[2]]}`:          3,
		`{"a":"[[[["}`:           1,
		`{"a":"\"[","b":[]}`:     2,
		`{"a":"\\","b":[[]]}`:    3,
		`[[[`:                    3,
		`{"a":{"b":{"c":{}}}}`:   4,
		`{"a":"[[","b":[]}`:      2,
		"{\n\t\"a\": [ { } ]\n}": 3,
	}
	for doc, want := range tests {
		if got := jsonNestingDepth([]byte(doc)); got != want {
			t.Errorf("jsonNestingDepth(%q) = %d, want %d", doc, got, want)
		}
	}
}

// The benchmark payload is a realistic event with the nesting a typical
// schema constrains: numbers, strings, an array, and a nested object.
var benchPayload = []byte(`{"id":123456,"sku":"ABC-123","qty":4,"price":19.99,"tags":["a","b","c"],"meta":{"source":"web","attempt":1,"nested":{"x":1.5,"y":[1,2,3]}},"note":"hello world hello world hello world"}`)

const benchSchema = `{"type":"object","required":["id","sku"],"properties":{"id":{"type":"integer"},"sku":{"type":"string","pattern":"^[A-Z]+-[0-9]+$"},"qty":{"type":"integer","minimum":0},"price":{"type":"number"},"tags":{"type":"array","items":{"type":"string"}},"meta":{"type":"object","properties":{"attempt":{"type":"integer"}}}}}`

// BenchmarkValidateWithSchema is the per-produce cost a schema topic
// pays on top of a schema-less one (BenchmarkValidateWithoutSchema).
func BenchmarkValidateWithSchema(b *testing.B) {
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(benchSchema)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	for i := 0; i < b.N; i++ {
		if err := r.Validate(context.Background(), "t", benchPayload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkValidateWithoutSchema is the registry lookup a topic with no
// schema pays: one RLock and a map miss, no decode.
func BenchmarkValidateWithoutSchema(b *testing.B) {
	r := NewJSONSchema()
	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	for i := 0; i < b.N; i++ {
		if err := r.Validate(context.Background(), "t", benchPayload); !errors.Is(err, ErrSchemaNotFound) {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateRejected(b *testing.B) {
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(benchSchema)); err != nil {
		b.Fatal(err)
	}
	bad := []byte(`{"id":"not-an-int","sku":"ABC-123"}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := r.Validate(context.Background(), "t", bad); err == nil {
			b.Fatal("accepted")
		}
	}
}
