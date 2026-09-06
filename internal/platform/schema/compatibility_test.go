package schema

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// TestCheckCompatible pins the allowlist documented on checkCompatible:
// every widening it understands passes, every narrowing it understands
// is rejected with a message naming the keyword, and every construct
// outside the allowlist fails closed unless it is unchanged.
func TestCheckCompatible(t *testing.T) {
	cases := []struct {
		name    string
		prev    string
		next    string
		wantErr string // substring of the error; "" means compatible
	}{
		// ---- widenings that pass -------------------------------------------
		{
			name: "identical schema",
			prev: `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`,
			next: `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`,
		},
		{
			name: "annotations may change freely",
			prev: `{"title":"v1","type":"object","properties":{"id":{"type":"string","description":"old"}}}`,
			next: `{"title":"v2","$comment":"x","type":"object","properties":{"id":{"type":"string","description":"new","examples":["a"]}}}`,
		},
		{
			name: "adding an optional property under an open model (documented exception)",
			prev: `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}`,
			next: `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"email":{"type":"string"}},"required":["id"]}`,
		},
		{
			name: "adding an optional property under a closed model",
			prev: `{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}`,
			next: `{"type":"object","properties":{"id":{"type":"integer"},"n":{"type":"integer"}},"additionalProperties":false}`,
		},
		{
			name: "new property wider than the old additionalProperties schema",
			prev: `{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":{"type":"string","maxLength":3}}`,
			next: `{"type":"object","properties":{"id":{"type":"integer"},"tag":{"type":"string"}},"additionalProperties":{"type":"string","maxLength":3}}`,
		},
		{
			name: "widening a type union",
			prev: `{"properties":{"id":{"type":["string","null"]}}}`,
			next: `{"properties":{"id":{"type":["string","null","number"]}}}`,
		},
		{
			name: "integer widens to number",
			prev: `{"properties":{"qty":{"type":"integer"}}}`,
			next: `{"properties":{"qty":{"type":"number"}}}`,
		},
		{
			name: "dropping type",
			prev: `{"properties":{"qty":{"type":"integer"}}}`,
			next: `{"properties":{"qty":{}}}`,
		},
		{
			name: "enum superset",
			prev: `{"properties":{"s":{"enum":["a","b"]}}}`,
			next: `{"properties":{"s":{"enum":["a","b","c"]}}}`,
		},
		{
			name: "enum with numbers compares numerically",
			prev: `{"properties":{"n":{"enum":[1,2.5]}}}`,
			next: `{"properties":{"n":{"enum":[1.0,2.50,3]}}}`,
		},
		{
			name: "const widens to enum containing it",
			prev: `{"properties":{"s":{"const":"a"}}}`,
			next: `{"properties":{"s":{"enum":["a","b"]}}}`,
		},
		{
			name: "enum of one widens to const of the same value",
			prev: `{"properties":{"s":{"enum":["a"]}}}`,
			next: `{"properties":{"s":{"const":"a"}}}`,
		},
		{
			name: "dropping enum",
			prev: `{"properties":{"s":{"type":"string","enum":["a"]}}}`,
			next: `{"properties":{"s":{"type":"string"}}}`,
		},
		{
			name: "loosening minimum and maximum",
			prev: `{"properties":{"n":{"type":"number","minimum":5,"maximum":10}}}`,
			next: `{"properties":{"n":{"type":"number","minimum":1,"maximum":100}}}`,
		},
		{
			name: "exclusiveMinimum becomes inclusive at the same value",
			prev: `{"properties":{"n":{"exclusiveMinimum":5}}}`,
			next: `{"properties":{"n":{"minimum":5}}}`,
		},
		{
			name: "inclusive maximum becomes exclusive at a higher value",
			prev: `{"properties":{"n":{"maximum":5}}}`,
			next: `{"properties":{"n":{"exclusiveMaximum":6}}}`,
		},
		{
			name: "dropping bounds",
			prev: `{"properties":{"n":{"minimum":5,"maximum":10,"multipleOf":2}}}`,
			next: `{"properties":{"n":{}}}`,
		},
		{
			name: "multipleOf to a divisor",
			prev: `{"properties":{"n":{"multipleOf":10}}}`,
			next: `{"properties":{"n":{"multipleOf":5}}}`,
		},
		{
			name: "minLength decreases, maxLength increases",
			prev: `{"properties":{"s":{"type":"string","minLength":2,"maxLength":5}}}`,
			next: `{"properties":{"s":{"type":"string","minLength":1,"maxLength":50}}}`,
		},
		{
			name: "minLength 0 may appear (vacuous)",
			prev: `{"properties":{"s":{"type":"string"}}}`,
			next: `{"properties":{"s":{"type":"string","minLength":0}}}`,
		},
		{
			name: "dropping pattern, format, uniqueItems and required",
			prev: `{"properties":{"s":{"type":"string","pattern":"^a","format":"email"},"a":{"type":"array","uniqueItems":true}},"required":["s"]}`,
			next: `{"properties":{"s":{"type":"string"},"a":{"type":"array"}}}`,
		},
		{
			name: "identical pattern kept",
			prev: `{"properties":{"s":{"pattern":"^a"}}}`,
			next: `{"properties":{"s":{"pattern":"^a"}}}`,
		},
		{
			name: "nested object widened",
			prev: `{"properties":{"c":{"type":"object","properties":{"tier":{"type":"integer","minimum":1}},"required":["tier"]}}}`,
			next: `{"properties":{"c":{"type":"object","properties":{"tier":{"type":"number","minimum":0},"vip":{"type":"boolean"}}}}}`,
		},
		{
			name: "items widened and dropped",
			prev: `{"properties":{"a":{"items":{"type":"integer"}},"b":{"items":{"type":"string"}}}}`,
			next: `{"properties":{"a":{"items":{"type":"number"}},"b":{}}}`,
		},
		{
			name: "closed model opens",
			prev: `{"properties":{"id":{"type":"string"}},"additionalProperties":false}`,
			next: `{"properties":{"id":{"type":"string"}}}`,
		},
		{
			name: "additionalProperties schema widened",
			prev: `{"additionalProperties":{"type":"string"}}`,
			next: `{"additionalProperties":{"type":["string","null"]}}`,
		},
		{
			name: "additionalProperties false widened to a schema",
			prev: `{"additionalProperties":false}`,
			next: `{"additionalProperties":{"type":"string"}}`,
		},
		{
			name: "anyOf gains a branch",
			prev: `{"anyOf":[{"type":"string"},{"type":"integer"}]}`,
			next: `{"anyOf":[{"type":"string"},{"type":"number"},{"type":"null"}]}`,
		},
		{
			name: "allOf loses a conjunct",
			prev: `{"allOf":[{"type":"object"},{"required":["id"]}]}`,
			next: `{"allOf":[{"type":"object"}]}`,
		},
		{
			name: "droppable opaque keywords removed",
			prev: `{"oneOf":[{"type":"string"}],"not":{"const":"x"},"if":{"type":"string"},"then":{"minLength":1},"propertyNames":{"pattern":"^a"},"dependentRequired":{"a":["b"]},"contains":{"type":"string"}}`,
			next: `{}`,
		},
		{
			name: "opaque keyword kept identical",
			prev: `{"type":"object","patternProperties":{"^x-":{"type":"string"}},"oneOf":[{"required":["a"]},{"required":["b"]}]}`,
			next: `{"type":"object","patternProperties":{"^x-":{"type":"string"}},"oneOf":[{"required":["a"]},{"required":["b"]}],"properties":{"a":{"type":"string"}}}`,
		},
		{
			name: "patternProperties dropped while additional properties stay open",
			prev: `{"type":"object","patternProperties":{"^x-":{"type":"string"}}}`,
			next: `{"type":"object"}`,
		},
		{
			name: "unevaluatedProperties kept in an identical subschema",
			prev: `{"type":"object","properties":{"a":{"type":"string"}},"unevaluatedProperties":false}`,
			next: `{"type":"object","properties":{"a":{"type":"string"}},"unevaluatedProperties":false}`,
		},
		{
			name: "in-document $ref resolved on both sides",
			prev: `{"properties":{"q":{"$ref":"#/$defs/qty"}},"$defs":{"qty":{"type":"integer","minimum":1}}}`,
			next: `{"properties":{"q":{"$ref":"#/$defs/quantity"}},"$defs":{"quantity":{"type":"number","minimum":0}}}`,
		},
		{
			name: "$ref inlined in the new version",
			prev: `{"properties":{"q":{"$ref":"#/$defs/qty"}},"$defs":{"qty":{"type":"integer"}}}`,
			next: `{"properties":{"q":{"type":"integer"}}}`,
		},
		{
			name: "$ref with escaped pointer tokens",
			prev: `{"properties":{"q":{"$ref":"#/$defs/a~1b"}},"$defs":{"a/b":{"type":"string"}}}`,
			next: `{"properties":{"q":{"type":["string","null"]}}}`,
		},
		{
			name: "boolean true subschema is the open schema",
			prev: `{"properties":{"q":{"type":"string"}}}`,
			next: `{"properties":{"q":true}}`,
		},
		{
			name: "previous false subschema accepts nothing so anything is wider",
			prev: `{"properties":{"q":false}}`,
			next: `{"properties":{"q":{"type":"string"}}}`,
		},
		{
			name: "same draft spelled with and without trailing #",
			prev: `{"$schema":"https://json-schema.org/draft/2020-12/schema#","type":"object"}`,
			next: `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}`,
		},
		{
			name: "root $id is an annotation",
			prev: `{"$id":"https://example.com/a","type":"object"}`,
			next: `{"$id":"https://example.com/b","type":"object"}`,
		},

		// ---- narrowings the check understands and rejects ---------------------
		{
			name:    "property removed",
			prev:    `{"properties":{"id":{"type":"integer"},"name":{"type":"string"}}}`,
			next:    `{"properties":{"id":{"type":"integer"}}}`,
			wantErr: `property "name" removed`,
		},
		{
			name:    "required property added",
			prev:    `{"properties":{"id":{"type":"string"},"region":{"type":"string"}}}`,
			next:    `{"properties":{"id":{"type":"string"},"region":{"type":"string"}},"required":["region"]}`,
			wantErr: `required property "region" added`,
		},
		{
			name:    "type changed",
			prev:    `{"properties":{"id":{"type":"integer"}}}`,
			next:    `{"properties":{"id":{"type":"string"}}}`,
			wantErr: `at /properties/id: type "integer" no longer allowed`,
		},
		{
			name:    "type union narrowed",
			prev:    `{"properties":{"id":{"type":["string","null"]}}}`,
			next:    `{"properties":{"id":{"type":["string"]}}}`,
			wantErr: `type "null" no longer allowed`,
		},
		{
			name:    "number narrowed to integer",
			prev:    `{"properties":{"n":{"type":"number"}}}`,
			next:    `{"properties":{"n":{"type":"integer"}}}`,
			wantErr: `type "number" no longer allowed`,
		},
		{
			name:    "type added where none existed",
			prev:    `{"properties":{"n":{}}}`,
			next:    `{"properties":{"n":{"type":"string"}}}`,
			wantErr: "type restricted",
		},
		{
			name:    "root type changed",
			prev:    `{"type":"object"}`,
			next:    `{"type":"array"}`,
			wantErr: `schema root: type "object" no longer allowed`,
		},
		{
			name:    "enum narrowed",
			prev:    `{"properties":{"s":{"enum":["a","b"]}}}`,
			next:    `{"properties":{"s":{"enum":["a"]}}}`,
			wantErr: `value "b" accepted by the previous enum is not in the new enum`,
		},
		{
			name:    "enum added",
			prev:    `{"properties":{"s":{"type":"string"}}}`,
			next:    `{"properties":{"s":{"type":"string","enum":["a"]}}}`,
			wantErr: "enum added",
		},
		{
			name:    "const changed",
			prev:    `{"properties":{"s":{"const":"a"}}}`,
			next:    `{"properties":{"s":{"const":"b"}}}`,
			wantErr: `value "a" accepted by the previous const is not in the new const`,
		},
		{
			name:    "minimum tightened",
			prev:    `{"properties":{"n":{"minimum":1}}}`,
			next:    `{"properties":{"n":{"minimum":2}}}`,
			wantErr: "lower bound tightened from 1 to 2",
		},
		{
			name:    "inclusive minimum becomes exclusive at the same value",
			prev:    `{"properties":{"n":{"minimum":5}}}`,
			next:    `{"properties":{"n":{"exclusiveMinimum":5}}}`,
			wantErr: "lower bound tightened",
		},
		{
			name:    "maximum added",
			prev:    `{"properties":{"n":{"type":"number"}}}`,
			next:    `{"properties":{"n":{"type":"number","maximum":10}}}`,
			wantErr: "upper bound 10 added",
		},
		{
			name:    "maximum tightened",
			prev:    `{"properties":{"n":{"maximum":10}}}`,
			next:    `{"properties":{"n":{"maximum":9}}}`,
			wantErr: "upper bound tightened from 10 to 9",
		},
		{
			name:    "draft-04 boolean exclusiveMinimum is refused",
			prev:    `{"properties":{"n":{"minimum":1,"exclusiveMinimum":true}}}`,
			next:    `{"properties":{"n":{"minimum":1,"exclusiveMinimum":true}}}`,
			wantErr: "exclusiveMinimum must be a number",
		},
		{
			name:    "multipleOf added",
			prev:    `{"properties":{"n":{}}}`,
			next:    `{"properties":{"n":{"multipleOf":2}}}`,
			wantErr: "multipleOf added",
		},
		{
			name:    "multipleOf to a non-divisor",
			prev:    `{"properties":{"n":{"multipleOf":10}}}`,
			next:    `{"properties":{"n":{"multipleOf":3}}}`,
			wantErr: "does not divide",
		},
		{
			name:    "minLength increased",
			prev:    `{"properties":{"s":{"minLength":1}}}`,
			next:    `{"properties":{"s":{"minLength":2}}}`,
			wantErr: "minLength tightened from 1 to 2",
		},
		{
			name:    "maxLength added",
			prev:    `{"properties":{"s":{"type":"string"}}}`,
			next:    `{"properties":{"s":{"type":"string","maxLength":5}}}`,
			wantErr: "maxLength added",
		},
		{
			name:    "maxItems decreased",
			prev:    `{"properties":{"a":{"maxItems":5}}}`,
			next:    `{"properties":{"a":{"maxItems":4}}}`,
			wantErr: "maxItems tightened",
		},
		{
			name:    "minContains 0 dropped tightens to the default 1",
			prev:    `{"contains":{"type":"string"},"minContains":0}`,
			next:    `{"contains":{"type":"string"}}`,
			wantErr: "minContains tightened from 0 to 1",
		},
		{
			name:    "pattern added",
			prev:    `{"properties":{"s":{"type":"string"}}}`,
			next:    `{"properties":{"s":{"type":"string","pattern":"^a"}}}`,
			wantErr: "pattern added",
		},
		{
			name:    "pattern changed",
			prev:    `{"properties":{"s":{"pattern":"^a"}}}`,
			next:    `{"properties":{"s":{"pattern":"^b"}}}`,
			wantErr: "pattern changed",
		},
		{
			name:    "format added",
			prev:    `{"properties":{"s":{"type":"string"}}}`,
			next:    `{"properties":{"s":{"type":"string","format":"email"}}}`,
			wantErr: "format added",
		},
		{
			name:    "uniqueItems added",
			prev:    `{"properties":{"a":{"type":"array"}}}`,
			next:    `{"properties":{"a":{"type":"array","uniqueItems":true}}}`,
			wantErr: "uniqueItems: true added",
		},
		{
			name:    "additionalProperties false added",
			prev:    `{"properties":{"id":{"type":"string"}}}`,
			next:    `{"properties":{"id":{"type":"string"}},"additionalProperties":false}`,
			wantErr: "additionalProperties: false closes a content model",
		},
		{
			name:    "additionalProperties schema added",
			prev:    `{"properties":{"id":{"type":"string"}}}`,
			next:    `{"properties":{"id":{"type":"string"}},"additionalProperties":{"type":"string"}}`,
			wantErr: "additionalProperties schema added",
		},
		{
			name:    "additionalProperties schema narrowed",
			prev:    `{"additionalProperties":{"type":["string","null"]}}`,
			next:    `{"additionalProperties":{"type":"string"}}`,
			wantErr: `at /additionalProperties: type "null" no longer allowed`,
		},
		{
			name:    "new property narrower than the old additionalProperties schema",
			prev:    `{"properties":{"id":{"type":"integer"}},"additionalProperties":{"type":"string"}}`,
			next:    `{"properties":{"id":{"type":"integer"},"tag":{"type":"string","maxLength":3}},"additionalProperties":{"type":"string"}}`,
			wantErr: `new property "tag" must accept everything the previous additionalProperties schema did`,
		},
		{
			name:    "items added",
			prev:    `{"properties":{"a":{"type":"array"}}}`,
			next:    `{"properties":{"a":{"type":"array","items":{"type":"string"}}}}`,
			wantErr: "items constraint added",
		},
		{
			name:    "items narrowed",
			prev:    `{"properties":{"a":{"items":{"type":"number"}}}}`,
			next:    `{"properties":{"a":{"items":{"type":"integer"}}}}`,
			wantErr: `at /properties/a/items: type "number" no longer allowed`,
		},
		{
			name:    "tuple-form items is refused",
			prev:    `{"items":[{"type":"string"}]}`,
			next:    `{"items":[{"type":"string"}]}`,
			wantErr: "array-form items",
		},
		{
			name:    "nested required added",
			prev:    `{"properties":{"c":{"properties":{"tier":{"type":"integer"}}}}}`,
			next:    `{"properties":{"c":{"properties":{"tier":{"type":"integer"}},"required":["tier"]}}}`,
			wantErr: `at /properties/c: required property "tier" added`,
		},
		{
			name:    "anyOf added",
			prev:    `{"type":"string"}`,
			next:    `{"type":"string","anyOf":[{"minLength":1}]}`,
			wantErr: "anyOf added",
		},
		{
			name:    "anyOf branch lost",
			prev:    `{"anyOf":[{"type":"string"},{"type":"integer"}]}`,
			next:    `{"anyOf":[{"type":"string"}]}`,
			wantErr: "anyOf branch 1 of the previous version is not covered",
		},
		{
			name:    "allOf added",
			prev:    `{"type":"object"}`,
			next:    `{"type":"object","allOf":[{"required":["id"]}]}`,
			wantErr: "allOf added",
		},
		{
			name:    "allOf gains a conjunct",
			prev:    `{"allOf":[{"type":"object"}]}`,
			next:    `{"allOf":[{"type":"object"},{"required":["id"]}]}`,
			wantErr: "allOf branch 1 of the new version is not implied",
		},
		{
			name:    "new schema false",
			prev:    `{"properties":{"q":{"type":"string"}}}`,
			next:    `{"properties":{"q":false}}`,
			wantErr: "new schema is false",
		},
		{
			name:    "$schema draft changed",
			prev:    `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`,
			next:    `{"type":"object"}`,
			wantErr: "$schema changed",
		},

		// ---- outside the allowlist: fail closed --------------------------------
		{
			name:    "oneOf added",
			prev:    `{"type":"object"}`,
			next:    `{"type":"object","oneOf":[{"required":["a"]},{"required":["b"]}]}`,
			wantErr: `"oneOf" added; it is outside the compatibility check's vocabulary`,
		},
		{
			name:    "oneOf changed",
			prev:    `{"oneOf":[{"type":"string"}]}`,
			next:    `{"oneOf":[{"type":"string"},{"type":"integer"}]}`,
			wantErr: `"oneOf" changed`,
		},
		{
			name:    "not added",
			prev:    `{"type":"string"}`,
			next:    `{"type":"string","not":{"const":"x"}}`,
			wantErr: `"not" added`,
		},
		{
			name:    "if/then added",
			prev:    `{"type":"object"}`,
			next:    `{"type":"object","if":{"required":["a"]},"then":{"required":["b"]}}`,
			wantErr: `"if" added`,
		},
		{
			name:    "patternProperties changed",
			prev:    `{"patternProperties":{"^x-":{"type":"string"}}}`,
			next:    `{"patternProperties":{"^x-":{"type":"integer"}}}`,
			wantErr: `"patternProperties" changed`,
		},
		{
			name:    "patternProperties dropped while additionalProperties constrains",
			prev:    `{"patternProperties":{"^x-":{"type":"string"}},"additionalProperties":false}`,
			next:    `{"additionalProperties":false}`,
			wantErr: `"patternProperties" removed`,
		},
		{
			name:    "prefixItems dropped while items stays",
			prev:    `{"prefixItems":[{"type":"string"}],"items":{"type":"integer"}}`,
			next:    `{"items":{"type":"integer"}}`,
			wantErr: `"prefixItems" removed`,
		},
		{
			name:    "prefixItems dropped together with items",
			prev:    `{"prefixItems":[{"type":"string"}],"items":{"type":"integer"}}`,
			next:    `{}`,
			wantErr: "",
		},
		{
			name:    "propertyNames added",
			prev:    `{"type":"object"}`,
			next:    `{"type":"object","propertyNames":{"maxLength":3}}`,
			wantErr: `"propertyNames" added`,
		},
		{
			name:    "dependentRequired changed",
			prev:    `{"dependentRequired":{"a":["b"]}}`,
			next:    `{"dependentRequired":{"a":["b","c"]}}`,
			wantErr: `"dependentRequired" changed`,
		},
		{
			name:    "unevaluatedProperties with any other change",
			prev:    `{"type":"object","properties":{"a":{"type":"string"}},"unevaluatedProperties":false}`,
			next:    `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"unevaluatedProperties":false}`,
			wantErr: `"unevaluatedProperties" is outside the compatibility check's vocabulary`,
		},
		{
			name:    "$dynamicRef is refused",
			prev:    `{"type":"object"}`,
			next:    `{"type":"object","$dynamicRef":"#node"}`,
			wantErr: `"$dynamicRef" is outside`,
		},
		{
			name:    "contentSchema added",
			prev:    `{"type":"string"}`,
			next:    `{"type":"string","contentMediaType":"application/json","contentSchema":{"type":"object"}}`,
			wantErr: `"contentMediaType" added`,
		},
		{
			name:    "nested $id is refused",
			prev:    `{"properties":{"a":{"$id":"sub","type":"string"}}}`,
			next:    `{"properties":{"a":{"$id":"sub","type":"string"}}}`,
			wantErr: "nested $id is not supported",
		},
		{
			name:    "$ref with sibling keywords is refused",
			prev:    `{"properties":{"q":{"$ref":"#/$defs/qty","minimum":0}},"$defs":{"qty":{"type":"integer"}}}`,
			next:    `{"properties":{"q":{"$ref":"#/$defs/qty","minimum":0}},"$defs":{"qty":{"type":"integer"}}}`,
			wantErr: `$ref alongside "minimum" is not supported`,
		},
		{
			name:    "external $ref is refused",
			prev:    `{"properties":{"q":{"type":"integer"}}}`,
			next:    `{"properties":{"q":{"$ref":"https://example.com/qty.json"}}}`,
			wantErr: "only JSON-pointer references into the same document",
		},
		{
			name:    "anchor $ref is refused",
			prev:    `{"properties":{"q":{"type":"integer"}}}`,
			next:    `{"properties":{"q":{"$ref":"#qty"}},"$defs":{"qty":{"$anchor":"qty","type":"integer"}}}`,
			wantErr: "only JSON-pointer references into the same document",
		},
		{
			name:    "dangling $ref",
			prev:    `{"properties":{"q":{"$ref":"#/$defs/missing"}}}`,
			next:    `{"properties":{"q":{}}}`,
			wantErr: `$ref "#/$defs/missing" does not resolve`,
		},
		{
			// A chain of nothing but $ref that loops is the validator's
			// "reference cycle" failure: it accepts no document, so the
			// previous version accepted nothing at q and {} is wider.
			name: "$ref cycle in the previous version accepts nothing",
			prev: `{"properties":{"q":{"$ref":"#/$defs/a"}},"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}}}`,
			next: `{"properties":{"q":{}}}`,
		},
		{
			name:    "$ref cycle in the new version rejects everything",
			prev:    `{"properties":{"q":{}}}`,
			next:    `{"properties":{"q":{"$ref":"#/$defs/a"}},"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}}}`,
			wantErr: "new schema is false",
		},
		{
			name:    "malformed properties",
			prev:    `{"properties":[]}`,
			next:    `{"properties":{}}`,
			wantErr: "properties must be an object",
		},
		{
			name:    "malformed previous JSON",
			prev:    `{"type":`,
			next:    `{}`,
			wantErr: "previous schema is not valid JSON",
		},
		{
			name:    "malformed new JSON",
			prev:    `{}`,
			next:    `{"type":`,
			wantErr: "new schema is not valid JSON",
		},
		{
			name:    "subschema that is neither object nor boolean",
			prev:    `{"properties":{"q":{"type":"string"}}}`,
			next:    `{"properties":{"q":"string"}}`,
			wantErr: "new schema is neither an object nor a boolean",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCompatible([]byte(tc.prev), []byte(tc.next))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("checkCompatible() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkCompatible() error = nil, want %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("checkCompatible() error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestJSONEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{`1`, `1.0`, true},
		{`1`, `2`, false},
		{`"a"`, `"a"`, true},
		{`"1"`, `1`, false},
		{`[1,{"a":null}]`, `[1.0,{"a":null}]`, true},
		{`{"a":1,"b":2}`, `{"b":2,"a":1}`, true},
		{`{"a":1}`, `{"a":1,"b":2}`, false},
		{`true`, `true`, true},
		{`null`, `false`, false},
	}
	for _, tc := range cases {
		a, _ := decodeForTest(t, tc.a)
		b, _ := decodeForTest(t, tc.b)
		if got := jsonEqual(a, b); got != tc.want {
			t.Errorf("jsonEqual(%s, %s) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func decodeForTest(t *testing.T, s string) (any, error) {
	t.Helper()
	v, err := jsonschema.UnmarshalJSON(strings.NewReader(s))
	if err != nil {
		t.Fatal(err)
	}
	return v, nil
}
