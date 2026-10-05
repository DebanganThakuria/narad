package schema

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// libraryForm compiles raw with the plain library: no enum sets, no
// first-failure arrays. It is the verdict every form must agree with.
func libraryForm(t *testing.T, raw []byte) *jsonschema.Schema {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := newSchemaCompiler()
	if err := c.AddResource(schemaResourceURL("ref", 1), doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile(schemaResourceURL("ref", 1))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// formsAgree checks that the plain library, the full form (enum sets)
// and the yes-or-no form (enum sets, first-failure arrays) all give
// payload the same verdict. It reports whether the payload was valid.
func formsAgree(t *testing.T, ref *jsonschema.Schema, cs *compiledSchema, raw, payload []byte) bool {
	t.Helper()
	instance, err := decodePayload(payload)
	if err != nil {
		return false
	}
	check, err := cs.checkForm()
	if err != nil {
		t.Fatalf("check form of %s: %v", raw, err)
	}
	want := ref.Validate(instance) == nil
	if got := cs.full.Validate(instance) == nil; got != want {
		t.Fatalf("full form says valid=%v, library says %v\nschema:  %s\npayload: %s", got, want, raw, payload)
	}
	if got := check.Validate(instance) == nil; got != want {
		t.Fatalf("check form says valid=%v, library says %v\nschema:  %s\npayload: %s", got, want, raw, payload)
	}
	return want
}

// TestCheckFormAgreesOnArrays pins the array and contains rules the
// yes-or-no form reimplements, across drafts.
func TestCheckFormAgreesOnArrays(t *testing.T) {
	schemas := []string{
		`{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"type":"integer"},{"type":"string"}],"additionalItems":false}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"type":"integer"}],"additionalItems":{"type":"string"}}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","items":[{"type":"integer"}]}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","items":{"type":"integer"},"additionalItems":false}`,
		`{"$schema":"http://json-schema.org/draft-04/schema#","items":[{"type":"integer"}],"additionalItems":false}`,
		`{"$schema":"https://json-schema.org/draft/2019-09/schema","items":[{"type":"integer"}],"additionalItems":{"type":"string"},"unevaluatedItems":false}`,
		`{"$schema":"https://json-schema.org/draft/2019-09/schema","contains":{"type":"string"},"minContains":2,"maxContains":3}`,
		`{"prefixItems":[{"type":"integer"},{"type":"string"}],"items":false}`,
		`{"prefixItems":[{"type":"integer"}],"items":{"type":"string"},"contains":{"const":"x"}}`,
		`{"prefixItems":[{"type":"integer"}],"unevaluatedItems":false}`,
		`{"contains":{"type":"string"},"unevaluatedItems":false}`,
		`{"contains":{"type":"string"},"minContains":0,"maxContains":1}`,
		`{"contains":{"type":"string"},"minContains":2}`,
		`{"allOf":[{"prefixItems":[true]}],"unevaluatedItems":{"type":"integer"}}`,
		`{"anyOf":[{"items":{"type":"integer"}},{"contains":{"type":"null"}}],"unevaluatedItems":false}`,
		`{"type":"array","items":{"$ref":"#"}}`,
		`{"items":{"items":{"type":"integer"}},"maxItems":3}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","$ref":"#/definitions/a","items":{"type":"string"},"definitions":{"a":{"type":"array"}}}`,
	}
	payloads := []string{
		`[]`, `[1]`, `["a"]`, `[1,"a"]`, `[1,"a",2]`, `[1,"a","b"]`, `["x"]`, `["x","y"]`, `["x","y","z","w"]`,
		`[null]`, `[1,null]`, `[[1],[2]]`, `[[1],["a"]]`, `[[],[]]`, `[[[]]]`, `[1,2,3,4]`, `[1,"x",null]`, `{}`, `"s"`, `1`,
	}
	for _, s := range schemas {
		raw := []byte(s)
		cs, err := compileTopicBytes("t", 1, raw)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		ref := libraryForm(t, raw)
		valid := 0
		for _, p := range payloads {
			if formsAgree(t, ref, cs, raw, []byte(p)) {
				valid++
			}
		}
		if valid == 0 || valid == len(payloads) {
			t.Errorf("%s: %d of %d payloads valid; the case does not tell the forms apart", s, valid, len(payloads))
		}
	}
}

// TestCheckFormAgreesOnGeneratedSchemas runs the three forms against
// the package's schema and payload generators, adversarial keywords
// included, and against arbitrary JSON.
func TestCheckFormAgreesOnGeneratedSchemas(t *testing.T) {
	rounds := 3000
	if testing.Short() || raceEnabled {
		rounds = 400
	}
	rng := rand.New(rand.NewSource(20260930))
	checked, valid := 0, 0
	for i := 0; i < rounds; i++ {
		b := make([]byte, 32+rng.Intn(400))
		rng.Read(b)
		src := newByteSrc(b)
		doc := genSchemaDoc(src, genOptions{adversarial: i%3 == 0})
		raw := mustJSON(doc)
		if NewJSONSchema().ValidateDefinition(context.Background(), "t", raw) != nil {
			continue
		}
		cs, err := compileTopicBytes("t", 1, raw)
		if err != nil {
			t.Fatalf("registered schema does not compile: %v\n%s", err, raw)
		}
		ref := libraryForm(t, raw)
		for j := 0; j < 8; j++ {
			var payload []byte
			if j%3 == 2 {
				payload = randomJSON(rng, nil, 0)
			} else {
				payload = mustJSON(genPayload(src, doc))
			}
			checked++
			if formsAgree(t, ref, cs, raw, payload) {
				valid++
			}
		}
	}
	t.Logf("%d payloads checked, %d valid", checked, valid)
	if checked == 0 || valid == 0 || valid == checked {
		t.Fatalf("generated cases do not exercise both verdicts: %d checked, %d valid", checked, valid)
	}
}

// randomJSON appends an arbitrary JSON value nested at most a few
// levels below depth.
func randomJSON(rng *rand.Rand, b []byte, depth int) []byte {
	kind := rng.Intn(9)
	if depth > 4 && kind >= 7 {
		kind = rng.Intn(7)
	}
	switch kind {
	case 0:
		return append(b, "null"...)
	case 1:
		return append(b, "true"...)
	case 2:
		return strconv.AppendInt(b, int64(rng.Intn(2000)-1000), 10)
	case 3:
		return strconv.AppendFloat(b, rng.NormFloat64()*100, 'g', -1, 64)
	case 4, 5, 6:
		names := []string{"a", "b", "id", "kind", "", "x"}
		return strconv.AppendQuote(b, names[rng.Intn(len(names))])
	case 7:
		b = append(b, '[')
		for i, n := 0, rng.Intn(4); i < n; i++ {
			if i > 0 {
				b = append(b, ',')
			}
			b = randomJSON(rng, b, depth+1)
		}
		return append(b, ']')
	default:
		names := []string{"a", "b", "id", "kind", "", "x"}
		b = append(b, '{')
		for i, n := 0, rng.Intn(4); i < n; i++ {
			if i > 0 {
				b = append(b, ',')
			}
			b = strconv.AppendQuote(b, names[rng.Intn(len(names))])
			b = append(b, ':')
			b = randomJSON(rng, b, depth+1)
		}
		return append(b, '}')
	}
}
