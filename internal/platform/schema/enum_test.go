package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// maxEnumSchema builds the largest {"type":"array","items":{"enum":
// [...]}} under MaxSchemaBytes, numeric or string, and returns it with
// its last value (the one a linear scan reaches last).
func maxEnumSchema(numeric bool) (doc []byte, last string) {
	var vals []any
	for size := 40; size < MaxSchemaBytes-16; {
		var lit string
		if numeric {
			lit = strconv.Itoa(len(vals))
			vals = append(vals, json.Number(lit))
		} else {
			s := fmt.Sprintf("id-%x", len(vals))
			vals = append(vals, s)
			lit = strconv.Quote(s)
		}
		size += len(lit) + 1
	}
	for {
		doc, _ = json.Marshal(map[string]any{"type": "array", "items": map[string]any{"enum": vals}})
		if len(doc) <= MaxSchemaBytes {
			lb, _ := json.Marshal(vals[len(vals)-1])
			return doc, string(lb)
		}
		vals = vals[:len(vals)-1]
	}
}

func repeatedItems(item string, n int) []byte {
	return []byte("[" + strings.TrimSuffix(strings.Repeat(item+",", n), ",") + "]")
}

// TestLargeEnumIsNotLinear: the validator checked enum membership by a
// linear scan that parses both sides of every numeric comparison into a
// big.Rat, so a registrable 256 KiB allowlist of 45k integer IDs took
// 14 ms per payload item (about 14 s for 1000 items) and a 26k-value
// string enum 11.5 s on a 1 MiB body. Membership is now a hash lookup.
func TestLargeEnumIsNotLinear(t *testing.T) {
	for _, tc := range []struct {
		name    string
		numeric bool
		items   int
		limit   time.Duration
	}{
		{"45k integer IDs, 1000 late items", true, 1000, 500 * time.Millisecond},
		{"26k strings, 1 MiB of late items", false, -1, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			doc, last := maxEnumSchema(tc.numeric)
			r := NewJSONSchema()
			if err := r.ValidateDefinition(ctx, "t", doc); err != nil {
				t.Fatalf("allowlist schema refused: %v", err)
			}
			if err := r.Load(ctx, "t", 1, doc); err != nil {
				t.Fatal(err)
			}
			n := tc.items
			if n < 0 {
				n = (1<<20 - 2) / (len(last) + 1)
			}
			payload := repeatedItems(last, n)
			start := time.Now()
			err := r.Validate(ctx, "t", payload)
			took := time.Since(start)
			t.Logf("%d-byte schema, %d-byte payload of %d items: %v", len(doc), len(payload), n, took)
			if err != nil {
				t.Fatalf("payload of enum members refused: %v", err)
			}
			limit := tc.limit
			if raceEnabled {
				limit *= 10
			}
			if took > limit {
				t.Errorf("validation took %v; the bound is %v", took, limit)
			}
			// A value outside the enum is still refused, with a bounded
			// message.
			nonMember := `"not-a-member"`
			if tc.numeric {
				nonMember = "-1"
			}
			bad := string(repeatedItems(last, 10))
			bad = bad[:len(bad)-1] + "," + nonMember + "]"
			err = r.Validate(ctx, "t", []byte(bad))
			if err == nil || !strings.Contains(err.Error(), "/10") {
				t.Errorf("non-member at /10: err = %.200v, want a refusal at /10", err)
			}
			if err != nil && len(err.Error()) > maxValidationErrorBytes+64 {
				t.Errorf("error message is %d bytes", len(err.Error()))
			}
		})
	}
}

// TestEnumSetAgreesWithTheLibrary pins enum membership by hash against
// the library's scan: numbers by value in any spelling (big ones
// included), object members in any order, arrays in order, and the
// draft-07 rule that every sibling of $ref is ignored, enum included.
func TestEnumSetAgreesWithTheLibrary(t *testing.T) {
	members := []any{
		json.Number("1"), json.Number("2.50"), json.Number("-0"), json.Number("1e3"), json.Number("123456789012345678901234567890"),
		json.Number("0.1"), "a", "", "日本", "1", "null", nil, true,
		map[string]any{"k": json.Number("1"), "z": []any{"x", json.Number("2")}},
		[]any{json.Number("1"), "a"},
		[]any{},
	}
	for len(members) < enumSetMinValues+4 {
		members = append(members, fmt.Sprintf("pad-%d", len(members)))
	}
	enum, _ := json.Marshal(members)
	payloads := []string{
		`1`, `1.0`, `1e0`, `10e-1`, `2.5`, `2.500`, `0`, `-0`, `0.0`, `1000`, `1E3`, `1e3`, `123456789012345678901234567890`,
		`1.23456789012345678901234567890e29`, `0.1`, `1e-1`, `0.10000000000000001`, `"a"`, `""`, `"日本"`, `"1"`, `"null"`, `null`,
		`true`, `false`, `{"z":["x",2],"k":1}`, `{"k":1.0,"z":["x",2e0]}`, `{"k":1,"z":[2,"x"]}`, `{"k":1}`, `[1,"a"]`, `[1.0,"a"]`,
		`["a",1]`, `[]`, `[[]]`, `{}`, `"pad-17"`, `"pad-99"`, `2`, `1e999`, `-1e-999`,
	}
	for _, s := range []string{
		`{"enum":` + string(enum) + `}`,
		`{"type":"array","items":{"enum":` + string(enum) + `}}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","$ref":"#/definitions/any","enum":` + string(enum) + `,"definitions":{"any":{}}}`,
		`{"$ref":"#/$defs/any","enum":` + string(enum) + `,"$defs":{"any":{}}}`,
		`{"not":{"enum":` + string(enum) + `}}`,
	} {
		vacuous := strings.Contains(s, "draft-07")
		raw := []byte(s)
		cs, err := compileTopicBytes("t", 1, raw)
		if err != nil {
			t.Fatalf("%.80s: %v", s, err)
		}
		ref := libraryForm(t, raw)
		valid := 0
		for _, p := range payloads {
			if strings.Contains(s, `"items"`) {
				p = "[" + p + "]"
			}
			if formsAgree(t, ref, cs, raw, []byte(p)) {
				valid++
			}
		}
		if vacuous {
			if valid != len(payloads) {
				t.Errorf("%.80s: %d of %d payloads valid; the enum beside a draft-07 $ref must stay ignored", s, valid, len(payloads))
			}
		} else if valid == 0 || valid == len(payloads) {
			t.Errorf("%.80s: %d of %d payloads valid; the case does not tell members apart", s, valid, len(payloads))
		}
	}
}
