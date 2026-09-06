package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Fuzz targets and property tests for the schema package. The fuzz
// functions double as ordinary tests: `go test` runs their seed corpus
// (f.Add calls plus testdata/fuzz/<Name>/*), and `go test -fuzz=Name`
// keeps generating. Timing assertions are only armed when
// NARAD_FUZZ_TIMING=1 so a loaded CI runner under -race cannot flake
// them; set NARAD_FUZZ_SLOW_DIR to have slow inputs written out.

const (
	// slowFinding is the per-call budget above which an input is worth
	// reporting; slowBug is where it is a bug outright.
	slowFinding = 500 * time.Millisecond
	slowBug     = 5 * time.Second
)

var (
	fuzzTiming  = os.Getenv("NARAD_FUZZ_TIMING") == "1"
	fuzzSlowDir = os.Getenv("NARAD_FUZZ_SLOW_DIR")
)

// noteDuration enforces the time bound and records slow inputs.
func noteDuration(t *testing.T, what string, d time.Duration, inputs ...[]byte) {
	t.Helper()
	if !fuzzTiming {
		return
	}
	if d > slowFinding && fuzzSlowDir != "" {
		name := fmt.Sprintf("%s-%d-%dms", strings.ReplaceAll(what, " ", "_"), time.Now().UnixNano(), d.Milliseconds())
		for i, in := range inputs {
			_ = os.WriteFile(filepath.Join(fuzzSlowDir, fmt.Sprintf("%s.%d", name, i)), in, 0o644)
		}
	}
	if d > slowBug {
		t.Fatalf("%s took %v (bug threshold %v)", what, d, slowBug)
	}
}

// externalRefs lists $ref values in schema positions that do not stay
// inside the document, and reports whether $schema names a non
// built-in draft.
func documentedRefusals(doc any) (external []string, unknownDraft bool) {
	walkSchemaPositions(doc, func(node map[string]any) {
		if ref, ok := node["$ref"].(string); ok && !strings.HasPrefix(ref, "#") {
			external = append(external, ref)
		}
	})
	if root, ok := doc.(map[string]any); ok {
		if s, ok := root["$schema"].(string); ok && !builtinDraftURL(s) {
			unknownDraft = true
		}
	}
	return external, unknownDraft
}

// builtinDraftURL reports whether s names one of the embedded drafts:
// the five versioned metaschema URLs and json-schema.org/schema (the
// library's alias for the latest draft), under http or https, with or
// without a trailing "#".
func builtinDraftURL(s string) bool {
	s = strings.TrimSuffix(s, "#")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "https://"), "http://")
	if s == "json-schema.org/schema" {
		return true
	}
	for _, d := range builtinDrafts[1:] {
		d = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(d, "#"), "https://"), "http://")
		if s == d {
			return true
		}
	}
	return false
}

// checkDefinitionInvariants runs ValidateDefinition on raw and checks
// the documented refusals against what the document contains.
func checkDefinitionInvariants(t *testing.T, raw []byte) error {
	t.Helper()
	r := NewJSONSchema()
	start := time.Now()
	err := r.ValidateDefinition(context.Background(), "t", raw)
	noteDuration(t, "ValidateDefinition", time.Since(start), raw)

	doc, decodeErr := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if decodeErr != nil {
		if err == nil {
			t.Fatalf("undecodable document accepted: %q", truncate(raw))
		}
		return err
	}
	external, unknownDraft := documentedRefusals(doc)
	if err == nil {
		if len(raw) > MaxSchemaBytes {
			t.Fatalf("document of %d bytes accepted", len(raw))
		}
		if d := containerDepth(doc); d > MaxSchemaDepth {
			t.Fatalf("document nesting %d levels accepted", d)
		}
		if len(external) > 0 {
			t.Fatalf("external $ref %q accepted in %s", external[0], truncate(raw))
		}
		if unknownDraft {
			t.Fatalf("non built-in $schema accepted in %s", truncate(raw))
		}
		switch v := doc.(type) {
		case map[string]any:
		case bool:
			if !v {
				t.Fatalf("false accepted as a schema")
			}
		default:
			t.Fatalf("non-object document accepted: %s", truncate(raw))
		}
		return nil
	}
	msg := err.Error()
	for _, ref := range external {
		if strings.Contains(ref, "://") && strings.Contains(msg, ref) {
			t.Fatalf("error echoes the external reference %q: %s", ref, msg)
		}
	}
	// A file:// URL or filesystem path in the message that the caller
	// did not send is the broker's environment leaking (a $id the
	// caller wrote is echoed back in a duplicate-id error, which is
	// their own text).
	for _, marker := range []string{"file://", "/etc/", cwd} {
		if marker == "" {
			continue
		}
		if i := strings.Index(msg, marker); i >= 0 {
			end := i + len(marker)
			for end < len(msg) && msg[end] != '"' && msg[end] != ' ' && msg[end] != '\'' {
				end++
			}
			if !bytes.Contains(raw, []byte(msg[i:end])) {
				t.Fatalf("error leaks a path: %s", msg)
			}
		}
	}
	return err
}

var cwd, _ = os.Getwd()

func truncate(b []byte) string {
	if len(b) > 400 {
		return string(b[:400]) + "..."
	}
	return string(b)
}

// FuzzValidateDefinitionBytes feeds arbitrary bytes to the
// registration entry point.
func FuzzValidateDefinitionBytes(f *testing.F) {
	for _, s := range []string{
		`{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}`,
		`true`, `false`, `null`, `{}`, `{"type":`, ``, `  `,
		`{"$ref":"file:///etc/passwd"}`,
		`{"$id":"https://example.com/root","$ref":"other.json"}`,
		`{"$schema":"https://example.com/my-draft","type":"integer"}`,
		`{"$schema":"http://json-schema.org/draft-07/schema#","type":"string","format":"email"}`,
		`{"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}},"$ref":"#/$defs/a"}`,
		`{"anyOf":[{"$ref":"#"},{"$ref":"#"}]}`,
		`{"type":"string","pattern":"^(?=a)a$"}`,
		`{"pattern":"((a{1000}){1000}){1000}"}`,
		`{"minimum":1e99999999}`,
		`{"minimum":1e999999}`,
		`{"enum":[1e400,9007199254740993,"é",null,{"a":[]}]}`,
		`{"properties":{"$ref":{"type":"string"}},"const":{"$ref":"http://x"}}`,
		`{"$id":"file:///etc/passwd","type":"integer"}`,
		strings.Repeat(`{"properties":{"a":`, 33) + `{}` + strings.Repeat(`}}`, 33),
		strings.Repeat(`[`, 100) + strings.Repeat(`]`, 100),
		"\xef\xbb\xbf{}",
		`{"type":"string","maxLength":"3"}`,
		`{"patternProperties":{"[":{}}}`,
		`{"$ref":"#/$defs/a/../../x","$defs":{"a":{}}}`,
		`{"$ref":"#%zz"}`,
		`{"$dynamicRef":"#dyn","$defs":{"x":{"$dynamicAnchor":"dyn"}}}`,
		`{"$anchor":"bad anchor"}`,
		`{"items":[true,false]}`,
		`{"$schema":"http://json-schema.org/draft-04/schema#","minimum":1,"exclusiveMinimum":true}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		checkDefinitionInvariants(t, data)
	})
}

// FuzzGeneratedSchema drives the structural generator in adversarial
// mode: nested objects, $ref cycles, $defs, big enums, deep
// combinators, patterns, formats and unicode, plus garbage values.
func FuzzGeneratedSchema(f *testing.F) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 24; i++ {
		b := make([]byte, 8+rng.Intn(400))
		rng.Read(b)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		src := newByteSrc(data)
		opts := genOptions{adversarial: true, maxDepth: 4 + src.n(40), maxNodes: 20 + src.n(600)}
		doc := genSchemaDoc(src, opts)
		raw := mustJSON(doc)
		if len(raw) > MaxSchemaBytes {
			t.Skip("generated document over the size limit")
		}
		if err := checkDefinitionInvariants(t, raw); err != nil {
			return
		}
		// A schema that registers must also load and validate without
		// panicking, and must be equal to itself.
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, raw); err != nil {
			t.Fatalf("Load after successful ValidateDefinition: %v", err)
		}
		if !Equal(raw, raw) {
			t.Fatalf("Equal(raw, raw) is false")
		}
		payload := mustJSON(genPayload(src, doc))
		start := time.Now()
		_ = r.Validate(context.Background(), "t", payload)
		noteDuration(t, "Validate generated", time.Since(start), raw, payload)
	})
}

// checkPayloadInvariants validates payload against the schema loaded
// in r and checks what the contract promises about the outcome.
func checkPayloadInvariants(t *testing.T, r *JSONSchema, raw, payload []byte) error {
	t.Helper()
	start := time.Now()
	err := r.Validate(context.Background(), "t", payload)
	noteDuration(t, "Validate", time.Since(start), raw, payload)
	if err != nil {
		if n := len(err.Error()); n > maxValidationErrorBytes+64 {
			t.Fatalf("error message is %d bytes: %s", n, err.Error()[:200])
		}
	}
	if !utf8.Valid(payload) {
		if err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
			t.Fatalf("invalid UTF-8 payload not refused as such: %v", err)
		}
		return err
	}
	if !json.Valid(payload) {
		if err == nil {
			t.Fatalf("malformed JSON accepted: %q", truncate(payload))
		}
		return err
	}
	if bytes.HasPrefix(bytes.TrimLeft(payload, " \t\r\n"), []byte("\xef\xbb\xbf")) && err == nil {
		t.Fatalf("BOM accepted")
	}
	return err
}

// FuzzValidatePayload validates arbitrary bytes and generated payloads
// against generated well-formed schemas.
func FuzzValidatePayload(f *testing.F) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 16; i++ {
		seed := make([]byte, 8+rng.Intn(200))
		rng.Read(seed)
		for _, p := range []string{`{"id":1}`, `[1,2,3]`, `"s"`, `1e99999999`, `9007199254740993`, "\xff", `{"a":"\x00"}`, strings.Repeat("[", 200) + strings.Repeat("]", 200), `-0`, `1e-999999`, `{"p0":"x-1","extra_1":null}`} {
			f.Add(seed, []byte(p))
		}
	}
	f.Fuzz(func(t *testing.T, seed, payload []byte) {
		src := newByteSrc(seed)
		doc := genSchemaDoc(src, genOptions{})
		raw := mustJSON(doc)
		r := NewJSONSchema()
		if err := r.ValidateDefinition(context.Background(), "t", raw); err != nil {
			return
		}
		if err := r.Load(context.Background(), "t", 1, raw); err != nil {
			t.Fatalf("Load: %v", err)
		}
		checkPayloadInvariants(t, r, raw, payload)
		generated := mustJSON(genPayload(src, doc))
		checkPayloadInvariants(t, r, raw, generated)
	})
}

// compatStats accumulates what a compatibility round saw.
type compatStats struct {
	rounds, accepted, rejected, expectedAcceptRejected int
	payloadsChecked, payloadsSkipped                   int
}

// compatRound generates an old schema, mutates it, runs the check and,
// when the check accepts, asserts that every generated payload valid
// under the old schema is valid under the new one.
func compatRound(t *testing.T, data []byte, payloads int, st *compatStats) {
	t.Helper()
	src := newByteSrc(data)
	oldDoc := genSchemaDoc(src, genOptions{})
	oldRaw := mustJSON(oldDoc)
	// Only registrable schemas matter: a PATCH runs ValidateDefinition
	// on the new document before CheckCompatible, and the previous
	// version went through it when it was registered.
	reg := NewJSONSchema()
	if reg.ValidateDefinition(context.Background(), "t", oldRaw) != nil {
		return
	}
	oldCompiled, err := compileSchema("t", 1, oldRaw)
	if err != nil {
		return
	}
	// Any compilable well-formed schema must be compatible with itself.
	start := time.Now()
	if err := checkCompatible(oldRaw, oldRaw); err != nil {
		t.Fatalf("schema rejected as incompatible with itself: %v\nschema: %s", err, oldRaw)
	}
	noteDuration(t, "CheckCompatible self", time.Since(start), oldRaw)

	newDoc, mut, ok := mutateSchema(src, oldDoc)
	if !ok {
		return
	}
	newRaw := mustJSON(newDoc)
	if reg.ValidateDefinition(context.Background(), "t", newRaw) != nil {
		return
	}
	newCompiled, err := compileSchema("t", 2, newRaw)
	if err != nil {
		return
	}
	st.rounds++
	start = time.Now()
	err = checkCompatible(oldRaw, newRaw)
	noteDuration(t, "CheckCompatible", time.Since(start), oldRaw, newRaw)
	if err != nil {
		st.rejected++
		if mut.expectAccept {
			st.expectedAcceptRejected++
			t.Errorf("documented widening rejected (%s): %v\nold: %s\nnew: %s", mut.name, err, oldRaw, newRaw)
		}
		return
	}
	st.accepted++
	for i := 0; i < payloads; i++ {
		payload := mustJSON(genPayload(src, oldDoc))
		instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("generated payload is not JSON: %v", err)
		}
		if oldCompiled.Validate(instance) != nil {
			st.payloadsSkipped++
			continue
		}
		st.payloadsChecked++
		if verr := newCompiled.Validate(instance); verr != nil {
			t.Fatalf("SOUNDNESS: compatibility check accepted a change (%s) that rejects a previously valid payload\nold: %s\nnew: %s\npayload: %s\nerror: %v", mut.name, oldRaw, newRaw, payload, verr)
		}
	}
}

// FuzzCompatibility property-tests CheckCompatible.
func FuzzCompatibility(f *testing.F) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 32; i++ {
		b := make([]byte, 16+rng.Intn(300))
		rng.Read(b)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var st compatStats
		compatRound(t, data, 16, &st)
	})
}

// TestPropertyCompatibilitySoundness is the deterministic, always-on
// version of FuzzCompatibility: a fixed seed, many rounds, and a
// printed tally so a drop in accepted rounds or checked payloads is
// visible.
func TestPropertyCompatibilitySoundness(t *testing.T) {
	rounds := 1500
	if testing.Short() {
		rounds = 200
	}
	// NARAD_PROP_ROUNDS overrides the round count for long soak runs.
	if v, err := strconv.Atoi(os.Getenv("NARAD_PROP_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	rng := rand.New(rand.NewSource(20260906))
	var st compatStats
	for i := 0; i < rounds; i++ {
		b := make([]byte, 32+rng.Intn(400))
		rng.Read(b)
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			compatRound(t, b, 24, &st)
		})
	}
	t.Logf("rounds with a compilable mutation: %d, accepted: %d, rejected: %d, documented widenings rejected: %d, payloads checked under both: %d, payloads discarded (invalid under old): %d",
		st.rounds, st.accepted, st.rejected, st.expectedAcceptRejected, st.payloadsChecked, st.payloadsSkipped)
	if st.accepted == 0 || st.payloadsChecked == 0 {
		t.Fatalf("property test exercised nothing: %+v", st)
	}
}

// TestPropertyExactNumbers checks type integer, multipleOf and the four
// bounds against a big.Rat oracle over random values in every spelling
// (plain, exponent, trailing .0, beyond 2^53 and 2^63).
func TestPropertyExactNumbers(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	randRat := func() *big.Rat {
		switch rng.Intn(6) {
		case 0:
			return big.NewRat(int64(rng.Intn(2001)-1000), 1)
		case 1:
			return big.NewRat(int64(rng.Intn(2001)-1000), int64(1+rng.Intn(20)))
		case 2:
			n := new(big.Int).Lsh(big.NewInt(1), uint(50+rng.Intn(30)))
			n.Add(n, big.NewInt(int64(rng.Intn(5)-2)))
			return new(big.Rat).SetInt(n)
		case 3:
			r, _ := new(big.Rat).SetString(fmt.Sprintf("%d.%d", rng.Intn(100), rng.Intn(1000)))
			return r
		case 4:
			r, _ := new(big.Rat).SetString(fmt.Sprintf("%de%d", 1+rng.Intn(9), rng.Intn(60)))
			return r
		default:
			r, _ := new(big.Rat).SetString(fmt.Sprintf("%de-%d", 1+rng.Intn(9), rng.Intn(30)))
			return r
		}
	}
	spell := func(v *big.Rat) string {
		b := make([]byte, 4)
		rng.Read(b)
		return spellRat(newByteSrc(b), v)
	}
	validate := func(schema, payload string) bool {
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(schema)); err != nil {
			t.Fatalf("Load(%s): %v", schema, err)
		}
		return r.Validate(context.Background(), "t", []byte(payload)) == nil
	}
	for i := 0; i < 1500; i++ {
		v := randRat()
		lit := spell(v)
		parsed, ok := new(big.Rat).SetString(lit)
		if !ok || parsed.Cmp(v) != 0 {
			continue // inexact spelling; nothing to assert
		}
		if got, want := validate(`{"type":"integer"}`, lit), v.IsInt(); got != want {
			t.Fatalf("integer detection for %s: got %v, want %v", lit, got, want)
		}
		m := randRat()
		if m.Sign() <= 0 {
			m = big.NewRat(3, 7)
		}
		mLit := spell(m)
		if mp, ok := new(big.Rat).SetString(mLit); ok && mp.Cmp(m) == 0 {
			want := new(big.Rat).Quo(v, m).IsInt()
			if got := validate(`{"multipleOf":`+mLit+`}`, lit); got != want {
				t.Fatalf("multipleOf %s for %s: got %v, want %v", mLit, lit, got, want)
			}
		}
		b := randRat()
		bLit := spell(b)
		if bp, ok := new(big.Rat).SetString(bLit); ok && bp.Cmp(b) == 0 {
			cmp := v.Cmp(b)
			cases := []struct {
				kw   string
				want bool
			}{
				{"minimum", cmp >= 0},
				{"exclusiveMinimum", cmp > 0},
				{"maximum", cmp <= 0},
				{"exclusiveMaximum", cmp < 0},
			}
			for _, c := range cases {
				if got := validate(`{"`+c.kw+`":`+bLit+`}`, lit); got != c.want {
					t.Fatalf("%s %s for %s: got %v, want %v", c.kw, bLit, lit, got, c.want)
				}
			}
		}
	}
}
