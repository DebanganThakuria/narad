package schema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// wp4bSeeds are the payload shapes where encoding/json and a token
// walk could plausibly disagree.
var wp4bSeeds = []string{
	// duplicate names: the last value wins, whole (never merged)
	`{"a":1,"a":2}`, `{"a":{"x":1},"a":{"y":2}}`, `{"a":[1,2],"a":[3]}`, `{"a":1,"a":{"y":2}}`, `{"a":{"b":1,"b":{}},"a":{"b":[]}}`,
	// escapes: lone and paired surrogates, every short escape
	`"\ud800"`, `"\udc00"`, `"\ud800\ud800"`, `"\udc00\ud800x"`, `"\ud83d\ude00"`, `"\ud83dx\ude00"`, `"\u0000"`, `"\u00e9\u20AC"`,
	`"\b\f\n\r\t\"\\\/"`, `{"\ud800":1,"�":2}`, `"\u"`, `"\u12"`, `"\x"`, `"\ud800\u"`,
	// raw text
	"\"\xff\"", "\"a\x00b\"", "\"a\x1fb\"", "\"é€😀\"", "\xef\xbb\xbf{}", `"a	b"`,
	// numbers and the exponent bound
	`0`, `-0`, `-0.0`, `01`, `-`, `1.`, `.5`, `1e`, `1e+`, `+1`, `1.5e3`, `1E+2`, `1e-2`, `12345678901234567890123`,
	`1e1000`, `1e1001`, `1e-1000`, `1E-1001`, `1e0001000`, `1e00001001`, `1e9999`, `[1e5,1e1001]`, `[1e1001,]`, `{"e":1e5}`,
	`"1e9999"`, `{"a":"\"e9999"}`, `{"1e9999":1}`, `9007199254740993`, `1e400`, `-1.7976931348623157e308`,
	// structure and trailing data
	``, ` `, "\t\n\r ", `{`, `}`, `[`, `]`, `{"a":1} {`, `{"a":1}   `, `{} x`, `{}]`, `{}{}`, `1 2`, `nul`, `truex`, `true false`,
	`[1,]`, `{"a":1,}`, `{,}`, `[,1]`, `{"a"}`, `{"a":}`, `{1:2}`, `{"a" 1}`, `["a":1]`, `{"a":1 "b":2}`, `[] `, ` []`,
	`{"id":123456,"sku":"ABC-123","nested":{"x":1.5,"y":[1,2,3]}}`, `[[],{},[{}],{"":[]}]`, `{"":""}`,
	strings.Repeat("[", 64) + "1" + strings.Repeat("]", 64), strings.Repeat(`{"a":`, 50) + "null" + strings.Repeat("}", 50),
}

// wp4bFastWalk is decodePayload without the fallback: the token walk's
// own verdict.
func wp4bFastWalk(payload []byte) (any, bool) {
	d := payloadDecoders.Get().(*payloadDecoder)
	defer payloadDecoders.Put(d)
	d.r.Reset(payload)
	d.dec.Reset(&d.r, payloadDecodeOptions...)
	v, ok := d.value()
	if ok {
		_, err := d.dec.ReadToken()
		ok = err == io.EOF
	}
	d.r.Reset(nil)
	return v, ok
}

// wp4bCheckDecode asserts the decode Validate uses agrees with the
// reference: same verdict, same error text, same value. It also checks
// the walk on its own: whenever it accepts, the reference must accept
// with an equal value (the property the fallback cannot rescue), and
// whenever the reference accepts, the walk must too (or every valid
// payload would pay both decodes).
func wp4bCheckDecode(t *testing.T, payload []byte) (accepted bool) {
	t.Helper()
	if !utf8.Valid(payload) {
		return false // Validate refuses it before decoding
	}
	want, wantErr := decodePayloadSlow(payload)
	got, gotErr := decodePayload(payload)
	switch {
	case (wantErr == nil) != (gotErr == nil):
		t.Fatalf("%q: verdict differs: reference err=%v, decodePayload err=%v", payload, wantErr, gotErr)
	case wantErr != nil && wantErr.Error() != gotErr.Error():
		t.Fatalf("%q: error text differs:\nreference:    %v\ndecodePayload: %v", payload, wantErr, gotErr)
	case wantErr == nil && !reflect.DeepEqual(want, got):
		t.Fatalf("%q: value differs:\nreference:    %#v\ndecodePayload: %#v", payload, want, got)
	}
	fast, ok := wp4bFastWalk(payload)
	if ok && wantErr != nil {
		t.Fatalf("%q: token walk accepted what the reference rejects (%v)", payload, wantErr)
	}
	if ok && !reflect.DeepEqual(want, fast) {
		t.Fatalf("%q: token walk value differs:\nreference: %#v\nwalk:      %#v", payload, want, fast)
	}
	if !ok && wantErr == nil {
		t.Fatalf("%q: token walk gave up on a payload the reference accepts", payload)
	}
	return wantErr == nil
}

func TestWP4BDecodePayloadSeeds(t *testing.T) {
	for _, s := range wp4bSeeds {
		wp4bCheckDecode(t, []byte(s))
	}
	// jsontext and encoding/json share the nesting limit.
	for _, depth := range []int{9999, 10000, 10001, 20000} {
		for _, open := range []string{"[", `{"a":`} {
			closeCh := "]"
			if open != "[" {
				closeCh = "}"
			}
			p := strings.Repeat(open, depth) + "1" + strings.Repeat(closeCh, depth)
			wp4bCheckDecode(t, []byte(p))
		}
	}
}

// TestWP4BDecodePayloadRandomCorpus is the differential test: generated
// JSON (duplicate names, every escape, numbers around the exponent
// bound, arbitrary whitespace) and byte-level mutations of it and of
// the seeds, each decoded by both paths.
func TestWP4BDecodePayloadRandomCorpus(t *testing.T) {
	generated, mutated := 400_000, 1_600_000
	if raceEnabled || testing.Short() {
		generated, mutated = 20_000, 80_000
	}
	rng := rand.New(rand.NewSource(20260926))
	corpus := make([][]byte, 0, 256)
	for _, s := range wp4bSeeds {
		corpus = append(corpus, []byte(s))
	}
	var acceptedGen, acceptedMut int
	for i := 0; i < generated; i++ {
		// Generated text is valid JSON; only an exponent past the bound
		// makes the reference reject it.
		p := wp4bGenJSON(rng)
		if wp4bCheckDecode(t, p) {
			acceptedGen++
		}
		if len(corpus) < cap(corpus) {
			corpus = append(corpus, p)
		} else {
			corpus[rng.Intn(len(corpus))] = p
		}
	}
	alphabet := []byte(`{}[]:,"\ue0E+-.19atrufnlsx dD8` + "\t\n\xc3\xa9\x00\x7f")
	for i := 0; i < mutated; i++ {
		p := append([]byte(nil), corpus[rng.Intn(len(corpus))]...)
		for m := 1 + rng.Intn(4); m > 0; m-- {
			switch rng.Intn(4) {
			case 0:
				if len(p) > 0 {
					p[rng.Intn(len(p))] = alphabet[rng.Intn(len(alphabet))]
				}
			case 1:
				pos := rng.Intn(len(p) + 1)
				p = append(p[:pos], append([]byte{alphabet[rng.Intn(len(alphabet))]}, p[pos:]...)...)
			case 2:
				if len(p) > 0 {
					pos := rng.Intn(len(p))
					p = append(p[:pos], p[pos+1:]...)
				}
			case 3:
				// Splice a chunk of another corpus entry in.
				other := corpus[rng.Intn(len(corpus))]
				if len(other) > 0 {
					from := rng.Intn(len(other))
					to := from + rng.Intn(len(other)-from) + 1
					pos := rng.Intn(len(p) + 1)
					p = append(p[:pos], append(append([]byte(nil), other[from:to]...), p[pos:]...)...)
				}
			}
		}
		if wp4bCheckDecode(t, p) {
			acceptedMut++
		}
	}
	t.Logf("generated=%d (accepted %d) mutated=%d (accepted %d)", generated, acceptedGen, mutated, acceptedMut)
}

// TestWP4BDecodePayloadKeepsNoAliases decodes many payloads through the
// pool, keeps every result, and only then compares them: a value that
// aliased a pooled decoder's buffer would have been overwritten by a
// later decode.
func TestWP4BDecodePayloadKeepsNoAliases(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	type pair struct {
		payload []byte
		got     any
	}
	var kept []pair
	for i := 0; i < 5000; i++ {
		p := wp4bGenJSON(rng)
		got, err := decodePayload(p)
		if err != nil {
			continue // an exponent past the bound
		}
		kept = append(kept, pair{p, got})
	}
	for _, k := range kept {
		want, err := decodePayloadSlow(k.payload)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, k.got) {
			t.Fatalf("%q: kept value changed after later decodes:\nwant %#v\ngot  %#v", k.payload, want, k.got)
		}
	}
}

// TestWP4BDecodePayloadConcurrent runs the pooled decoder from many
// goroutines under the race detector.
func TestWP4BDecodePayloadConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 500; i++ {
				p := wp4bGenJSON(rng)
				want, wantErr := decodePayloadSlow(p)
				got, err := decodePayload(p)
				if (err == nil) != (wantErr == nil) || !reflect.DeepEqual(want, got) {
					t.Errorf("%q: got %#v, %v; want %#v, %v", p, got, err, want, wantErr)
					return
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

// TestWP4BValidateMatchesReference compares Validate's verdicts with
// the decode it replaced, against schemas whose keywords read the
// decoded value (types, bounds, multipleOf, patterns, enums).
func TestWP4BValidateMatchesReference(t *testing.T) {
	schemas := []string{
		wp4bDriverSchema,
		benchSchema,
		`{"type":"object","properties":{"a":{"type":"integer","multipleOf":3,"maximum":1e30},"b":{"enum":["x","�",1.5,null]},"c":{"type":"array","maxItems":3,"uniqueItems":true}},"additionalProperties":{"type":["string","number"]}}`,
		`{"type":["array","number","string","null","boolean"],"minimum":-5,"pattern":"^[a-f]*$"}`,
	}
	rng := rand.New(rand.NewSource(99))
	n := 50_000
	if raceEnabled || testing.Short() {
		n = 5_000
	}
	for i, raw := range schemas {
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(raw)); err != nil {
			t.Fatalf("schema %d: %v", i, err)
		}
		compiled := r.schemas["t"][1]
		check := func(p []byte) {
			want := wp4bReferenceValidate(compiled, p)
			got := r.Validate(context.Background(), "t", p)
			if (want == nil) != (got == nil) {
				t.Fatalf("schema %d, payload %q: reference err=%v, Validate err=%v", i, p, want, got)
			}
		}
		for _, s := range wp4bSeeds {
			check([]byte(s))
		}
		for j := 0; j < n; j++ {
			check(wp4bGenJSON(rng))
		}
	}
}

// wp4bReferenceValidate is Validate as it was before decodePayload.
func wp4bReferenceValidate(compiled *jsonschema.Schema, payload []byte) error {
	if !utf8.Valid(payload) {
		return errors.New("schema: invalid JSON payload: not valid UTF-8")
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}
	if err := checkNumberExponents(payload); err != nil {
		return fmt.Errorf("schema: invalid JSON payload: %w", err)
	}
	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("schema: %w", boundedError(err, maxValidationErrorBytes))
	}
	return nil
}

// FuzzWP4BDecodePayload is the differential check as a fuzz target.
func FuzzWP4BDecodePayload(f *testing.F) {
	for _, s := range wp4bSeeds {
		f.Add([]byte(s))
	}
	rng := rand.New(rand.NewSource(1))
	for range 32 {
		f.Add(wp4bGenJSON(rng))
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		wp4bCheckDecode(t, payload)
	})
}

// wp4bGenJSON generates one valid JSON text biased toward what a decoder
// can get wrong.
func wp4bGenJSON(rng *rand.Rand) []byte {
	var b []byte
	b = wp4bSpace(rng, b)
	b = wp4bGenValue(rng, b, 0)
	return wp4bSpace(rng, b)
}

func wp4bSpace(rng *rand.Rand, b []byte) []byte {
	if rng.Intn(3) != 0 {
		return b
	}
	for n := rng.Intn(3); n >= 0; n-- {
		b = append(b, " \t\n\r"[rng.Intn(4)])
	}
	return b
}

var wp4bNames = []string{"a", "b", "id", "e", "", "é", `\u0061`, `\ud800`, "1e9999", `\"`, "key with space"}

func wp4bGenValue(rng *rand.Rand, b []byte, depth int) []byte {
	kind := rng.Intn(10)
	if depth > 5 && kind >= 8 {
		kind = rng.Intn(8)
	}
	switch kind {
	case 0:
		return append(b, "null"...)
	case 1:
		return append(b, "true"...)
	case 2:
		return append(b, "false"...)
	case 3, 4:
		return wp4bGenNumber(rng, b)
	case 5, 6, 7:
		return wp4bGenString(rng, b)
	case 8:
		b = append(b, '[')
		for i, n := 0, rng.Intn(5); i < n; i++ {
			if i > 0 {
				b = append(b, ',')
			}
			b = wp4bSpace(rng, b)
			b = wp4bGenValue(rng, b, depth+1)
			b = wp4bSpace(rng, b)
		}
		return append(b, ']')
	default:
		b = append(b, '{')
		for i, n := 0, rng.Intn(5); i < n; i++ {
			if i > 0 {
				b = append(b, ',')
			}
			b = wp4bSpace(rng, b)
			if rng.Intn(4) == 0 {
				b = wp4bGenString(rng, b)
			} else {
				b = append(append(append(b, '"'), wp4bNames[rng.Intn(len(wp4bNames))]...), '"')
			}
			b = wp4bSpace(rng, b)
			b = append(b, ':')
			b = wp4bSpace(rng, b)
			b = wp4bGenValue(rng, b, depth+1)
		}
		return append(b, '}')
	}
}

func wp4bGenNumber(rng *rand.Rand, b []byte) []byte {
	if rng.Intn(2) == 0 {
		b = append(b, '-')
	}
	if rng.Intn(4) == 0 {
		b = append(b, '0')
	} else {
		b = append(b, byte('1'+rng.Intn(9)))
		for n := rng.Intn(25); n > 0; n-- {
			b = append(b, byte('0'+rng.Intn(10)))
		}
	}
	if rng.Intn(3) == 0 {
		b = append(b, '.')
		for n := 1 + rng.Intn(6); n > 0; n-- {
			b = append(b, byte('0'+rng.Intn(10)))
		}
	}
	if rng.Intn(3) == 0 {
		b = append(b, "eE"[rng.Intn(2)])
		switch rng.Intn(3) {
		case 0:
			b = append(b, '+')
		case 1:
			b = append(b, '-')
		}
		for n := rng.Intn(3); n > 0; n-- {
			b = append(b, '0')
		}
		// Mostly small, sometimes right at or past MaxNumberExponent.
		exps := []int{0, 1, 7, 42, 308, 999, 1000, 1001, 1500, 99999}
		b = fmt.Appendf(b, "%d", exps[rng.Intn(len(exps))])
	}
	return b
}

var wp4bStringParts = []string{
	"a", "Z", " ", "é", "€", "😀", "ǅ", `\n`, `\t`, `\"`, `\\`, `\/`, `\b`, `\f`, `\r`,
	`\u0000`, `\u001f`, `\u00e9`, `\uffff`, `\ud800`, `\udbff`, `\udc00`, `\udfff`, `\ud83d\ude00`, `\uD83D\uDE00`,
	`\ud800\ud800`, `\udc00\ud800`, "1e9999", "[", "{", ":", ",",
}

func wp4bGenString(rng *rand.Rand, b []byte) []byte {
	b = append(b, '"')
	for n := rng.Intn(6); n > 0; n-- {
		b = append(b, wp4bStringParts[rng.Intn(len(wp4bStringParts))]...)
	}
	return append(b, '"')
}
