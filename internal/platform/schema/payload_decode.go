package schema

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"io"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// maxPooledPayloadBytes is the largest payload whose decoder goes back
// to the pool. A decoder keeps its buffer, which holds a copy of the
// last payload it read; pooling one after a 1 MiB produce would pin a
// megabyte per P until a GC empties the pool. Payloads this large are
// rare and get a fresh decoder each, as every payload used to.
const maxPooledPayloadBytes = 64 << 10

// payloadDecodeOptions reproduce what jsonschema.UnmarshalJSON (the
// encoding/json v1 decoder) accepts: a duplicate key keeps its last
// value, and an escaped lone surrogate ("\ud800") decodes to U+FFFD.
// Raw invalid UTF-8 never gets here; Validate refuses it first.
var payloadDecodeOptions = []jsontext.Options{
	jsontext.AllowDuplicateNames(true),
	jsontext.AllowInvalidUTF8(true),
}

// MaxPayloadDepth is the deepest nesting of objects and arrays a
// payload to a schema topic may have: [[1]] nests two levels. Deeper
// payloads are refused before validation. The validator builds an
// error at every level above a failing value, each carrying a copy of
// the whole path to it, so the cost of one deep invalid payload grows
// with the square of its depth: a 20 KB body nested 9990 levels (the
// decoder's own limit) held 0.8 GB, and a union schema three times
// that. At 256 levels the path copies of one error chain stay under
// half a megabyte, and real documents nest a handful of levels.
const MaxPayloadDepth = 256

// errPayloadTooDeep refuses a payload nested deeper than
// MaxPayloadDepth. Both decode paths return this exact value.
var errPayloadTooDeep error = &payloadDepthError{}

// payloadDepthError is the refusal of a payload nested deeper than
// MaxPayloadDepth. It is the client's to fix: the produce path reports
// it as an invalid payload (400).
type payloadDepthError struct{}

func (*payloadDepthError) Error() string {
	return "payload nests deeper than 256 levels; schema topics accept at most 256 levels of nested objects and arrays"
}

// payloadStats describes a decoded payload's shape, which bounds what
// a detailed validation report can cost: the validator can build an
// error for every value, and each one copies the path to its value.
type payloadStats struct {
	values   int // JSON values, containers and scalars alike
	pathLen  int // sum over values of their depth (a child of the root has depth 1)
	maxDepth int // deepest nesting of containers
}

// boundStats bounds the stats of a payload of n bytes nesting
// maxDepth levels without walking it: every value takes at least a
// byte, and none sits deeper than maxDepth.
func boundStats(n, maxDepth int) payloadStats {
	return payloadStats{values: n, pathLen: n * maxDepth, maxDepth: maxDepth}
}

// payloadDecoder is a reusable token decoder. It reads through a
// bytes.Reader, which the decoder copies into its own buffer: the
// payload is written to the WAL after validation, so the decoder must
// never alias it (a bytes.Buffer source would).
type payloadDecoder struct {
	r   bytes.Reader
	dec *jsontext.Decoder

	maxDepth int  // deepest nesting of containers seen
	tooDeep  bool // the walk stopped at a container past MaxPayloadDepth
}

var payloadDecoders = sync.Pool{New: func() any {
	d := &payloadDecoder{}
	d.dec = jsontext.NewDecoder(&d.r)
	return d
}}

// decodePayload decodes a produce payload, already known to be valid
// UTF-8, into the value the validator takes: map[string]any, []any,
// string, bool, nil and json.Number. The result is exactly what
// jsonschema.UnmarshalJSON returns, but built by walking tokens from a
// pooled decoder instead of reflecting through encoding/json, which
// was most of Validate's cost on typical payloads.
//
// When the walk gives up (a syntax error, trailing data, or a number
// exponent outside ±MaxNumberExponent), the payload goes through the
// old decode and exponent check, so a rejected payload gets the error
// it always did and the walk only ever has to agree with the old
// decode on payloads it accepts.
func decodePayload(payload []byte) (any, error) {
	v, _, err := decodePayloadDepth(payload)
	return v, err
}

// decodePayloadDepth is decodePayload that also reports how deep the
// payload nests. A payload nested deeper than MaxPayloadDepth is
// refused with errPayloadTooDeep as soon as the walk reaches the level
// past it.
func decodePayloadDepth(payload []byte) (any, int, error) {
	d := payloadDecoders.Get().(*payloadDecoder)
	d.r.Reset(payload)
	d.dec.Reset(&d.r, payloadDecodeOptions...)
	d.maxDepth, d.tooDeep = 0, false
	v, ok := d.value(0)
	if ok {
		// One JSON text must fill the payload.
		_, err := d.dec.ReadToken()
		ok = err == io.EOF
	}
	maxDepth, tooDeep := d.maxDepth, d.tooDeep
	d.r.Reset(nil)
	if len(payload) <= maxPooledPayloadBytes {
		payloadDecoders.Put(d)
	}
	switch {
	case ok:
		return v, maxDepth, nil
	case tooDeep:
		// Every token up to the level past the limit was well formed,
		// so the reference decode would see the same depth.
		return nil, 0, errPayloadTooDeep
	}
	v, err := decodePayloadSlow(payload)
	if err != nil {
		return nil, 0, err
	}
	return v, statsOf(v, 0, &payloadStats{}).maxDepth, nil
}

// decodePayloadSlow is the reference decode: the depth bound,
// encoding/json with json.Number, a single JSON text, then the exponent
// bound. Its errors are the ones clients see for a malformed payload.
// The depth is checked first, on the raw bytes, so a payload too deep
// to validate is never built into a value.
func decodePayloadSlow(payload []byte) (any, error) {
	if jsonNestingDepth(payload) > MaxPayloadDepth {
		return nil, errPayloadTooDeep
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if err := checkNumberExponents(payload); err != nil {
		return nil, err
	}
	return instance, nil
}

// statsOf measures an already decoded value, whose depth is known to
// be within MaxPayloadDepth.
func statsOf(v any, depth int, st *payloadStats) payloadStats {
	st.values++
	st.pathLen += depth
	switch t := v.(type) {
	case map[string]any:
		st.maxDepth = max(st.maxDepth, depth+1)
		for _, child := range t {
			statsOf(child, depth+1, st)
		}
	case []any:
		st.maxDepth = max(st.maxDepth, depth+1)
		for _, child := range t {
			statsOf(child, depth+1, st)
		}
	}
	return *st
}

// value reads one JSON value at the given depth (the root's is 0). ok
// is false when the walk has to give up; tooDeep is set when it gave up
// because a container would nest past MaxPayloadDepth.
func (d *payloadDecoder) value(depth int) (any, bool) {
	tok, err := d.dec.ReadToken()
	if err != nil {
		return nil, false
	}
	switch k := tok.Kind(); k {
	case jsontext.KindNull:
		return nil, true
	case jsontext.KindTrue:
		return true, true
	case jsontext.KindFalse:
		return false, true
	case jsontext.KindString:
		return tok.String(), true
	case jsontext.KindNumber:
		lit := tok.String()
		if strings.ContainsAny(lit, "eE") && checkNumberExponents([]byte(lit)) != nil {
			return nil, false
		}
		return json.Number(lit), true
	case jsontext.KindBeginObject, jsontext.KindBeginArray:
		if depth >= MaxPayloadDepth {
			d.tooDeep = true
			return nil, false
		}
		d.maxDepth = max(d.maxDepth, depth+1)
		if k == jsontext.KindBeginArray {
			return d.array(depth)
		}
		m := map[string]any{}
		for d.dec.PeekKind() != jsontext.KindEndObject {
			name, err := d.dec.ReadToken()
			if err != nil {
				return nil, false
			}
			// Copy the name out before the next read: a token aliases
			// the decoder's buffer.
			key := name.String()
			v, ok := d.value(depth + 1)
			if !ok {
				return nil, false
			}
			m[key] = v
		}
		if _, err := d.dec.ReadToken(); err != nil {
			return nil, false
		}
		return m, true
	}
	return nil, false
}

// array reads the elements of an array whose opening bracket, at the
// given depth, was just read.
func (d *payloadDecoder) array(depth int) (any, bool) {
	a := []any{}
	for d.dec.PeekKind() != jsontext.KindEndArray {
		v, ok := d.value(depth + 1)
		if !ok {
			return nil, false
		}
		a = append(a, v)
	}
	if _, err := d.dec.ReadToken(); err != nil {
		return nil, false
	}
	return a, true
}
