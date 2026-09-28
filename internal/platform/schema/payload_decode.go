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

// payloadDecoder is a reusable token decoder. It reads through a
// bytes.Reader, which the decoder copies into its own buffer: the
// payload is written to the WAL after validation, so the decoder must
// never alias it (a bytes.Buffer source would).
type payloadDecoder struct {
	r   bytes.Reader
	dec *jsontext.Decoder
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
	d := payloadDecoders.Get().(*payloadDecoder)
	d.r.Reset(payload)
	d.dec.Reset(&d.r, payloadDecodeOptions...)
	v, ok := d.value()
	if ok {
		// One JSON text must fill the payload.
		_, err := d.dec.ReadToken()
		ok = err == io.EOF
	}
	d.r.Reset(nil)
	if len(payload) <= maxPooledPayloadBytes {
		payloadDecoders.Put(d)
	}
	if ok {
		return v, nil
	}
	return decodePayloadSlow(payload)
}

// decodePayloadSlow is the reference decode: encoding/json with
// json.Number, a single JSON text, then the exponent bound. Its errors
// are the ones clients see for a malformed payload.
func decodePayloadSlow(payload []byte) (any, error) {
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if err := checkNumberExponents(payload); err != nil {
		return nil, err
	}
	return instance, nil
}

// value reads one JSON value. ok is false when the walk has to give up.
func (d *payloadDecoder) value() (any, bool) {
	tok, err := d.dec.ReadToken()
	if err != nil {
		return nil, false
	}
	switch tok.Kind() {
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
	case jsontext.KindBeginObject:
		m := map[string]any{}
		for d.dec.PeekKind() != jsontext.KindEndObject {
			name, err := d.dec.ReadToken()
			if err != nil {
				return nil, false
			}
			// Copy the name out before the next read: a token aliases
			// the decoder's buffer.
			key := name.String()
			v, ok := d.value()
			if !ok {
				return nil, false
			}
			m[key] = v
		}
		if _, err := d.dec.ReadToken(); err != nil {
			return nil, false
		}
		return m, true
	case jsontext.KindBeginArray:
		a := []any{}
		for d.dec.PeekKind() != jsontext.KindEndArray {
			v, ok := d.value()
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
	return nil, false
}
