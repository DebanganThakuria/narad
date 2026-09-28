package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// errListTooLong reports a list with more elements than its bound; the
// caller words the answer.
var errListTooLong = errors.New("list too long")

// decodeBoundedList decodes body, a JSON object whose one field, field,
// is an array of at most limit elements of T: the body of a batch
// produce ({"messages":[...]}) or a batch ack ({"receipt_handles":
// [...]}). It is as strict as DecodeJSONBytes decoding into a struct
// with that one field (unknown fields refused at every level, the field
// name matched without regard to case, null for none, nothing after the
// object), but it decodes the array one element at a time and stops at
// the element past limit with errListTooLong, and at the first element
// that does not decode.
//
// Decoding the whole array first and counting it afterwards made the
// cost of a request its element count rather than its bound: a 1 MiB
// body of [0,0,...] held about half a million elements, over 280 times
// the body in allocations, before the count check refused it. Here a
// request holds at most limit elements, and the decoder's buffer, which
// never exceeds the body.
//
// A body with fewer than limit commas cannot hold an array of more than
// limit elements, so for it the array is decoded in one call, which
// costs less per element than one decoder call each; only a body with
// more commas (a batch near the bound, payloads with many fields, or an
// attack) takes the element-at-a-time path. The two paths accept the
// same bodies; a refused one may be worded differently.
func decodeBoundedList[T any](body []byte, field string, limit int) ([]T, error) {
	return decodeBoundedListPath[T](body, field, limit, bytes.Count(body, comma) >= limit)
}

var comma = []byte{','}

// decodeBoundedListPath is decodeBoundedList with the path chosen by the
// caller: element at a time when stream is set, else the whole array in
// one call, which the caller may only choose when the body cannot hold
// more than limit elements in any array.
func decodeBoundedListPath[T any](body []byte, field string, limit int, stream bool) ([]T, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	var list []T
	switch tok {
	case nil:
		// null: a request with no list, as decoding null into the struct
		// leaves it.
	case json.Delim('{'):
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if name, _ := key.(string); !strings.EqualFold(name, field) {
				return nil, fmt.Errorf("json: unknown field %q", name)
			}
			if !stream {
				list = nil
				if err := dec.Decode(&list); err != nil {
					return nil, err
				}
				continue
			}
			if list, err = decodeListValue[T](dec, field, limit); err != nil {
				return nil, err
			}
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("json: cannot unmarshal %s into a request body, which is an object {%q:[...]}", jsonTokenKind(tok), field)
	}
	if err := checkJSONEnd(dec); err != nil {
		return nil, err
	}
	return list, nil
}

// decodeListValue decodes the value of the list field: null (no list) or
// an array of at most limit elements.
func decodeListValue[T any](dec *json.Decoder, field string, limit int) ([]T, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if tok == nil {
		return nil, nil
	}
	if tok != json.Delim('[') {
		return nil, fmt.Errorf("json: cannot unmarshal %s into field %q, which is an array", jsonTokenKind(tok), field)
	}
	var list []T
	for dec.More() {
		if len(list) == limit {
			return nil, errListTooLong
		}
		list = append(list, *new(T))
		if err := dec.Decode(&list[len(list)-1]); err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return list, nil
}

// checkJSONEnd reports anything but whitespace after the value dec
// decoded. It reads one token, so a trailing value costs no more than
// its first token (a whole string at worst, bounded by the body), where
// decoding it into an any built the whole value.
func checkJSONEnd(dec *json.Decoder) error {
	_, err := dec.Token()
	switch {
	case err == io.EOF:
		return nil
	case err == nil:
		return errors.New("multiple JSON values")
	default:
		return err
	}
}

// jsonTokenKind names a token's JSON kind for an error message.
func jsonTokenKind(tok json.Token) string {
	switch tok.(type) {
	case json.Delim:
		if tok == json.Delim('[') {
			return "array"
		}
		return "object"
	case string:
		return "string"
	case float64, json.Number:
		return "number"
	case bool:
		return "bool"
	}
	return "value"
}
