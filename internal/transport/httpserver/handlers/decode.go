package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// ReadBody reads the request body up to limit bytes. On failure it
// responds to the client (413 for an oversize body, 400 otherwise)
// and returns false.
func (s *Set) ReadBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
		return nil, false
	}
	return body, true
}

// DecodeJSON reads a JSON body in strict mode (unknown fields
// rejected). Responds to the client on failure and returns false.
func (s *Set) DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxJSONBodyBytes))
	return s.decodeJSON(w, dec, dst)
}

// DecodeJSONBytes decodes body with the same strict rules as
// DecodeJSON. Handlers use it when the raw body has already been read
// so it can also be forwarded verbatim to another node.
func (s *Set) DecodeJSONBytes(w http.ResponseWriter, body []byte, dst any) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	return s.decodeJSON(w, dec, dst)
}

func (s *Set) decodeJSON(w http.ResponseWriter, dec *json.Decoder, dst any) bool {
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			s.WriteError(w, http.StatusBadRequest, "invalid json: multiple JSON values")
			return false
		}
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

// Validator is implemented by request types that carry their own
// post-decode invariants.
type Validator interface {
	Validate() error
}

// DecodeAndValidate is DecodeJSON followed by dst.Validate(). Bad
// validation responds with 400 and returns false.
func (s *Set) DecodeAndValidate(w http.ResponseWriter, r *http.Request, dst Validator) bool {
	if !s.DecodeJSON(w, r, dst) {
		return false
	}
	if err := dst.Validate(); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}
