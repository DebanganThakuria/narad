package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// exactBodyReadMax is the largest declared Content-Length ReadBody
// allocates in full before any body bytes arrive. Above it the body is
// read incrementally, so memory follows the bytes actually received: a
// client that declares 1 MiB and then stalls pins at most this much
// until the read timeout, not the declared size.
const exactBodyReadMax = 64 << 10

// errBodyOverrun reports a body longer than its declared length.
var errBodyOverrun = errors.New("body longer than declared length")

// ReadBody reads the request body up to limit bytes. On failure it
// responds to the client (413 for an oversize body, 400 otherwise)
// and returns false.
//
// When the client declared a Content-Length within the limit the body
// is read by readDeclaredBody, which never grows past the declared
// length (see there). Unknown or negative lengths keep the
// MaxBytesReader path.
func (s *Set) ReadBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	if n := r.ContentLength; n > 0 && n <= limit {
		body, err := readDeclaredBody(r.Body, n)
		switch {
		case errors.Is(err, errBodyOverrun):
			s.WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		case err != nil:
			s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
			return nil, false
		}
		return body, true
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			s.WriteError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
		return nil, false
	}
	return body, true
}

// readDeclaredBody reads a body that declared length n and checks it
// is exactly that long: a short body is io.ErrUnexpectedEOF (or the
// reader's own error) and one extra byte is errBodyOverrun. The buffer
// has room for n+1 bytes so the overrun probe reads into it too.
//
// Up to exactBodyReadMax the buffer is allocated once at full size,
// instead of through io.ReadAll's grow-and-copy loop (a produce payload
// used to cost several intermediate buffers). A larger body starts at
// exactBodyReadMax and grows fourfold, straight to n+1 once that covers
// the declared length, only when the buffer is full. A stalled client
// therefore holds exactBodyReadMax or four times what it sent, whichever
// is larger, and an honest 1 MiB body pays two extra buffers (64 KiB and
// 256 KiB); doubling would halve that bound but double the copies.
func readDeclaredBody(body io.Reader, n int64) ([]byte, error) {
	size := int(n) + 1
	initial := size
	if n > exactBodyReadMax {
		initial = exactBodyReadMax
	}
	buf := make([]byte, 0, initial)
	for {
		if len(buf) == cap(buf) {
			next := 4 * cap(buf)
			if next >= int(n) {
				next = size
			}
			grown := make([]byte, len(buf), next)
			copy(grown, buf)
			buf = grown
		}
		m, err := body.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+m]
		switch {
		case int64(len(buf)) > n:
			return nil, errBodyOverrun
		case err == io.EOF:
			if int64(len(buf)) < n {
				return nil, io.ErrUnexpectedEOF
			}
			return buf, nil
		case err != nil:
			return nil, err
		}
	}
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
