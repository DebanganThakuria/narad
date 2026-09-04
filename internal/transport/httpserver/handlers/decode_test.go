package handlers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReadBodyPreSizedPath covers the Content-Length fast path: an
// honest declared length within the limit is read in full, a body
// longer than its declared length is a 413, and a body shorter than
// its declared length is a 400 (the connection died mid-body).
func TestReadBodyPreSizedPath(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	const limit = 64

	t.Run("exact length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello narad"))
		if req.ContentLength != 11 {
			t.Fatalf("test setup: ContentLength = %d", req.ContentLength)
		}
		res := httptest.NewRecorder()
		body, ok := s.ReadBody(res, req, limit)
		if !ok {
			t.Fatalf("ReadBody() ok = false, status %d body %s", res.Code, res.Body.String())
		}
		if string(body) != "hello narad" {
			t.Fatalf("ReadBody() = %q", body)
		}
		if cap(body) != 12 {
			t.Fatalf("ReadBody() cap = %d, want exactly ContentLength+1", cap(body))
		}
	})

	t.Run("length equal to limit", func(t *testing.T) {
		payload := bytes.Repeat([]byte("x"), limit)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
		res := httptest.NewRecorder()
		body, ok := s.ReadBody(res, req, limit)
		if !ok || len(body) != limit {
			t.Fatalf("ReadBody() = %d bytes, ok %v; status %d", len(body), ok, res.Code)
		}
	})

	t.Run("body longer than declared length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello narad, and then some"))
		req.ContentLength = 11 // lies: the reader has more
		res := httptest.NewRecorder()
		if _, ok := s.ReadBody(res, req, limit); ok {
			t.Fatal("ReadBody() ok = true for a body longer than Content-Length")
		}
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", res.Code, http.StatusRequestEntityTooLarge)
		}
	})

	t.Run("body shorter than declared length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
		req.ContentLength = 11
		res := httptest.NewRecorder()
		if _, ok := s.ReadBody(res, req, limit); ok {
			t.Fatal("ReadBody() ok = true for a truncated body")
		}
		if res.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", res.Code, http.StatusBadRequest)
		}
	})
}

// TestReadBodyLimitPath covers the MaxBytesReader path: a declared
// length beyond the limit and an undeclared (chunked) body beyond the
// limit both map to 413; an undeclared body within the limit reads.
func TestReadBodyLimitPath(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	const limit = 16

	t.Run("declared length above limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bytes.Repeat([]byte("x"), limit+1)))
		if req.ContentLength != limit+1 {
			t.Fatalf("test setup: ContentLength = %d", req.ContentLength)
		}
		res := httptest.NewRecorder()
		if _, ok := s.ReadBody(res, req, limit); ok {
			t.Fatal("ReadBody() ok = true for an oversize declared body")
		}
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", res.Code, http.StatusRequestEntityTooLarge)
		}
	})

	t.Run("unknown length above limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), limit+1))))
		req.ContentLength = -1
		res := httptest.NewRecorder()
		if _, ok := s.ReadBody(res, req, limit); ok {
			t.Fatal("ReadBody() ok = true for an oversize chunked body")
		}
		if res.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", res.Code, http.StatusRequestEntityTooLarge)
		}
	})

	t.Run("unknown length within limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", io.NopCloser(strings.NewReader("chunked")))
		req.ContentLength = -1
		res := httptest.NewRecorder()
		body, ok := s.ReadBody(res, req, limit)
		if !ok || string(body) != "chunked" {
			t.Fatalf("ReadBody() = %q, ok %v; status %d", body, ok, res.Code)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		res := httptest.NewRecorder()
		body, ok := s.ReadBody(res, req, limit)
		if !ok || len(body) != 0 {
			t.Fatalf("ReadBody() = %q, ok %v; status %d", body, ok, res.Code)
		}
	})
}
