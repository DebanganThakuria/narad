package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWP8ErrorBodyIsValidJSON: an error message can carry a decoded
// path value, so any byte may reach writeError. strconv.AppendQuote
// wrote Go escapes (\x01) that JSON forbids and passed invalid UTF-8
// through, so a client could not parse the error at all.
func TestWP8ErrorBodyIsValidJSON(t *testing.T) {
	s := &Set{Deps: Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	for _, tc := range []struct{ msg, want string }{
		{msg: "topic \"a\x01b\" not found", want: "topic \"a\u0001b\" not found"},
		{msg: "bad key \xff\xfe", want: "bad key ��"},
		{msg: "tab\tline\nsep end\\", want: "tab\tline\nsep end\\"},
		{msg: "plain", want: "plain"},
	} {
		rec := httptest.NewRecorder()
		s.WriteError(rec, http.StatusBadRequest, tc.msg)
		body := rec.Body.Bytes()
		if !json.Valid(body) {
			t.Fatalf("error body for %q is not valid JSON: %s", tc.msg, body)
		}
		var got struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if got.Error != tc.want {
			t.Fatalf("error = %q, want %q", got.Error, tc.want)
		}
	}
}

// wp8DiscardWriter is a ResponseWriter that keeps nothing.
type wp8DiscardWriter struct{ h http.Header }

func (w *wp8DiscardWriter) Header() http.Header         { return w.h }
func (w *wp8DiscardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *wp8DiscardWriter) WriteHeader(int)             {}

// BenchmarkWP8WriteError is the cost of one JSON error response.
func BenchmarkWP8WriteError(b *testing.B) {
	s := &Set{Deps: Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	w := &wp8DiscardWriter{h: make(http.Header)}
	b.ReportAllocs()
	for range b.N {
		s.WriteError(w, http.StatusNotFound, `topic "orders-2026" not found`)
	}
}
