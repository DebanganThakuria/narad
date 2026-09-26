package handlers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// BenchmarkWP1BReadBody measures produce body reads for honest
// clients: small and large declared lengths, and a chunked upload of
// unknown length.
func BenchmarkWP1BReadBody(b *testing.B) {
	s := newTestSet(&fakeBroker{})
	for _, tc := range []struct {
		name    string
		size    int
		chunked bool
	}{
		{"cl-100B", 100, false},
		{"cl-4KiB", 4 << 10, false},
		{"cl-64KiB", 64 << 10, false},
		{"cl-256KiB", 256 << 10, false},
		{"cl-1MiB", 1 << 20, false},
		{"chunked-100B", 100, true},
		{"chunked-1MiB", 1 << 20, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			payload := bytes.Repeat([]byte("x"), tc.size)
			rd := bytes.NewReader(payload)
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			w := httptest.NewRecorder()
			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			for b.Loop() {
				rd.Reset(payload)
				req.Body = io.NopCloser(rd)
				req.ContentLength = int64(tc.size)
				if tc.chunked {
					req.ContentLength = -1
				}
				body, ok := s.ReadBody(w, req, MaxMessageBodyBytes)
				if !ok || len(body) != tc.size {
					b.Fatalf("ReadBody = %d bytes, ok %v", len(body), ok)
				}
			}
		})
	}
}
