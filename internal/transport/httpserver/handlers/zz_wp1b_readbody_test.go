package handlers

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"testing/iotest"
)

// wp1bStallReader is a client that declared a large body, sent a few
// bytes and stopped: after data it fails the way the server's read
// deadline makes a stalled body fail.
type wp1bStallReader struct {
	data []byte
}

func (r *wp1bStallReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, errors.New("i/o timeout")
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestWP1BReadBodyMemoryTracksReceivedBytes: a declared Content-Length
// must not be allocated up front. A client that declares 1 MiB and
// sends one byte would otherwise pin 1 MiB per connection until the
// read timeout, and thousands of such connections exhaust the heap.
func TestWP1BReadBodyMemoryTracksReceivedBytes(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	const (
		declared = 1 << 20
		rounds   = 16
	)
	run := func() {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Body = io.NopCloser(&wp1bStallReader{data: []byte("x")})
		req.ContentLength = declared
		res := httptest.NewRecorder()
		if _, ok := s.ReadBody(res, req, MaxMessageBodyBytes); ok {
			t.Fatal("ReadBody() ok = true for a stalled body")
		}
		if res.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", res.Code, http.StatusBadRequest)
		}
	}
	run() // warm up lazily initialized state

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range rounds {
		run()
	}
	runtime.ReadMemStats(&after)
	perRound := (after.TotalAlloc - before.TotalAlloc) / rounds
	if perRound > 128<<10 {
		t.Fatalf("ReadBody allocated %d bytes per stalled request with 1 byte received, want memory to track received bytes (<= 128 KiB)", perRound)
	}
}

// TestWP1BReadBodyDeclaredLengths covers honest and dishonest declared
// lengths on both sides of the exact-size threshold, including readers
// that return data in small pieces or together with io.EOF.
func TestWP1BReadBodyDeclaredLengths(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	sizes := []int{1, 100, exactBodyReadMax - 1, exactBodyReadMax, exactBodyReadMax + 1, 3*exactBodyReadMax + 7, int(MaxMessageBodyBytes)}
	wraps := map[string]func(io.Reader) io.Reader{
		"plain":    func(r io.Reader) io.Reader { return r },
		"one byte": iotest.OneByteReader,
		"half":     iotest.HalfReader,
		"data+EOF": iotest.DataErrReader,
	}
	for _, size := range sizes {
		payload := bytes.Repeat([]byte{'a', 'b', 'c'}, size/3+1)[:size]
		for name, wrap := range wraps {
			if name == "one byte" && size > 4*exactBodyReadMax {
				continue // slow and adds nothing over the smaller sizes
			}
			read := func(body []byte, declared int64) (*httptest.ResponseRecorder, []byte, bool) {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Body = io.NopCloser(wrap(bytes.NewReader(body)))
				req.ContentLength = declared
				res := httptest.NewRecorder()
				got, ok := s.ReadBody(res, req, MaxMessageBodyBytes)
				return res, got, ok
			}

			t.Run(name+"/exact", func(t *testing.T) {
				res, got, ok := read(payload, int64(size))
				if !ok {
					t.Fatalf("size %d: ok = false, status %d %s", size, res.Code, res.Body.String())
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("size %d: body mismatch (got %d bytes)", size, len(got))
				}
			})
			t.Run(name+"/longer than declared", func(t *testing.T) {
				res, _, ok := read(append(bytes.Clone(payload), 'z'), int64(size))
				if ok || res.Code != http.StatusRequestEntityTooLarge {
					t.Fatalf("size %d: ok %v status %d, want 413", size, ok, res.Code)
				}
			})
			t.Run(name+"/shorter than declared", func(t *testing.T) {
				res, _, ok := read(payload[:size-1], int64(size))
				if ok || res.Code != http.StatusBadRequest {
					t.Fatalf("size %d: ok %v status %d, want 400", size, ok, res.Code)
				}
			})
		}
	}
}

// TestWP1BReadBodyLargeBodyCapacity bounds the slack an honest large
// body keeps: the incremental read grows toward the declared length,
// so the final buffer holds the body plus the one-byte overrun probe.
func TestWP1BReadBodyLargeBodyCapacity(t *testing.T) {
	s := newTestSet(&fakeBroker{})
	for _, size := range []int{exactBodyReadMax + 1, 200_000, int(MaxMessageBodyBytes)} {
		payload := bytes.Repeat([]byte("x"), size)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
		res := httptest.NewRecorder()
		got, ok := s.ReadBody(res, req, MaxMessageBodyBytes)
		if !ok || len(got) != size {
			t.Fatalf("size %d: ok %v len %d status %d", size, ok, len(got), res.Code)
		}
		if cap(got) != size+1 {
			t.Fatalf("size %d: cap = %d, want %d", size, cap(got), size+1)
		}
	}
}
