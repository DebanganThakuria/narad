package messaging

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// zzPerfDSameArray reports whether a and b are non-empty slices that
// start at the same element of one backing array.
func zzPerfDSameArray(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

// TestPerfDCaptureKeepsBodyWithoutCopy checks that a body handed over
// with KeepBody is kept as it is, not copied: the capture's body is the
// caller's slice, clipped to its length, the status defaults to 200 as
// for Write, and the headers set before stay as they were.
func TestPerfDCaptureKeepsBodyWithoutCopy(t *testing.T) {
	body := []byte(`{"messages":[{"offset":1}]}` + "\n")

	c := &captureWriter{}
	c.Header().Set("Content-Type", "application/json")
	c.Header().Set("X-Content-Type-Options", "nosniff")
	c.KeepBody(body)

	if !zzPerfDSameArray(c.body, body) || len(c.body) != len(body) {
		t.Fatalf("kept body is a copy (or a different length): got %d bytes, want the caller's %d-byte slice itself", len(c.body), len(body))
	}
	if cap(c.body) != len(body) {
		t.Fatalf("kept body cap = %d, want %d (clipped, so a later append cannot reach the caller's spare capacity)", cap(c.body), len(body))
	}
	if c.status != http.StatusOK || c.code() != http.StatusOK {
		t.Fatalf("status = %d (code %d), want 200 for a kept body with no WriteHeader", c.status, c.code())
	}
	if got := c.header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := c.header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if first, ok := c.message(); !ok || !bytes.Equal(first, bytes.TrimRight(body, "\n")) {
		t.Fatalf("message() = %q, %v; want the kept body without its newline", first, ok)
	}

	// A status written first is kept: KeepBody only defaults it.
	c = &captureWriter{}
	c.WriteHeader(http.StatusPartialContent)
	c.KeepBody(body)
	if c.code() != http.StatusPartialContent || !zzPerfDSameArray(c.body, body) {
		t.Fatalf("after WriteHeader(206): code %d, same array %v; want 206 and the caller's slice", c.code(), zzPerfDSameArray(c.body, body))
	}

	// A body already started by Write is appended to, as Write would,
	// so the capture never aliases a slice it has to extend.
	c = &captureWriter{}
	_, _ = c.Write([]byte("ab"))
	c.KeepBody([]byte("cd"))
	if string(c.body) != "abcd" {
		t.Fatalf("Write then KeepBody = %q, want %q", c.body, "abcd")
	}
}

// TestPerfDCaptureWriteAfterKeepLeavesCallerBytes checks that a Write
// after KeepBody appends into a new array: the kept slice has spare
// capacity past its length, and the capture must never write into it.
func TestPerfDCaptureWriteAfterKeepLeavesCallerBytes(t *testing.T) {
	p := make([]byte, 3, 16)
	copy(p, "abc")
	p[:4][3] = '#'

	c := &captureWriter{}
	c.KeepBody(p)
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}

	if got := p[:4][3]; got != '#' {
		t.Fatalf("caller's spare capacity overwritten: byte 3 = %q, want '#'", got)
	}
	if string(p) != "abc" {
		t.Fatalf("caller's bytes = %q, want %q", p, "abc")
	}
	if string(c.body) != "abcx" {
		t.Fatalf("capture body = %q, want %q", c.body, "abcx")
	}
	if zzPerfDSameArray(c.body, p) {
		t.Fatal("capture body still shares the caller's array after an append")
	}
}

// TestPerfDCaptureReplayAfterKeep checks that replay sends a kept body
// unchanged, with the status and headers captured with it.
func TestPerfDCaptureReplayAfterKeep(t *testing.T) {
	body := []byte(`{"error":"owner is draining"}` + "\n")
	want := bytes.Clone(body)

	c := &captureWriter{}
	c.Header().Set("Content-Type", "application/json")
	c.WriteHeader(http.StatusServiceUnavailable)
	c.KeepBody(body)

	rec := httptest.NewRecorder()
	c.replay(rec)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("replayed status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("replayed Content-Type = %q, want application/json", got)
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("replayed body = %q, want %q", rec.Body.Bytes(), want)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("caller's body changed by the capture: %q, want %q", body, want)
	}
	if status, msg := c.outcome(); status != http.StatusServiceUnavailable || msg != "owner is draining" {
		t.Fatalf("outcome() = %d %q, want 503 %q", status, msg, "owner is draining")
	}
}
