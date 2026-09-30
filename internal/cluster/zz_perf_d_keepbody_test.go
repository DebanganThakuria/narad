package cluster

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzPerfDKeeper is a response writer that takes a body handed over with
// KeepBody, as the batch handlers' capture does, and counts the calls
// of each kind.
type zzPerfDKeeper struct {
	h      http.Header
	status int
	kept   []byte
	keeps  int
	writes int
}

func (k *zzPerfDKeeper) Header() http.Header {
	if k.h == nil {
		k.h = make(http.Header)
	}
	return k.h
}

func (k *zzPerfDKeeper) WriteHeader(code int) {
	if k.status == 0 {
		k.status = code
	}
}

func (k *zzPerfDKeeper) Write(p []byte) (int, error) {
	k.writes++
	return len(p), nil
}

func (k *zzPerfDKeeper) KeepBody(p []byte) {
	k.keeps++
	k.kept = p
}

// zzPerfDRecorder is a client-side response writer that keeps the
// slices it is handed, without copying, so a test can see which array
// reached the client.
type zzPerfDRecorder struct {
	h      http.Header
	status int
	chunks [][]byte
}

func (r *zzPerfDRecorder) Header() http.Header  { return r.h }
func (r *zzPerfDRecorder) WriteHeader(code int) { r.status = code }

func (r *zzPerfDRecorder) Write(p []byte) (int, error) {
	r.chunks = append(r.chunks, p)
	return len(p), nil
}

// TestPerfDWritePeerResponseHandsBodyToKeeper checks writePeerResponse's
// hand-over: a writer with KeepBody receives the owner's body slice
// itself, typed and with its status, and no Write; any other writer
// receives the bytes through Write as before; an empty body reaches
// neither.
func TestPerfDWritePeerResponseHandsBodyToKeeper(t *testing.T) {
	body := []byte(`{"messages":[{"offset":7}]}` + "\n")
	want := bytes.Clone(body)

	k := &zzPerfDKeeper{}
	writePeerResponse(k, nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: body})
	if k.keeps != 1 || k.writes != 0 {
		t.Fatalf("keeper: %d KeepBody and %d Write calls, want 1 and 0", k.keeps, k.writes)
	}
	if len(k.kept) != len(body) || &k.kept[0] != &body[0] {
		t.Fatalf("keeper was handed a %d-byte slice that is not the owner's %d-byte body", len(k.kept), len(body))
	}
	if k.status != http.StatusOK {
		t.Fatalf("keeper status = %d, want 200", k.status)
	}
	if got := k.h.Get("Content-Type"); got != nodewire.ContentTypeJSON {
		t.Fatalf("keeper Content-Type = %q, want %q", got, nodewire.ContentTypeJSON)
	}
	if got := k.h.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("keeper X-Content-Type-Options = %q, want nosniff", got)
	}

	// A writer without KeepBody (every real http.ResponseWriter) is
	// written to exactly as before.
	rec := httptest.NewRecorder()
	writePeerResponse(rec, nodewire.Response{Status: http.StatusAccepted, ContentType: nodewire.ContentTypeJSON, Body: body})
	if rec.Code != http.StatusAccepted || !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("recorder: %d %q, want 202 %q", rec.Code, rec.Body.Bytes(), want)
	}
	if got := rec.Header().Get("Content-Type"); got != nodewire.ContentTypeJSON {
		t.Fatalf("recorder Content-Type = %q, want %q", got, nodewire.ContentTypeJSON)
	}

	// An empty body, which is every forwarded ack: a bare status, and
	// neither KeepBody nor Write.
	k = &zzPerfDKeeper{}
	writePeerResponse(k, nodewire.Response{Status: http.StatusNoContent})
	if k.keeps != 0 || k.writes != 0 || k.status != http.StatusNoContent || len(k.h) != 0 {
		t.Fatalf("empty body: %d KeepBody, %d Write, status %d, headers %v; want 0, 0, 204 and none", k.keeps, k.writes, k.status, k.h)
	}
}

// TestPerfDForwardedBatchReachesClientWithoutCopy is a batch consume
// through the HTTP handler on a node that owns none of the topic: the
// owner's {"messages":[...]} body must reach the client's writer as the
// slice the peer call returned, with no copy in between (the batch
// handler's capture keeps it rather than copying it).
func TestPerfDForwardedBatchReachesClientWithoutCopy(t *testing.T) {
	const n = 8
	body := zzPerfDBatchBody(n, 4<<10)
	want := bytes.Clone(body)
	h, req := zzPerfDForwardedConsume(t, body, n)

	w := &zzPerfDRecorder{h: make(http.Header)}
	h(w, req)
	if w.status != http.StatusOK || len(w.chunks) != 1 {
		t.Fatalf("forwarded batch consume = %d in %d writes, want 200 in 1", w.status, len(w.chunks))
	}
	got := w.chunks[0]
	if !bytes.Equal(got, want) {
		t.Fatalf("client body (%d bytes) differs from the owner's (%d bytes)", len(got), len(want))
	}
	if &got[0] != &body[0] {
		t.Fatal("the owner's batch body was copied on its way to the client")
	}
	if ct := w.h.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}
