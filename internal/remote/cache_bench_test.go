package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/security/remotecred"
)

// failingOpener fails the benchmark or test that runs it: after the
// cache's first build, nothing may decrypt.
type failingOpener struct {
	tb    testing.TB
	inner Opener
}

func (o *failingOpener) Open(domremote.Envelope, remotecred.AssociatedData) ([]byte, error) {
	o.tb.Fatal("the send path decrypted a credential")
	return nil, nil
}

func (o *failingOpener) Fingerprint(kv, id string, pw []byte) (string, error) {
	return o.inner.Fingerprint(kv, id, pw)
}
func (o *failingOpener) KeyClass(kv string) string { return o.inner.KeyClass(kv) }

// sendPathFixture is one warmed cache whose opener has been swapped for
// a failing one, plus what the rejected alternative needs.
type sendPathFixture struct {
	cache *Cache
	ring  *remotecred.Keyring
	rec   domremote.Record
	body  []byte
}

func newSendPathFixture(tb testing.TB) *sendPathFixture {
	tb.Helper()
	salt := randomBytes(tb, 32)
	secret := base64.StdEncoding.EncodeToString(randomBytes(tb, 32))
	ring, _ := remotecred.NewKeyring(secret, "", salt)
	reg := newFakeRegistry(salt)
	rec := sealer{t: tb, ring: ring}.record("b", "https://narad-b.example", "repl-from-a-7f3k9q", "password-one-0123456789ab", "", 1)
	reg.put(rec)
	f := &sendPathFixture{ring: ring, rec: rec, body: bytes.Repeat([]byte("x"), 960<<10)}
	var opener Opener = ring
	f.cache = NewCache(CacheConfig{
		Registry: reg, Posture: Posture{SecurityEnabled: true},
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Opener: func([]byte) (Opener, error) { return opener, nil },
	})
	if _, err := f.cache.Get("b"); err != nil {
		tb.Fatalf("warm-up: %v", err)
	}
	// After warm-up any decrypt fails the run.
	opener = &failingOpener{tb: tb, inner: ring}
	f.cache.opener = opener
	return f
}

// The lookup and the header assignment allocate nothing.
func TestSendPathLookupAndHeaderAllocateNothing(t *testing.T) {
	f := newSendPathFixture(t)
	req, _ := http.NewRequest(http.MethodPost, "https://narad-b.example/v1/topics/orders/produce/batch", nil)
	h := req.Header
	h["Authorization"] = nil
	allocs := testing.AllocsPerRun(1000, func() {
		e, err := f.cache.Get("b")
		if err != nil {
			t.Fatal(err)
		}
		h["Authorization"] = e.authz
	})
	if allocs != 0 {
		t.Fatalf("lookup and header assignment: %v allocs per send, want 0", allocs)
	}
}

// BenchmarkRemoteSendPath is the send path's credential cost per chunk:
// the cache lookup, the header assignment and the request build, with an
// opener that fails the benchmark if anything decrypts.
func BenchmarkRemoteSendPath(b *testing.B) {
	f := newSendPathFixture(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		e, err := f.cache.Get("b")
		if err != nil {
			b.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.base+"/v1/topics/orders/produce/batch", bytes.NewReader(f.body))
		if err != nil {
			b.Fatal(err)
		}
		req.Header["Authorization"] = e.authz
	}
}

// BenchmarkRemoteSendPathDecryptEachChunk is the rejected alternative,
// on record beside it: open the stored credential and build the header
// for every chunk.
func BenchmarkRemoteSendPathDecryptEachChunk(b *testing.B) {
	f := newSendPathFixture(b)
	tuple, _ := domremote.TupleOf(f.rec)
	ad := remotecred.ADFor(tuple)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		plain, err := f.ring.Open(f.rec.Credential, ad)
		if err != nil {
			b.Fatal(err)
		}
		authz := basicHeader(f.rec.Username, plain)
		clear(plain)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://narad-b.example/v1/topics/orders/produce/batch", bytes.NewReader(f.body))
		if err != nil {
			b.Fatal(err)
		}
		req.Header["Authorization"] = authz
	}
}

// BenchmarkRemoteLookupAndHeader is the cache's share alone: the lookup
// and the header assignment (0 allocations).
func BenchmarkRemoteLookupAndHeader(b *testing.B) {
	f := newSendPathFixture(b)
	req, _ := http.NewRequest(http.MethodPost, "https://narad-b.example/v1/topics/orders/produce/batch", nil)
	h := req.Header
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		e, _ := f.cache.Get("b")
		h["Authorization"] = e.authz
	}
}

// BenchmarkRemoteDecryptAndHeader is the rejected alternative's share
// alone: one open and one header build per chunk.
func BenchmarkRemoteDecryptAndHeader(b *testing.B) {
	f := newSendPathFixture(b)
	tuple, _ := domremote.TupleOf(f.rec)
	ad := remotecred.ADFor(tuple)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		plain, err := f.ring.Open(f.rec.Credential, ad)
		if err != nil {
			b.Fatal(err)
		}
		_ = basicHeader(f.rec.Username, plain)
		clear(plain)
	}
}
