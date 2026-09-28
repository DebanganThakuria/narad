package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

// A body with a second value after the first is refused by reading one
// token of that value, not by decoding it: decoding it into an any built
// the whole trailing value, so a 1 MiB body of {} followed by
// [{"":0},...] allocated about 72 MB (73 times the body) on every
// endpoint that decodes a JSON body, topic create included, before any
// authorization check ran.
func TestWP22DecodeJSONTrailingValueIsCheap(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"name":"orders"}[`)
	for int64(b.Len()) < MaxJSONBodyBytes-16 {
		b.WriteString(`{"":0},`)
	}
	b.WriteString(`{"":0}]`)
	body := b.String()

	type req struct {
		Name string `json:"name"`
	}
	s := newTestSet(&fakeBroker{})
	res := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	var decoded req
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ok := s.DecodeJSON(res, httpReq, &decoded)
	runtime.ReadMemStats(&after)
	if ok || res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "multiple JSON values") {
		t.Fatalf("DecodeJSON = %v, %d %s; want false, 400 multiple JSON values", ok, res.Code, res.Body)
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	if limit := uint64(4 * len(body)); allocated > limit {
		t.Fatalf("a %d-byte body with a trailing value allocated %d bytes (%.0fx), want at most %d",
			len(body), allocated, float64(allocated)/float64(len(body)), limit)
	}
}
