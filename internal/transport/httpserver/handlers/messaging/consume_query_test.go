package messaging

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestConsumeQueryFromRawQueryMatchesURLValues is the differential
// guard for the raw walk: for every input, each of the four parameters
// must equal what url.Values.Get returned from r.URL.Query() before the
// walk replaced it, including the odd corners (first occurrence wins,
// malformed escapes dropped, semicolons dropped, bare keys).
func TestConsumeQueryFromRawQueryMatchesURLValues(t *testing.T) {
	inputs := []string{
		"",
		"partition=1&offset=4&wait=5s&local_only=1",
		"partition=1&partition=2",
		"offset=7&offset=8&partition=3&partition=4",
		"partition=%ZZ",
		"partition=%ZZ&partition=2",
		"offset=%ZZ&offset=9",
		"wait=%zz&wait=2s",
		"partition%ZZ=1&partition=5",
		"partition=1;offset=2",
		"partition=1;partition=2&partition=3",
		"partition",
		"partition=&partition=6",
		"local_only",
		"local_only=1&local_only=0",
		"local_only=%31",
		"wait=1%2B0s",
		"wait=1+0s",
		"&&partition=2&&",
		"other=1&partition=2&extra=%ZZ",
		"PARTITION=1&partition=2",
		"p%61rtition=9",
		"offset=+5",
		"wait=%2D2s",
	}
	for _, raw := range inputs {
		want, _ := url.ParseQuery(raw) // r.URL.Query() discards this error too
		got := consumeQueryFromRawQuery(raw)
		check := func(name, got, want string) {
			if got != want {
				t.Errorf("raw %q: %s = %q, want %q (url.Values.Get)", raw, name, got, want)
			}
		}
		check("partition", got.partition, want.Get("partition"))
		check("offset", got.offset, want.Get("offset"))
		check("wait", got.wait, want.Get("wait"))
		check("local_only", got.localOnly, want.Get("local_only"))
	}
}

// TestParseConsumeQueryFirstOccurrenceWins pins semantic (a): with a
// repeated parameter the handler uses the first value, as
// url.Values.Get did.
func TestParseConsumeQueryFirstOccurrenceWins(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?partition=1&partition=2&offset=10&offset=20", nil)
	got, _, ok := parseConsumeQuery(s, res, req)
	if !ok {
		t.Fatalf("parseConsumeQuery() ok = false: %s", res.Body.String())
	}
	if got.Partition == nil || *got.Partition != 1 {
		t.Fatalf("partition = %+v, want first occurrence 1", got.Partition)
	}
	if got.Offset == nil || *got.Offset != 10 {
		t.Fatalf("offset = %+v, want first occurrence 10", got.Offset)
	}
}

// TestParseConsumeQueryDropsMalformedEscape pins semantic (b): a pair
// with a malformed percent-escape is silently dropped, so the parameter
// reads as unset rather than producing a 400.
func TestParseConsumeQueryDropsMalformedEscape(t *testing.T) {
	s := newTestSet(&fakeBroker{}, nil)

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?partition=%ZZ&offset=%ZZ&wait=%ZZ", nil)
	got, _, ok := parseConsumeQuery(s, res, req)
	if !ok {
		t.Fatalf("parseConsumeQuery() ok = false (status %d): malformed escape must not be a 400", res.Code)
	}
	if got.Partition != nil || got.Offset != nil || got.Wait != 0 {
		t.Fatalf("parseConsumeQuery() = %+v, want all parameters unset", got)
	}

	// A malformed first occurrence is dropped, so the next valid one is
	// the "first" that wins.
	res = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/?partition=%ZZ&partition=2", nil)
	got, _, ok = parseConsumeQuery(s, res, req)
	if !ok || got.Partition == nil || *got.Partition != 2 {
		t.Fatalf("parseConsumeQuery() = %+v, ok %v; want partition 2", got, ok)
	}
}

// BenchmarkConsumeQueryParse measures the per-request query walk on the
// consume hot path for the common shapes.
func BenchmarkConsumeQueryParse(b *testing.B) {
	queries := map[string]string{
		"queue_pull":   "wait=30s",
		"replay":       "partition=3&offset=128&wait=1s",
		"probe":        "local_only=1",
		"unrelated":    "client=abc&wait=5s&trace=1",
		"escaped_wait": "wait=1%2B0s",
	}
	for name, raw := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q := consumeQueryFromRawQuery(raw)
				if q.partition == "" && q.offset == "" && q.wait == "" && q.localOnly == "" {
					b.Fatal("unexpected empty parse result")
				}
			}
		})
	}
	b.Run("url_values_baseline", func(b *testing.B) {
		b.ReportAllocs()
		raw := "partition=3&offset=128&wait=1s"
		for b.Loop() {
			q, _ := url.ParseQuery(raw)
			if q.Get("partition") == "" {
				b.Fatal("unexpected empty parse result")
			}
		}
	})
}
