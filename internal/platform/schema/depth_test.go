package schema

import (
	"context"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
)

// heapUse is what one validation cost in memory and time.
type heapUse struct {
	retainedMiB float64 // still reachable after a collection, the result included
	allocMiB    float64 // allocated in total
	peakMiB     float64 // most heap in use above the starting point while it ran
	took        time.Duration
}

// measureHeap runs validate and reports what its result keeps alive
// after a collection, what it allocated, its heap peak and how long it
// took. The error is kept reachable until the second reading, so an
// error tree that holds the memory counts as retained.
func measureHeap(validate func() error) (heapUse, error) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()
	stop, done := make(chan struct{}), make(chan uint64)
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		var peak uint64
		for {
			metrics.Read(s)
			peak = max(peak, s[0].Value.Uint64())
			select {
			case <-stop:
				done <- peak
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	start := time.Now()
	err := validate()
	var use heapUse
	use.took = time.Since(start)
	close(stop)
	peak := <-done
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(err)
	if after.HeapAlloc > before.HeapAlloc {
		use.retainedMiB = float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
	}
	if peak > base {
		use.peakMiB = float64(peak-base) / (1 << 20)
	}
	use.allocMiB = float64(after.TotalAlloc-before.TotalAlloc) / (1 << 20)
	return use, err
}

func nested(open, leaf, closing string, depth int) string {
	return strings.Repeat(open, depth) + leaf + strings.Repeat(closing, depth)
}

// TestDeepPayloadIsRefusedBeforeValidation is the audit's repro
// (verify-schemas-0). Each schema is recursive, registrable and
// ordinary (arrays of arrays, object trees, a JSON-value union). A
// 20 KB payload nested 9990 deep made the validator build an error at
// every level, each cloning the whole instance path: about 800 MiB
// retained for the array shape and 2.4 GiB for the anyOf shape, and
// close to 800 MiB of garbage even for a valid payload of the anyOf
// shape. A payload deeper than MaxPayloadDepth is refused before
// validation, so it costs almost nothing.
func TestDeepPayloadIsRefusedBeforeValidation(t *testing.T) {
	const depth = 9990
	cases := []struct {
		name, schema, payload string
	}{
		{"array of arrays, invalid leaf", `{"type":"array","items":{"$ref":"#"}}`, nested("[", "1", "]", depth)},
		{"object tree, invalid leaf", `{"type":"object","additionalProperties":{"$ref":"#"}}`, nested(`{"a":`, "1", "}", depth)},
		{"anyOf union, invalid leaf", `{"anyOf":[{"type":"string"},{"type":"array","items":{"$ref":"#"}}]}`, nested("[", "1", "]", depth)},
		{"anyOf union, valid leaf", `{"anyOf":[{"type":"string"},{"type":"array","items":{"$ref":"#"}}]}`, nested("[", `"x"`, "]", depth)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewJSONSchema()
			if err := r.ValidateDefinition(context.Background(), "t", []byte(tc.schema)); err != nil {
				t.Fatalf("ordinary recursive schema refused at registration: %v", err)
			}
			if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
				t.Fatal(err)
			}
			use, err := measureHeap(func() error {
				return r.Validate(context.Background(), "t", []byte(tc.payload))
			})
			t.Logf("%d-byte payload nested %d deep: took %v, allocated %.1f MiB, retained %.1f MiB, err=%.120v",
				len(tc.payload), depth, use.took, use.allocMiB, use.retainedMiB, err)
			if err == nil || !strings.Contains(err.Error(), "deeper than 256") {
				t.Errorf("payload nested %d deep: err = %.200v, want a refusal naming the 256-level limit", depth, err)
			}
			if use.retainedMiB > 4 {
				t.Errorf("the refusal retains %.1f MiB; a payload refused before validation must retain almost nothing", use.retainedMiB)
			}
			if use.allocMiB > 16 {
				t.Errorf("the refusal allocated %.1f MiB; the depth check must stop the decode early", use.allocMiB)
			}
		})
	}
}

// TestPayloadDepthBoundary pins the limit: 256 levels of nesting
// validate, 257 are refused, on both decode paths, and brackets inside
// strings do not count. The decoder alone used to allow 10000.
func TestPayloadDepthBoundary(t *testing.T) {
	r := NewJSONSchema()
	if err := r.Load(context.Background(), "t", 1, []byte(`{"type":["array","object","string"],"items":{"$ref":"#"},"additionalProperties":{"$ref":"#"}}`)); err != nil {
		t.Fatal(err)
	}
	accepted := []string{
		nested("[", "", "]", 256),
		nested(`{"a":`, "{}", "}", 255),
		nested("[", `"`+strings.Repeat("[", 400)+`"`, "]", 255),
	}
	for _, p := range accepted {
		if err := r.Validate(context.Background(), "t", []byte(p)); err != nil {
			t.Errorf("payload nested at most 256 deep refused: %v", err)
		}
		if _, err := decodePayloadSlow([]byte(p)); err != nil {
			t.Errorf("reference decode refused a payload nested at most 256 deep: %v", err)
		}
	}
	refused := []string{
		nested("[", "", "]", 257),
		nested(`{"a":`, "{}", "}", 256),
		nested("[", `{"a":[]}`, "]", 255),
		// Too deep and malformed after the limit: still the depth.
		nested("[", "", "]", 257) + "x",
	}
	for _, p := range refused {
		err := r.Validate(context.Background(), "t", []byte(p))
		if err == nil || !strings.Contains(err.Error(), "deeper than 256") {
			t.Errorf("payload nested 257 deep: err = %v, want the depth refusal", err)
		}
		if _, err := decodePayloadSlow([]byte(p)); err == nil || !strings.Contains(err.Error(), "deeper than 256") {
			t.Errorf("reference decode of a payload nested 257 deep: err = %v, want the depth refusal", err)
		}
	}
}

// TestRejectedPayloadMemoryStaysBounded covers what the depth cap
// leaves possible: a whole 1 MiB body of 256-deep nests with failing
// leaves, the "comb" (one 255-deep spine holding every failing leaf)
// and a valid body of the anyOf shape. Before the bound the first shape
// retained about 1.2 GiB (every leaf's error chain cloning a 256-token
// path at every level) and the comb about 2 GiB. A payload whose
// detailed report would be that large is checked without building one
// and gets a summary.
func TestRejectedPayloadMemoryStaysBounded(t *testing.T) {
	skipBounds(t)
	const maxBody = 1 << 20
	nest := nested("[", "1", "]", 255)
	nests := "[" + strings.TrimSuffix(strings.Repeat(nest+",", (maxBody-2)/(len(nest)+1)), ",") + "]"
	combLeaves := (maxBody - 600) / 2
	comb := nested("[", strings.TrimSuffix(strings.Repeat("1,", combLeaves), ","), "]", 255)
	validNest := nested("[", `"x"`, "]", 255)
	validNests := "[" + strings.TrimSuffix(strings.Repeat(validNest+",", (maxBody-2)/(len(validNest)+1)), ",") + "]"
	cases := []struct {
		name, schema, payload string
		valid                 bool
	}{
		{"1 MiB of 256-deep nests, failing leaves", `{"type":"array","items":{"$ref":"#"}}`, nests, false},
		{"1 MiB comb, failing leaves at depth 256", `{"type":"array","items":{"$ref":"#"}}`, comb, false},
		{"1 MiB of 256-deep anyOf nests, failing leaves", `{"anyOf":[{"type":"string"},{"type":"array","items":{"$ref":"#"}}]}`, nests, false},
		{"1 MiB of 256-deep anyOf nests, valid leaves", `{"anyOf":[{"type":"string"},{"type":"array","items":{"$ref":"#"}}]}`, validNests, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.payload) > maxBody {
				t.Fatalf("payload is %d bytes", len(tc.payload))
			}
			r := NewJSONSchema()
			if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
				t.Fatal(err)
			}
			use, err := measureHeap(func() error {
				return r.Validate(context.Background(), "t", []byte(tc.payload))
			})
			t.Logf("%d-byte payload: took %v, allocated %.1f MiB, peak %.1f MiB, retained %.1f MiB, err=%.160v",
				len(tc.payload), use.took, use.allocMiB, use.peakMiB, use.retainedMiB, err)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v but err = %.300v", tc.valid, err)
			}
			if err != nil && len(err.Error()) > maxValidationErrorBytes+64 {
				t.Errorf("error message is %d bytes", len(err.Error()))
			}
			if use.retainedMiB > 16 {
				t.Errorf("validation retains %.1f MiB after it returned; the bound is 16 MiB", use.retainedMiB)
			}
			if use.peakMiB > 96 {
				t.Errorf("validation peaked at %.1f MiB of heap; the bound is 96 MiB", use.peakMiB)
			}
			if use.took > slowBug {
				t.Errorf("validation took %v", use.took)
			}
		})
	}
}
