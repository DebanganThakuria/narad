package schema

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestValidationMetrics checks the three metric families: every
// rejection reason rises by one for its trigger, the histogram samples
// only validations that ran under the limit, and the in-flight gauge
// reads 1 while a slow one runs. The metrics are process-global, so the
// test restores the unregistered state when it ends.
func TestValidationMetrics(t *testing.T) {
	t.Cleanup(func() { validationMetrics.Store(nil) })
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	RegisterMetrics(nil) // off: no effect
	m := validationMetrics.Load()
	count := func(reason string) float64 { return testutil.ToFloat64(m.rejections[reason]) }
	before := map[string]float64{}
	for _, r := range rejectReasons {
		before[r] = count(r)
	}

	ctx := context.Background()
	r := NewJSONSchema()
	r.SetValidationLimit(1, 20*time.Millisecond)
	if err := r.Load(ctx, "t", 1, []byte(`{"type":"array","items":{"type":"integer"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(ctx, "t", largeArray()); err != nil {
		t.Fatal(err)
	}
	_ = r.Validate(ctx, "t", []byte(strings.Repeat("[", 300)+strings.Repeat("]", 300)))
	_ = r.Validate(ctx, "t", []byte(`["x"]`))
	_ = r.Validate(ctx, "t", []byte(`[1,`))
	if err := r.limiter.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	_ = r.Validate(ctx, "t", largeArray())
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_ = r.Validate(cancelled, "t", largeArray())
	r.limiter.release()
	_ = r.ValidateDefinition(ctx, "t", []byte(dagSchema(10, "anyOf")))
	_ = r.ValidateDefinition(ctx, "t", []byte(`{"pattern":"a.{100}b"}`))

	for _, reason := range rejectReasons {
		if got := count(reason) - before[reason]; got != 1 {
			t.Errorf("narad_schema_rejections_total{reason=%q} rose by %v, want 1", reason, got)
		}
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, f := range families {
		found[f.GetName()] = true
		if f.GetName() == "narad_schema_validation_seconds" {
			if n := f.GetMetric()[0].GetHistogram().GetSampleCount(); n != 1 {
				t.Errorf("validation histogram holds %d samples, want 1 (only the large payload that ran)", n)
			}
		}
	}
	for _, name := range []string{"narad_schema_validation_seconds", "narad_schema_validations_in_flight", "narad_schema_rejections_total"} {
		if !found[name] {
			t.Errorf("metric %s not registered", name)
		}
	}

	// The in-flight gauge counts a validation while it runs: a schema
	// persisted before the path count, slow on purpose.
	if err := r.Load(ctx, "slow", 1, []byte(dagSchema(18, "allOf"))); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Validate(ctx, "slow", []byte(`"x"`)) }()
	gauge := func() float64 {
		families, _ := reg.Gather()
		for _, f := range families {
			if f.GetName() == "narad_schema_validations_in_flight" {
				return f.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return -1
	}
	saw := false
	var slowErr error
	for finished := false; !finished; {
		select {
		case slowErr = <-done:
			finished = true
		case <-time.After(time.Millisecond):
			saw = saw || gauge() == 1
		}
	}
	if slowErr != nil {
		t.Fatal(slowErr)
	}
	if !saw {
		t.Error("in-flight gauge never read 1 while a validation ran")
	}
	if g := gauge(); g != 0 {
		t.Errorf("in-flight gauge reads %v after every validation returned", g)
	}
}
