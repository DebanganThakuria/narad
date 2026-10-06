package schema

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Rejection reasons of narad_schema_rejections_total. The payload ones
// count produces refused by validation; the definition ones count
// schemas refused at registration by the cost checks.
const (
	rejectDepth             = "depth"              // payload nested deeper than MaxPayloadDepth
	rejectMalformed         = "malformed"          // payload not valid UTF-8 or not one JSON text
	rejectInvalid           = "invalid"            // payload does not match the schema
	rejectBusy              = "busy"               // no validation slot within the wait
	rejectCanceled          = "canceled"           // the request ended while waiting for a slot
	rejectDefinitionPaths   = "definition_paths"   // a subschema reached through too many validation paths
	rejectDefinitionPattern = "definition_pattern" // a pattern too costly to match
)

var rejectReasons = []string{
	rejectDepth, rejectMalformed, rejectInvalid, rejectBusy, rejectCanceled,
	rejectDefinitionPaths, rejectDefinitionPattern,
}

type validationMetricSet struct {
	seconds    prometheus.Histogram
	rejections map[string]prometheus.Counter
}

var (
	validationMetrics   atomic.Pointer[validationMetricSet]
	validationsInFlight atomic.Int64
)

// RegisterMetrics registers the schema validation metrics on reg and
// starts recording them; a nil reg leaves them off. It panics if reg
// already holds them, like every other collector set in the process.
//
//   - narad_schema_validation_seconds: time of each validation that ran
//     under the node's validation limit (payloads above 16 KiB, and any
//     payload on a schema the cost analysis flagged), including the
//     decode and excluding the wait for a slot. Small payloads on
//     ordinary schemas are not timed, so the hot path pays no shared
//     atomics; they are bounded by construction.
//   - narad_schema_validations_in_flight: validations running under the
//     limit now. It sits at GOMAXPROCS while the limit is saturated.
//   - narad_schema_rejections_total{reason}: payloads and schema
//     definitions refused, by reason (depth, malformed, invalid, busy,
//     canceled, definition_paths, definition_pattern).
func RegisterMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	m := &validationMetricSet{
		seconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "narad_schema_validation_seconds",
			Help:    "Duration of schema validations that ran under the node's validation limit (payloads above 16 KiB or schemas flagged costly), decode included, wait excluded.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}),
		rejections: make(map[string]prometheus.Counter, len(rejectReasons)),
	}
	rejections := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "narad_schema_rejections_total",
		Help: "Payloads refused by schema validation and schema definitions refused by the registration cost checks, by reason.",
	}, []string{"reason"})
	for _, r := range rejectReasons {
		m.rejections[r] = rejections.WithLabelValues(r)
	}
	inFlight := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "narad_schema_validations_in_flight",
		Help: "Schema validations running under the node's validation limit.",
	}, func() float64 { return float64(validationsInFlight.Load()) })
	reg.MustRegister(m.seconds, inFlight, rejections)
	validationMetrics.Store(m)
}

func countRejection(reason string) {
	if m := validationMetrics.Load(); m != nil {
		m.rejections[reason].Inc()
	}
}

func observeValidation(d time.Duration) {
	if m := validationMetrics.Load(); m != nil {
		m.seconds.Observe(d.Seconds())
	}
}
