package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Endpoint returns an http.Handler suitable for mounting at /metrics.
// It serves only collectors registered with reg — by passing the same
// Registerer used by New, you guarantee Narad's metrics are exported
// in isolation from any other process-global state.
func Endpoint(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		// Errors during scrape go to the process logger via promhttp's
		// default behaviour; we don't want a single broken collector
		// to fail the whole scrape.
		ErrorHandling: promhttp.ContinueOnError,
	})
}
