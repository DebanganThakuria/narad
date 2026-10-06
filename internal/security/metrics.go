package security

import "github.com/prometheus/client_golang/prometheus"

// Collector exports the authenticator's verification queue:
//
//	narad_auth_verify_queued  admitted bcrypt verifications not yet
//	                          finished (waiting for a slot or running),
//	                          including work whose callers already left
//
// A sustained non-zero value alongside a rising 401/429 rate is a
// failed-login flood. Register it once on the process registry.
func (a *Authenticator) Collector() prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "narad_auth_verify_queued",
		Help: "Admitted bcrypt password verifications not yet finished (waiting for a slot or running), including work whose callers already disconnected.",
	}, func() float64 { return float64(a.queued.Load()) })
}
