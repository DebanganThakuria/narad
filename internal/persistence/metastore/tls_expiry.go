package metastore

// Raft TLS certificate expiry. A node reads its Raft certificate, key
// and CA bundle once, at startup, and nothing reloads them; one
// certificate usually serves every node, so it runs out on all of them
// at once, and from then on peers refuse every new Raft connection. The
// store therefore exports the dates (narad_raft_tls_cert_not_after_seconds),
// logs them at startup, warns ahead of expiry, and lists an expired
// certificate in the /readyz "degraded" field. Readiness itself is not
// failed for it: that would take every pod out of its Services at the
// same moment, for a cause a restart with a renewed certificate fixes.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// raftTLSRenewHint ends every expiry line: nothing reloads the files.
const raftTLSRenewHint = "Narad reads the Raft TLS files only at startup: renew the certificate (and CA) in the Secret, then restart the pods one at a time"

// tlsExpiryCheckInterval is how often a running node re-checks the
// dates against its clock, and tlsExpiredRepeat how often it repeats
// the error once a certificate has expired.
const (
	tlsExpiryCheckInterval = time.Hour
	tlsExpiredRepeat       = 24 * time.Hour
)

// The /readyz "degraded" entries (ReadinessDegraded).
const (
	// DegradedRaftTLSCertificateExpired: the node's Raft certificate
	// has expired.
	DegradedRaftTLSCertificateExpired = "raft_tls_certificate_expired"
	// DegradedRaftTLSCAExpired: every CA in the node's Raft CA bundle
	// has expired.
	DegradedRaftTLSCAExpired = "raft_tls_ca_expired"
)

var descRaftTLSNotAfter = prometheus.NewDesc("narad_raft_tls_cert_not_after_seconds",
	"Unix time at which this node's Raft TLS certificate (kind=leaf) or the earliest-expiring CA in its Raft CA bundle (kind=ca) expires. Narad reads the files only at startup, so a renewed certificate shows here after the restart that loads it.",
	[]string{"kind"}, nil)

// tlsExpiryCollector exports the dates of the Raft TLS files in use.
type tlsExpiryCollector struct{ cfg *TLSConfig }

func (c *tlsExpiryCollector) Describe(ch chan<- *prometheus.Desc) { ch <- descRaftTLSNotAfter }

func (c *tlsExpiryCollector) Collect(ch chan<- prometheus.Metric) {
	if na := c.cfg.LeafNotAfter(); !na.IsZero() {
		ch <- prometheus.MustNewConstMetric(descRaftTLSNotAfter, prometheus.GaugeValue, float64(na.Unix()), "leaf")
	}
	if na := c.cfg.CANotAfter(); !na.IsZero() {
		ch <- prometheus.MustNewConstMetric(descRaftTLSNotAfter, prometheus.GaugeValue, float64(na.Unix()), "ca")
	}
}

// degraded lists what is expired at now, for /readyz: the node
// certificate, and the CA bundle once every CA in it has expired (one
// expired CA of two is a rotation not yet finished, not an outage).
func (t *TLSConfig) degraded(now time.Time) []string {
	if t == nil {
		return nil
	}
	var out []string
	if na := t.LeafNotAfter(); !na.IsZero() && now.After(na) {
		out = append(out, DegradedRaftTLSCertificateExpired)
	}
	if len(t.CACertificates) > 0 {
		all := true
		for _, ca := range t.CACertificates {
			if !now.After(ca.NotAfter) {
				all = false
				break
			}
		}
		if all {
			out = append(out, DegradedRaftTLSCAExpired)
		}
	}
	return out
}

// ReadinessDegraded lists the conditions /readyz reports under
// "degraded" while it still answers 200: an expired Raft certificate
// (DegradedRaftTLSCertificateExpired) or CA bundle
// (DegradedRaftTLSCAExpired). nil when there is nothing to report or
// the Raft transport runs without TLS. It is deliberately not part of
// ClusterReady.
func (s *Store) ReadinessDegraded() []string {
	return s.tls.degraded(time.Now())
}

// expiryBand is how close a certificate is to its expiry, in the steps
// that get a log line.
type expiryBand int

const (
	bandFar expiryBand = iota
	band30Days
	band7Days
	band1Day
	bandExpired
)

func expiryBandAt(notAfter, now time.Time) expiryBand {
	left := notAfter.Sub(now)
	switch {
	case now.After(notAfter):
		return bandExpired
	case left <= 24*time.Hour:
		return band1Day
	case left <= 7*24*time.Hour:
		return band7Days
	case left <= 30*24*time.Hour:
		return band30Days
	default:
		return bandFar
	}
}

// tlsExpirySubject is one certificate the watch announces.
type tlsExpirySubject struct {
	kind        string // the gauge's label value
	name        string // how the log lines call it
	consequence string // what an expiry breaks
	notAfter    time.Time
	// band is the closest band already announced; lastExpired is when
	// the expired error was last logged.
	band        expiryBand
	lastExpired time.Time
}

// tlsExpiryWatch announces the Raft TLS dates in the process log: once
// at startup, then on every check that crosses 30 days, 7 days or 1 day
// before expiry, and every tlsExpiredRepeat once expired.
type tlsExpiryWatch struct {
	cfg      *TLSConfig
	log      *slog.Logger
	subjects []*tlsExpirySubject

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newTLSExpiryWatch(cfg *TLSConfig, log *slog.Logger) *tlsExpiryWatch {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	w := &tlsExpiryWatch{cfg: cfg, log: log}
	if na := cfg.LeafNotAfter(); !na.IsZero() {
		w.subjects = append(w.subjects, &tlsExpirySubject{
			kind: "leaf", name: "raft TLS certificate", notAfter: na,
			consequence: "peers refuse the Raft connections this node opens or accepts",
		})
	}
	if na := cfg.CANotAfter(); !na.IsZero() {
		w.subjects = append(w.subjects, &tlsExpirySubject{
			kind: "ca", name: "raft TLS CA certificate", notAfter: na,
			consequence: "Raft connections whose certificates chain only to it are refused",
		})
	}
	return w
}

// logStart logs the dates the node runs with, and an error when the
// node certificate is not valid yet at now.
func (w *tlsExpiryWatch) logStart(now time.Time) {
	attrs := []any{"not_before", w.cfg.LeafNotBefore(), "not_after", w.cfg.LeafNotAfter()}
	if na := w.cfg.CANotAfter(); !na.IsZero() {
		attrs = append(attrs, "ca_not_after", na, "ca_certificates", len(w.cfg.CACertificates))
	}
	w.log.Info("raft TLS certificate in use", attrs...)
	if nb := w.cfg.LeafNotBefore(); !nb.IsZero() && now.Before(nb) {
		w.log.Error("raft TLS certificate is not valid yet; peers refuse the Raft connections this node opens or accepts until its not_before has passed on their clocks. Check this node's clock and the issuer's",
			"kind", "leaf", "not_before", nb, "valid_in", nb.Sub(now).Round(time.Second))
	}
}

// check logs every subject that reached a closer band at now than any
// announced before, and repeats an expired one every tlsExpiredRepeat.
func (w *tlsExpiryWatch) check(now time.Time) {
	for _, sub := range w.subjects {
		band := expiryBandAt(sub.notAfter, now)
		switch {
		case band == bandExpired:
			if sub.band == bandExpired && now.Sub(sub.lastExpired) < tlsExpiredRepeat {
				continue
			}
			sub.band, sub.lastExpired = band, now
			w.log.Error(sub.name+" has expired; "+sub.consequence+" until it is renewed. "+raftTLSRenewHint,
				"kind", sub.kind, "not_after", sub.notAfter, "expired_for", now.Sub(sub.notAfter).Round(time.Minute))
		case band > sub.band:
			sub.band = band
			level, within := slog.LevelWarn, "30 days"
			switch band {
			case band7Days:
				within = "7 days"
			case band1Day:
				level, within = slog.LevelError, "a day"
			}
			w.log.Log(context.Background(), level, sub.name+" expires in less than "+within+"; once it does, "+sub.consequence+". "+raftTLSRenewHint,
				"kind", sub.kind, "not_after", sub.notAfter, "expires_in", sub.notAfter.Sub(now).Round(time.Minute))
		}
	}
}

// start logs the dates and checks them once now, then every interval
// on a goroutine until close.
func (w *tlsExpiryWatch) start(interval time.Duration, clock func() time.Time) {
	w.logStart(clock())
	w.check(clock())
	w.stop, w.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				w.check(clock())
			}
		}
	}()
}

// close stops the goroutine start began and waits for it. Safe to call
// more than once, on a nil watch, and on one never started.
func (w *tlsExpiryWatch) close() {
	if w == nil || w.stop == nil {
		return
	}
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}

// startTLSExpiryWatch starts the watch when the Raft transport runs TLS.
func (s *Store) startTLSExpiryWatch() {
	if s.tls == nil {
		return
	}
	s.tlsWatch = newTLSExpiryWatch(s.tls, s.log)
	s.tlsWatch.start(tlsExpiryCheckInterval, time.Now)
}
