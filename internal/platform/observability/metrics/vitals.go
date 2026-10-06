package metrics

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// The vitals loop: the few gauges alerts page on, each read through its
// own probe so that one source that hangs or panics cannot freeze or
// crash the rest.

// vitalResult is the outcome of one probed read.
type vitalResult struct {
	value    float64
	err      error
	panicked bool
}

// vitalProbe runs one source's read on its own goroutine, at most one at
// a time. A read that outlives the pass waiting for it stays in flight
// and its result is taken by a later pass; no second read of that source
// starts meanwhile, so a source that never returns costs one goroutine,
// not one per tick. Used only under Poller.vitalsMu.
type vitalProbe struct {
	// pending carries the in-flight read's result; nil when idle.
	pending chan vitalResult
	// stuck is set once a pass gave up waiting for the in-flight read,
	// and failing holds the last read error logged; both keep a lasting
	// fault to one log line per episode instead of one per pass.
	stuck   bool
	failing string
}

// start launches read unless one is already in flight. A panic in read
// is recovered and reported as that read's error.
func (pr *vitalProbe) start(read func() (float64, error)) {
	if pr.pending != nil {
		return
	}
	ch := make(chan vitalResult, 1)
	pr.pending = ch
	go func() {
		var r vitalResult
		defer func() {
			if rec := recover(); rec != nil {
				r = vitalResult{err: fmt.Errorf("panic: %v", rec), panicked: true}
			}
			ch <- r
		}()
		r.value, r.err = read()
	}()
}

// await returns the in-flight read's result, waiting until ctx is done.
// ok is false when the read is still running.
func (pr *vitalProbe) await(ctx context.Context) (vitalResult, bool) {
	if pr.pending == nil {
		return vitalResult{}, false
	}
	// A result that is already there wins over an expired deadline.
	select {
	case r := <-pr.pending:
		pr.pending = nil
		return r, true
	default:
	}
	select {
	case r := <-pr.pending:
		pr.pending = nil
		return r, true
	case <-ctx.Done():
		return vitalResult{}, false
	}
}

// vital is one vital-sign source and the gauge it feeds. name is also
// the kind counted in narad_errors_total{component="metrics"} when a
// read fails (with _timeout or _panic appended for those).
type vital struct {
	name  string
	gauge prometheus.Gauge
	read  func() (float64, error)
}

// vitals lists the wired vital-sign sources.
func (p *Poller) vitals() []vital {
	var out []vital
	if backlog := p.ingressBacklog; backlog != nil {
		out = append(out, vital{"ingress_backlog", p.metrics.IngressDispatchBacklog, func() (float64, error) {
			return float64(backlog()), nil
		}})
	}
	if healthy := p.ingressHealthy; healthy != nil {
		out = append(out, vital{"ingress_wal", p.metrics.IngressWALFailed, func() (float64, error) {
			if healthy() {
				return 0, nil
			}
			return 1, nil
		}})
	}
	if open := p.openLogs; open != nil {
		out = append(out, vital{"open_logs", p.metrics.OpenPartitionLogs, func() (float64, error) {
			return float64(open()), nil
		}})
	}
	if restarts := p.reaperRestarts; restarts != nil {
		out = append(out, vital{"reaper_restarts", p.metrics.ReaperRestarts, func() (float64, error) {
			return float64(restarts()), nil
		}})
	}
	if dir, statfs := p.dataDir, p.statfs; dir != "" && statfs != nil {
		out = append(out, vital{"data_dir_available", p.metrics.DataDirAvailableBytes, func() (float64, error) {
			n, err := statfs(dir)
			return float64(n), err
		}})
	}
	return out
}

// vitalsTick runs one vitals pass: every wired source is read on its own
// probe, and the pass waits at most vitalsDeadline for them. A source
// that answers sets its gauge. One that fails, panics or is still
// running keeps its last value, is counted in narad_errors_total and
// logged once per episode, and makes the pass incomplete, so
// narad_poller_last_success_timestamp_seconds{loop="vitals"} moves only
// when every vital sign is current.
func (p *Poller) vitalsTick(ctx context.Context) {
	p.vitalsMu.Lock()
	defer p.vitalsMu.Unlock()

	vitals := p.vitals()
	for _, v := range vitals {
		p.probe(v.name).start(v.read)
	}
	deadline := p.vitalsDeadline
	if deadline <= 0 {
		deadline = vitalsReadDeadline
	}
	wait, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	complete := true
	for _, v := range vitals {
		pr := p.probe(v.name)
		r, ok := pr.await(wait)
		switch {
		case !ok:
			complete = false
			if ctx.Err() != nil {
				continue // shutting down, not a stuck source
			}
			p.metrics.IncError("metrics", v.name+"_timeout")
			if !pr.stuck {
				pr.stuck = true
				p.logger.Warn("metrics: vital sign read did not answer in time; its gauge keeps its last value until it does",
					"source", v.name, "deadline", deadline, "data_dir", p.dataDir)
			}
		case r.err != nil:
			complete = false
			pr.stuck = false
			kind, level := v.name, slog.LevelWarn
			if r.panicked {
				kind, level = v.name+"_panic", slog.LevelError
			}
			p.metrics.IncError("metrics", kind)
			if msg := r.err.Error(); msg != pr.failing {
				pr.failing = msg
				p.logger.Log(ctx, level, "metrics: vital sign read failed; its gauge keeps its last value",
					"source", v.name, "data_dir", p.dataDir, "err", r.err)
			}
		default:
			if pr.stuck || pr.failing != "" {
				p.logger.Info("metrics: vital sign read answers again", "source", v.name)
			}
			pr.stuck, pr.failing = false, ""
			v.gauge.Set(r.value)
		}
	}
	if complete && ctx.Err() == nil {
		p.markSuccess(pollerLoopVitals)
	}
}

func (p *Poller) probe(name string) *vitalProbe {
	if p.probes == nil {
		p.probes = make(map[string]*vitalProbe)
	}
	pr := p.probes[name]
	if pr == nil {
		pr = &vitalProbe{}
		p.probes[name] = pr
	}
	return pr
}
