package main

import (
	"errors"
	"log/slog"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Member heartbeat visibility. The leader marks a member dead after 30 s
// without a heartbeat, and a node cut off from the leader's node RPC (a
// blocked port, a mismatched cluster secret) used to say so only at
// debug level. These are the thresholds for saying it at warning level.
const (
	// heartbeatWarnAfter is the consecutive failure count at which a
	// failing heartbeat is logged at warning level.
	heartbeatWarnAfter = 3
	// heartbeatWarnMinIntervals is how many heartbeat intervals a
	// failing streak must also have lasted: before the first
	// registration the loop retries every memberRegisterRetryInterval
	// while no leader exists yet, and a few of those are a normal boot.
	heartbeatWarnMinIntervals = 2
	// heartbeatWarnEvery spaces the repeated warnings of one streak.
	heartbeatWarnEvery = time.Minute
)

// heartbeatHealth tracks one heartbeater's failure streak, logs it and
// exports it (narad_member_heartbeat_failures,
// narad_member_heartbeat_last_success_timestamp_seconds). Not safe for
// concurrent use: the heartbeat loop is its only caller.
type heartbeatHealth struct {
	log      *slog.Logger
	metrics  *metrics.Metrics // nil records nothing
	member   string
	interval time.Duration
	now      func() time.Time

	failures    int
	streakStart time.Time
	lastWarn    time.Time // zero while this streak has not warned
	removed     bool      // errMemberRemoved has been logged
}

func newHeartbeatHealth(log *slog.Logger, m *metrics.Metrics, member string, interval time.Duration, now func() time.Time) *heartbeatHealth {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if now == nil {
		now = time.Now
	}
	return &heartbeatHealth{log: log, metrics: m, member: member, interval: interval, now: now}
}

// observe records one heartbeat attempt's outcome. Every failure is
// logged at debug level as before. A streak of heartbeatWarnAfter
// failures that has lasted heartbeatWarnMinIntervals intervals is
// logged at warning level, then at most once per heartbeatWarnEvery,
// and the success that ends a warned streak at info level. A removed
// member is logged at error level once: it can never register again.
func (h *heartbeatHealth) observe(err error) {
	now := h.now()
	if err == nil {
		if !h.lastWarn.IsZero() {
			h.log.Info("member heartbeat recovered", "member", h.member,
				"failures", h.failures, "failing_for", now.Sub(h.streakStart).Round(time.Millisecond))
		}
		h.failures, h.streakStart, h.lastWarn, h.removed = 0, time.Time{}, time.Time{}, false
		h.metrics.RecordMemberHeartbeat(0, now)
		return
	}

	if h.failures == 0 {
		h.streakStart = now
	}
	h.failures++
	h.metrics.RecordMemberHeartbeat(h.failures, time.Time{})
	h.log.Debug("member heartbeat failed", "member", h.member, "err", err)

	if errors.Is(err, errMemberRemoved) {
		if !h.removed {
			h.removed = true
			h.log.Error("member heartbeat refused: this node was decommissioned and removed from the cluster, and it can never register again. Scale it away, or delete its volume to rejoin as a new node",
				"member", h.member, "err", err)
		}
		return
	}
	failingFor := now.Sub(h.streakStart)
	if h.failures < heartbeatWarnAfter || failingFor < heartbeatWarnMinIntervals*h.interval {
		return
	}
	if !h.lastWarn.IsZero() && now.Sub(h.lastWarn) < heartbeatWarnEvery {
		return
	}
	h.lastWarn = now
	h.log.Warn("member heartbeat failing; the leader marks a member dead after 30s without one",
		"member", h.member, "failures", h.failures, "since", h.streakStart,
		"failing_for", failingFor.Round(time.Millisecond), "err", err)
}
