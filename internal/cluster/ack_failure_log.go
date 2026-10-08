package cluster

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Forwarded ack failures.
//
// A forwarded ack, extend or nack that fails answers its client 503,
// 502 or 499 (see ackForwardFailure), and nothing else on this node
// would record it: a queue wait that runs out never becomes an RPC, so
// narad_cluster_rpc_requests_total misses it, and the client's answer
// no longer carries the owner or the error. ackFailureLog counts every
// failed record in narad_cluster_ack_forward_failures_total and writes
// at most one warning per owner per ackFailureLogInterval, summing up
// the failures since that owner's last one.

// ackFailureLogInterval is the least time between two warnings about
// one owner.
const ackFailureLogInterval = time.Minute

// Outcome labels of narad_cluster_ack_forward_failures_total: what the
// client was told.
const (
	ackFailureNotSent    = "not_sent"    // 503, nothing applied
	ackFailureUnknown    = "unknown"     // 502, may have been applied
	ackFailureClientGone = "client_gone" // 499, the client left
)

// errAckQueued marks a forwarded ack that gave up waiting for a slot to
// its owner (see ackQueueTimeout): the failure's phase is "queue".
var errAckQueued = errors.New("waited too long for a slot to the partition owner")

// ackFailureLog is the router's record of failed forwarded acks. The
// zero value counts nothing and logs through the router's logger.
type ackFailureLog struct {
	// counter is narad_cluster_ack_forward_failures_total; nil until
	// RegisterMetrics.
	counter *prometheus.CounterVec
	// now is the clock; nil means time.Now. Tests replace it.
	now func() time.Time

	mu     sync.Mutex
	owners map[string]*ackFailureTally
}

// ackFailureTally is one owner's failures since its last warning.
type ackFailureTally struct {
	logged  time.Time
	notSent int
	unknown int
}

// RegisterMetrics builds and registers the router's metrics on reg:
// narad_cluster_ack_forward_failures_total{op,outcome}. Call before
// serving.
func (rt *Router) RegisterMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "narad_cluster_ack_forward_failures_total",
		Help: "Acks, extends and nacks this node forwarded to a partition owner that failed, per record, by op and by what the client was told: not_sent (503, nothing applied), unknown (502, may have been applied) or client_gone (499).",
	}, []string{"op", "outcome"})
	reg.MustRegister(c)
	rt.ackFailures.counter = c
}

// noteAckFailure records n records of one op forwarded to addr that
// failed with err and answered status (see ackForwardFailure).
func (rt *Router) noteAckFailure(addr string, mode nodewire.AckMode, status int, err error, n int) {
	outcome := ackFailureUnknown
	switch status {
	case http.StatusServiceUnavailable:
		outcome = ackFailureNotSent
	case statusClientClosedRequest:
		outcome = ackFailureClientGone
	}
	l := &rt.ackFailures
	if l.counter != nil {
		l.counter.WithLabelValues(ackModeLabel(mode), outcome).Add(float64(n))
	}
	if outcome == ackFailureClientGone {
		return
	}
	now := time.Now
	if l.now != nil {
		now = l.now
	}
	t := now()
	l.mu.Lock()
	if l.owners == nil {
		l.owners = make(map[string]*ackFailureTally)
	}
	tally := l.owners[addr]
	if tally == nil {
		tally = &ackFailureTally{}
		l.owners[addr] = tally
	}
	if outcome == ackFailureNotSent {
		tally.notSent += n
	} else {
		tally.unknown += n
	}
	if !tally.logged.IsZero() && t.Sub(tally.logged) < ackFailureLogInterval {
		l.mu.Unlock()
		return
	}
	notSent, unknown := tally.notSent, tally.unknown
	tally.logged, tally.notSent, tally.unknown = t, 0, 0
	l.mu.Unlock()

	phase := "round_trip"
	if errors.Is(err, errAckQueued) {
		phase = "queue"
	}
	rt.logger.Warn("forwarded acks failed",
		slog.String("owner", addr),
		slog.Int("not_sent", notSent),
		slog.Int("unknown", unknown),
		slog.String("phase", phase),
		slog.Any("last_error", err),
	)
}

// ackModeLabel is the op label of an ack-shaped record.
func ackModeLabel(mode nodewire.AckMode) string {
	switch mode {
	case nodewire.AckModeExtend:
		return ackOpExtend
	case nodewire.AckModeNack:
		return ackOpNack
	default:
		return ackOpAck
	}
}

// ackFailureFor is ackForwardFailure for a record forwarded to addr,
// recorded as one failure.
func (rt *Router) ackFailureFor(ctx context.Context, addr string, mode nodewire.AckMode, err error) (int, string) {
	status, msg := ackForwardFailure(ctx, err)
	rt.noteAckFailure(addr, mode, status, err, 1)
	return status, msg
}
