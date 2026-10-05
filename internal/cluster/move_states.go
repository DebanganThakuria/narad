package cluster

// What every move targeting this node is doing (MoveStates): each
// running worker records its phase, copy attempts, last error, copied
// bytes and, for a move it cannot finish on its own, why. The operator
// surface reads it, and narad_moves_blocked counts the blocked moves by
// reason.

import (
	"cmp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Move phases (MoveState.Phase).
const (
	// MovePhaseCopying: copying the partition with produce flowing on
	// the source (or about to freeze it).
	MovePhaseCopying = "copying"
	// MovePhaseFrozen: the source is frozen and the worker drains the
	// tail, verifies the copy and installs it.
	MovePhaseFrozen = "frozen"
	// MovePhaseFlipPending: the copy is installed and the ownership flip
	// is proposed or being resolved with the leader.
	MovePhaseFlipPending = "flip_pending"
	// MovePhaseWaitingForSource: the source cannot be reached through
	// its member record, or reads dead and has not been dead long enough
	// for a force-promote.
	MovePhaseWaitingForSource = "waiting_for_source"
	// MovePhaseBlocked: the move cannot finish on its own (Blocked says
	// why) until the source returns, or an operator aborts it.
	MovePhaseBlocked = "blocked"
)

// Reasons a move cannot finish on its own (MoveState.Blocked, and the
// reason label of narad_moves_blocked).
const (
	// MoveBlockedCopyUnverifiable: the staged copy failed verification
	// twice, the second time after a fresh copy, so the worker stopped
	// freezing the source; or a dead source's copy fails it.
	MoveBlockedCopyUnverifiable = "copy_unverifiable"
	// MoveBlockedSourceDeadCopyBehind: the source is dead and the copy
	// is behind its last high watermark, so it cannot be force-promoted.
	MoveBlockedSourceDeadCopyBehind = "source_dead_copy_behind"
)

// moveBlockedReasons are every MoveState.Blocked value, each exported by
// narad_moves_blocked even at 0.
var moveBlockedReasons = []string{MoveBlockedCopyUnverifiable, MoveBlockedSourceDeadCopyBehind}

// MoveState is one move this node runs as the destination. It is the
// nodewire type, so OpNodeStatus carries it as is.
type MoveState = nodewire.MoveState

// moveStatus is one worker's MoveState: written by the worker goroutine,
// read by MoveStates. A nil *moveStatus (a worker built without
// trackMove) records nothing.
type moveStatus struct {
	key moveKey
	mu  sync.Mutex
	st  MoveState
}

func (s *moveStatus) update(fn func(*MoveState)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	fn(&s.st)
	s.mu.Unlock()
}

func (s *moveStatus) snapshot() MoveState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

// setPhase records phase and clears Blocked: only setBlocked blocks.
func (s *moveStatus) setPhase(phase string) {
	s.update(func(st *MoveState) { st.Phase, st.Blocked = phase, "" })
}

// setBlocked records that the move cannot finish on its own, and why.
func (s *moveStatus) setBlocked(reason string, err error) {
	s.update(func(st *MoveState) {
		st.Phase, st.Blocked = MovePhaseBlocked, reason
		if err != nil {
			st.LastError = err.Error()
		}
	})
}

// setError records the last failure.
func (s *moveStatus) setError(err error) {
	if err == nil {
		return
	}
	s.update(func(st *MoveState) { st.LastError = err.Error() })
}

// startAttempt records a copy attempt against a live source.
func (s *moveStatus) startAttempt() {
	s.update(func(st *MoveState) {
		st.Attempts++
		st.Phase, st.Blocked = MovePhaseCopying, ""
	})
}

// setCopied records the bytes the current copy has fetched.
func (s *moveStatus) setCopied(n int64) {
	s.update(func(st *MoveState) { st.CopiedBytes = n })
}

// trackMove registers a worker's status for MoveStates; untrackMove
// drops it once the worker has exited.
func (r *MoveRunner) trackMove(topicName string, partition int, source string, started time.Time) *moveStatus {
	s := &moveStatus{
		key: moveKey{topic: topicName, partition: partition, target: r.selfID},
		st: MoveState{
			Topic: topicName, Partition: partition, Source: source, Target: r.selfID,
			StartedAt: started, Phase: MovePhaseCopying,
		},
	}
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	r.statuses[s.key] = s
	return s
}

func (r *MoveRunner) untrackMove(s *moveStatus) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	if r.statuses[s.key] == s {
		delete(r.statuses, s.key)
	}
}

// MoveStates reports every move targeting this node that has a running
// worker, ordered by topic and partition.
func (r *MoveRunner) MoveStates() []MoveState {
	r.statusMu.Lock()
	out := make([]MoveState, 0, len(r.statuses))
	for _, s := range r.statuses {
		out = append(out, s.snapshot())
	}
	r.statusMu.Unlock()
	slices.SortFunc(out, func(a, b MoveState) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		return cmp.Compare(a.Partition, b.Partition)
	})
	return out
}

// leaderBlockedMoves is the leader's own count of in-flight moves
// blocked on a dead node (controller.BlockedMoves), exported beside this
// node's worker reasons under narad_moves_blocked: one metric name can
// have one collector only.
type leaderBlockedMoves struct {
	reasons []string
	counts  func() map[string]int
}

// SetLeaderBlockedMoves adds the leader's blocked-move counts, by the
// given reasons (each exported even at 0), to narad_moves_blocked. Call
// before serving metrics.
func (r *MoveRunner) SetLeaderBlockedMoves(reasons []string, counts func() map[string]int) {
	r.leaderBlocked.Store(&leaderBlockedMoves{reasons: slices.Clone(reasons), counts: counts})
}

// movesBlockedCollector exports narad_moves_blocked{reason} at scrape
// time: this node's workers' blocked reasons from MoveStates, and the
// leader's reasons when SetLeaderBlockedMoves wired them.
type movesBlockedCollector struct {
	r    *MoveRunner
	desc *prometheus.Desc
}

func newMovesBlockedCollector(r *MoveRunner) *movesBlockedCollector {
	return &movesBlockedCollector{r: r, desc: prometheus.NewDesc(
		"narad_moves_blocked",
		"Partition moves that cannot finish on their own, by reason. On the move's destination: copy_unverifiable (the staged copy failed verification again after a fresh copy, so the source is not frozen again) or source_dead_copy_behind (the source is dead and the copy is behind its last high watermark). On the leader only: source_dead and target_dead (an in-flight move whose source or destination member is dead).",
		[]string{"reason"}, nil,
	)}
}

func (c *movesBlockedCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *movesBlockedCollector) Collect(ch chan<- prometheus.Metric) {
	counts := map[string]int{}
	for _, s := range c.r.MoveStates() {
		if s.Blocked != "" {
			counts[s.Blocked]++
		}
	}
	for _, reason := range moveBlockedReasons {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(counts[reason]), reason)
	}
	lb := c.r.leaderBlocked.Load()
	if lb == nil {
		return
	}
	leader := lb.counts()
	for _, reason := range lb.reasons {
		if slices.Contains(moveBlockedReasons, reason) {
			continue // never two series for one reason
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(leader[reason]), reason)
	}
}
