package metastore

// The leader's view of its Raft peers' health: which peers are failing
// the heartbeats it sends, and how long this node has led. Join
// admission (join_admission.go) reads it before it promotes a staged
// non-voter, and forget (forget.go) before it removes a voter. It is
// built from one Raft observer and read through Raft's non-blocking
// accessors (State, CurrentTerm), so a stalled Raft main loop cannot
// block the join handler that consults it.

import (
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// raftObserverBuffer is the observer channel's capacity. Raft never
// blocks on a full channel (the observation is dropped), and handling
// one is a map operation, so the buffer only has to ride out a burst
// such as every peer failing a heartbeat at once.
const raftObserverBuffer = 256

// heartbeatFailureWindow is how long one failed heartbeat keeps a peer
// counted as failing. Raft reports a failed heartbeat on every failed
// attempt and a resumed one on the first success after failures, which
// clears the peer at once; the window only decides when a peer whose
// resumed observation was dropped stops counting. It has to outlast one
// failed attempt to a peer that drops packets: the transport's 10 s dial
// timeout plus Raft's retry backoff of at most half a heartbeat timeout.
const heartbeatFailureWindow = 12 * time.Second

// raftHealth is the Store's leader-side heartbeat view.
type raftHealth struct {
	r        *raft.Raft
	selfID   raft.ServerID
	obsCh    chan raft.Observation
	observer *raft.Observer
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once

	mu sync.Mutex
	// term is the Raft term the fields below describe. Any read or
	// observation in a later term starts them afresh: heartbeat health
	// is the current leader's view, and a term change means a new one.
	term uint64
	// lastFailure maps a peer to the time of the latest heartbeat this
	// node, as leader, failed to deliver to it. A resumed heartbeat or the
	// peer's removal deletes the entry.
	lastFailure map[raft.ServerID]time.Time
	// leaderSince is when this node was first seen leading in term, or
	// zero. It is read on the monotonic clock (time.Since).
	leaderSince time.Time
}

// newRaftHealth registers the observer on r and starts folding its
// observations in. close deregisters it.
func newRaftHealth(r *raft.Raft, selfID string) *raftHealth {
	h := &raftHealth{
		r:           r,
		selfID:      raft.ServerID(selfID),
		obsCh:       make(chan raft.Observation, raftObserverBuffer),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		lastFailure: map[raft.ServerID]time.Time{},
	}
	h.observer = raft.NewObserver(h.obsCh, false, raftHealthObservation)
	r.RegisterObserver(h.observer)
	go h.run()
	return h
}

// raftHealthObservation keeps only the observations the view uses.
func raftHealthObservation(o *raft.Observation) bool {
	switch o.Data.(type) {
	case raft.LeaderObservation, raft.FailedHeartbeatObservation, raft.ResumedHeartbeatObservation, raft.PeerObservation:
		return true
	default:
		return false
	}
}

func (h *raftHealth) run() {
	defer close(h.done)
	for {
		select {
		case <-h.stop:
			return
		case o := <-h.obsCh:
			h.handle(o.Data, time.Now())
		}
	}
}

// close deregisters the observer and stops the loop. Idempotent.
func (h *raftHealth) close() {
	h.once.Do(func() {
		h.r.DeregisterObserver(h.observer)
		close(h.stop)
		<-h.done
	})
}

// handle folds one observation into the view.
func (h *raftHealth) handle(data any, now time.Time) {
	term := h.r.CurrentTerm()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.syncTermLocked(term)
	switch o := data.(type) {
	case raft.LeaderObservation:
		// Raft reports only a change of leader: the failures seen so far
		// were the previous leader's view.
		clear(h.lastFailure)
		if o.LeaderID != h.selfID {
			h.leaderSince = time.Time{}
		} else if h.leaderSince.IsZero() {
			h.leaderSince = now
		}
	case raft.FailedHeartbeatObservation:
		h.lastFailure[o.PeerID] = now
	case raft.ResumedHeartbeatObservation:
		delete(h.lastFailure, o.PeerID)
	case raft.PeerObservation:
		if o.Removed {
			delete(h.lastFailure, o.Peer.ID)
		}
	}
}

// syncTermLocked starts the view afresh when term is not the one it
// describes.
func (h *raftHealth) syncTermLocked(term uint64) {
	if term == h.term {
		return
	}
	h.term = term
	clear(h.lastFailure)
	h.leaderSince = time.Time{}
}

// leaderFor reports how long this node has continuously led in the
// current term, or 0 while it does not lead. When the observation that
// it became leader has not been handled yet (or was dropped) the clock
// starts at this call, which only ever makes the node wait longer.
func (h *raftHealth) leaderFor() time.Duration {
	if h.r.State() != raft.Leader {
		return 0
	}
	term := h.r.CurrentTerm()
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.syncTermLocked(term)
	if h.leaderSince.IsZero() {
		h.leaderSince = now
	}
	return now.Sub(h.leaderSince)
}

// failingSince reports when this node, as leader, last failed to
// heartbeat id, and whether that is recent enough (heartbeatFailureWindow)
// to count the peer as failing now.
func (h *raftHealth) failingSince(id raft.ServerID) (time.Time, bool) {
	term := h.r.CurrentTerm()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.syncTermLocked(term)
	last, ok := h.lastFailure[id]
	if !ok || time.Since(last) >= heartbeatFailureWindow {
		return time.Time{}, false
	}
	return last, true
}
