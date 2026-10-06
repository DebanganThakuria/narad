package cluster

import "time"

// Pruning the token keeper's demand map.
//
// demandFor adds a topic to the map the first time a consumer parks on
// it, and nothing removed it: every topic that ever had a parked
// consumer here, deleted ones included, stayed for the life of the
// process, and every keeper pass (twice a second) looked each one's
// owners up in the local replica. 10k deleted topics cost 20k replica
// reads a second.
//
// The keeper now drops a topic's demand once nobody is parked on it and
// no owner holds a live token from here (pruneIdle), and skips the owner
// lookup for a topic nobody is parked on. Keeping a demand until its
// tokens lapse keeps what it is for: a consumer parking while a token it
// can share is still live sends nothing (registerShareWindow). A deleted
// topic's demand goes the same way, within one wait budget of its last
// consumer leaving.
//
// A pruned demand is marked dead under its own lock and the map's, so a
// caller that looked it up just before the prune sees the mark when it
// takes the demand's lock and looks the topic up again (lockDemand): a
// consumer is never queued on, nor a token stamped in, a demand the
// keeper can no longer see.

// lockDemand returns the topic's live demand, creating it if needed,
// with its mu held.
func (q *tokenRequester) lockDemand(topicName string) *topicDemand {
	for {
		d := q.demandFor(topicName)
		d.mu.Lock()
		if !d.dead {
			return d
		}
		d.mu.Unlock()
	}
}

// lockExistingDemand returns the topic's live demand with its mu held,
// or nil when the topic has none. It never creates one.
func (q *tokenRequester) lockExistingDemand(topicName string) *topicDemand {
	for {
		q.mu.RLock()
		d, ok := q.topics[topicName]
		q.mu.RUnlock()
		if !ok {
			return nil
		}
		d.mu.Lock()
		if !d.dead {
			return d
		}
		d.mu.Unlock()
	}
}

// idleLocked reports whether nothing here needs the demand any more: no
// consumer is parked on it and no owner holds a token from this node
// that has not lapsed. Must hold mu.
func (d *topicDemand) idleLocked(now time.Time) bool {
	if len(d.waiters) > 0 {
		return false
	}
	for _, tok := range d.owners {
		if tok.expiresAt.After(now) {
			return false
		}
	}
	return true
}

// pruneIdle drops every idle demand (idleLocked) from the map and
// returns how many it dropped. The candidates are found under the map's
// read lock, so parking consumers are held up only while the few found
// are removed under the write lock, each re-checked there.
func (q *tokenRequester) pruneIdle(now time.Time) int {
	var idle []string
	q.mu.RLock()
	for name, d := range q.topics {
		d.mu.Lock()
		if d.idleLocked(now) {
			idle = append(idle, name)
		}
		d.mu.Unlock()
	}
	q.mu.RUnlock()
	if len(idle) == 0 {
		return 0
	}
	pruned := 0
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, name := range idle {
		d, ok := q.topics[name]
		if !ok {
			continue
		}
		d.mu.Lock()
		if d.idleLocked(now) {
			d.dead = true
			delete(q.topics, name)
			pruned++
		}
		d.mu.Unlock()
	}
	return pruned
}

// enqueue adds w to the topic's queue of parked consumers.
func (q *tokenRequester) enqueue(topicName string, w *localWaiter) {
	d := q.lockDemand(topicName)
	d.waiters = append(d.waiters, w)
	w.demand.Store(d)
	d.mu.Unlock()
}
