package messaging

// Produce pause for a rebalance handoff. When a partition is about to
// hand off to a new owner, this node briefly stops accepting produce
// for it so no record lands after the destination captured the final
// tail. Paused produce reroutes to a live partition (AP) — the same
// path as a dead owner. The pause carries a TTL so if the handoff never
// completes (the destination died), the source auto-resumes.

import (
	"strconv"
	"time"
)

func producePauseKey(topicName string, partition int) string {
	return topicName + "/" + strconv.Itoa(partition)
}

// PauseProduceForHandoff stops accepting produce for a partition this
// node owns, until ResumeProduce is called or ttl elapses (whichever
// first). Idempotent — re-pausing extends the deadline.
func (e *Engine) PauseProduceForHandoff(topicName string, partition int, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	e.pauseMu.Lock()
	if e.producePauses == nil {
		e.producePauses = make(map[string]int64)
	}
	e.producePauses[producePauseKey(topicName, partition)] = time.Now().Add(ttl).UnixNano()
	e.pauseMu.Unlock()
}

// ResumeProduce clears a handoff pause (the flip completed, or the move
// aborted).
func (e *Engine) ResumeProduce(topicName string, partition int) {
	e.pauseMu.Lock()
	delete(e.producePauses, producePauseKey(topicName, partition))
	e.pauseMu.Unlock()
}

// isProducePaused reports whether the partition is currently paused for
// handoff, lazily expiring a lapsed pause (auto-resume) so a dead
// destination can never wedge produce forever.
func (e *Engine) isProducePaused(topicName string, partition int) bool {
	key := producePauseKey(topicName, partition)
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	exp, ok := e.producePauses[key]
	if !ok {
		return false
	}
	if time.Now().UnixNano() >= exp {
		delete(e.producePauses, key)
		return false
	}
	return true
}
