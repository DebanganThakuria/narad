package messaging

// Handoff freeze for a rebalance. When a partition is about to hand off
// to a new owner, this node briefly stops both ACCEPTING and COMMITTING
// produce for it, so no record lands after the destination captured the
// final tail. Frozen produce reroutes to a live partition and frozen
// commits make the ingress dispatcher retry to the new owner (AP) — the
// same paths as a dead owner. The freeze carries a TTL so if the handoff
// never completes (the destination died), the source auto-resumes.

import (
	"context"
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

// ResumeProduce clears a handoff pause, produce and consume alike (the
// flip completed, or the move aborted).
func (e *Engine) ResumeProduce(topicName string, partition int) {
	key := producePauseKey(topicName, partition)
	e.pauseMu.Lock()
	delete(e.producePauses, key)
	delete(e.consumePauses, key)
	e.pauseMu.Unlock()
}

// PauseConsumeForHandoff stops handing out new reservations for a
// partition this node owns, until ResumeProduce is called or ttl
// elapses. Acks, nacks, and extends for leases already handed out keep
// working, so the in-flight set drains to the frontier.
func (e *Engine) PauseConsumeForHandoff(topicName string, partition int, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	e.pauseMu.Lock()
	if e.consumePauses == nil {
		e.consumePauses = make(map[string]int64)
	}
	e.consumePauses[producePauseKey(topicName, partition)] = time.Now().Add(ttl).UnixNano()
	e.pauseMu.Unlock()
}

// isConsumePaused reports whether new reservations are frozen for the
// partition, lazily expiring a lapsed pause.
func (e *Engine) isConsumePaused(topicName string, partition int) bool {
	key := producePauseKey(topicName, partition)
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	exp, ok := e.consumePauses[key]
	if !ok {
		return false
	}
	if time.Now().UnixNano() >= exp {
		delete(e.consumePauses, key)
		return false
	}
	return true
}

// handoffDrainWait bounds how long PrepareHandoff waits for the leases
// in flight at freeze time to be acked or released before it reads the
// frontier. Consumers ack within milliseconds, so the bound is rarely
// reached; a lease still out when it is reached is simply redelivered
// by the new owner (at-least-once), as every lease was before.
const handoffDrainWait = 500 * time.Millisecond

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

// PrepareHandoff freezes a locally-owned partition and returns its now-
// final transfer info (segments + HWM + committed offset). This is the
// LAST-MOMENT freeze: the destination must first bulk-copy and tail the
// partition (produce flowing normally — a GB partition stays available
// for minutes) until it is caught up to within a tiny lag, and only
// THEN call PrepareHandoff. The freeze then covers just the final few
// records, so produce is paused for milliseconds regardless of
// partition size. After it, the destination copies to the returned
// (now-stable, commits frozen) HWM and proposes the ownership flip.
// Errors with ErrNotPartitionOwner if this node does not own the
// partition.
func (e *Engine) PrepareHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration) (PartitionTransferInfo, error) {
	if !e.isLocalOwner(topicName, partition) {
		return PartitionTransferInfo{}, ErrNotPartitionOwner
	}
	e.PauseProduceForHandoff(topicName, partition, freezeTTL)
	// The freeze stops new commits; existing ones under the produce lock
	// finish before this read, so the reported HWM is final.
	//
	// The consumer side is frozen too: no new reservations, and a short
	// wait for the leases already out to be acked, so the frontier read
	// below includes them. Without this the last acks on the source
	// landed after the frontier was copied and the new owner redelivered
	// those messages (seen as post-ack redeliveries on every co-location
	// move of a freshly created child topic).
	e.PauseConsumeForHandoff(topicName, partition, freezeTTL)
	if e.offsets != nil {
		deadline := time.Now().Add(min(handoffDrainWait, max(freezeTTL/4, 10*time.Millisecond)))
		for {
			inFlight, _ := e.offsets.Snapshot(topicName, partition)
			if inFlight == 0 || time.Now().After(deadline) || ctx.Err() != nil {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	return e.PartitionTransferInfo(ctx, topicName, partition)
}
