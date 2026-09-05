package messaging

// Handoff freeze for a rebalance. When a partition is about to hand off
// to a new owner, this node briefly stops both ACCEPTING and COMMITTING
// produce for it, so no record lands after the destination captured the
// final tail. Frozen produce reroutes to a live partition and frozen
// commits make the ingress dispatcher retry to the new owner (AP) — the
// same paths as a dead owner. The freeze carries a TTL so if the handoff
// never completes (the destination died), the source auto-resumes.
//
// The freeze is FENCED: arming it mints a token that the destination
// must present on every re-arm and on the fence right before the flip.
// A freeze that lapsed (the TTL passed with no re-arm) is gone for good:
// presenting its token fails, so a destination whose cutover outlived
// the TTL learns that commits may have landed on the source and drains
// again under a fresh freeze instead of flipping over them.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ErrHandoffFreezeLapsed reports that the freeze a token names is no
// longer active: its TTL passed without a re-arm, ownership changed, or
// a different freeze has since been armed. The destination must not
// flip on it.
var ErrHandoffFreezeLapsed = errors.New("handoff freeze lapsed")

// producePause is one armed handoff freeze: its expiry and the token
// that fences it.
type producePause struct {
	expiresUnixNano int64
	token           string
}

func producePauseKey(topicName string, partition int) string {
	return topicName + "/" + strconv.Itoa(partition)
}

func newFreezeToken() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A clock-derived token still fences against every realistic
		// lapse; only a byte-identical re-mint in the same nanosecond
		// would collide.
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

// armHandoffFreeze pauses produce for the partition until ttl elapses
// (or ResumeProduce), returning the freeze's token. With an empty
// requireToken it arms a fresh freeze or extends the active one, keeping
// the active one's token (idempotent). With a token it only EXTENDS the
// freeze that token names: if no freeze is active, or the active one
// carries a different token, it fails with ErrHandoffFreezeLapsed and
// leaves the pause state untouched, so a lapsed freeze is never silently
// resurrected under a stale token.
func (e *Engine) armHandoffFreeze(topicName string, partition int, ttl time.Duration, requireToken string) (string, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	key := producePauseKey(topicName, partition)
	now := time.Now().UnixNano()
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	if e.producePauses == nil {
		e.producePauses = make(map[string]producePause)
	}
	cur, active := e.producePauses[key]
	if active && now >= cur.expiresUnixNano {
		delete(e.producePauses, key)
		active = false
	}
	if requireToken != "" && (!active || cur.token != requireToken) {
		return "", ErrHandoffFreezeLapsed
	}
	token := cur.token
	if !active {
		token = newFreezeToken()
	}
	e.producePauses[key] = producePause{expiresUnixNano: now + int64(ttl), token: token}
	return token, nil
}

// PauseProduceForHandoff stops accepting produce for a partition this
// node owns, until ResumeProduce is called or ttl elapses (whichever
// first). Idempotent: re-pausing extends the deadline.
func (e *Engine) PauseProduceForHandoff(topicName string, partition int, ttl time.Duration) {
	_, _ = e.armHandoffFreeze(topicName, partition, ttl, "")
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
	p, ok := e.producePauses[key]
	if !ok {
		return false
	}
	if time.Now().UnixNano() >= p.expiresUnixNano {
		delete(e.producePauses, key)
		return false
	}
	return true
}

// handoffFreezeActive reports whether the freeze named by token is
// still armed for the partition. Tests use it to pin the fence.
func (e *Engine) handoffFreezeActive(topicName string, partition int, token string) bool {
	key := producePauseKey(topicName, partition)
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	p, ok := e.producePauses[key]
	return ok && p.token == token && time.Now().UnixNano() < p.expiresUnixNano
}

// PrepareHandoff freezes a locally-owned partition and returns its now-
// final transfer info (segments + HWM + committed offset + fan-out
// cursor sidecars) together with the freeze token. This is the
// LAST-MOMENT freeze: the destination must first bulk-copy and tail the
// partition (produce flowing normally — a GB partition stays available
// for minutes) until it is caught up to within a tiny lag, and only
// THEN call PrepareHandoff. The freeze then covers just the final few
// records, so produce is paused for milliseconds regardless of
// partition size. After it, the destination copies to the returned
// (now-stable, commits frozen) HWM, re-arms the freeze with the token
// while it drains (ConfirmHandoff), fences the flip with it, and
// proposes the ownership flip. Idempotent: a repeat call extends the
// active freeze and returns its existing token.
// Errors with ErrNotPartitionOwner if this node does not own the
// partition.
func (e *Engine) PrepareHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration) (PartitionTransferInfo, error) {
	return e.prepareHandoff(ctx, topicName, partition, freezeTTL, "")
}

// ConfirmHandoff re-arms the freeze that token names for another
// freezeTTL and returns the partition's current transfer info. It fails
// with ErrHandoffFreezeLapsed when that freeze is no longer active, so
// the destination knows the source may have accepted commits since and
// must not flip: it re-freezes (PrepareHandoff) and drains again. The
// destination calls it periodically while draining and once more as the
// fence immediately before proposing the flip.
func (e *Engine) ConfirmHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration, token string) (PartitionTransferInfo, error) {
	if token == "" {
		return PartitionTransferInfo{}, errors.New("confirm handoff: freeze token required")
	}
	return e.prepareHandoff(ctx, topicName, partition, freezeTTL, token)
}

func (e *Engine) prepareHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration, requireToken string) (PartitionTransferInfo, error) {
	if err := e.checkTransferable(ctx, topicName, partition); err != nil {
		return PartitionTransferInfo{}, err
	}
	token, err := e.armHandoffFreeze(topicName, partition, freezeTTL, requireToken)
	if err != nil {
		return PartitionTransferInfo{}, err
	}
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
	// The freeze stops NEW commits at the gate, but a commit that passed
	// the gate before the freeze may still be inside its append+fsync
	// under the produce lock. Taking that lock here waits for it to
	// finish, so the HWM reported is final: nothing can advance it
	// afterwards while the freeze holds.
	dir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	var info PartitionTransferInfo
	err = e.logs.WithProduceLock(topicName, partition, func(log *storage.Log) error {
		var ierr error
		info, ierr = e.transferInfoAt(dir, topicName, partition, log.HighWatermark())
		return ierr
	})
	if err != nil {
		return PartitionTransferInfo{}, err
	}
	info.FreezeToken = token
	return info, nil
}
