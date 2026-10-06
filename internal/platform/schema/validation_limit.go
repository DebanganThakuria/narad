package schema

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

// limitedPayloadBytes is the payload size above which validation runs
// under the node's validation limit. Below it a payload on an ordinary
// schema validates in well under a millisecond and pays nothing for the
// limit; above it the cost grows with the payload (up to the 1 MiB body
// limit) and one tenant's large produces could otherwise occupy every
// core.
const limitedPayloadBytes = 16 << 10

// defaultValidationWait is how long a validation waits for a slot
// before the produce is refused as retryable. It is well under the
// server's 30 s write timeout, so the refusal reaches the client.
const defaultValidationWait = 5 * time.Second

// validationLimiter bounds the validations that run at once on a node:
// every payload above limitedPayloadBytes, and every payload on a
// schema the cost analysis flagged (see compiledSchema.expensive),
// which is how a schema persisted before the registration checks
// existed stays bounded: persisted schemas are never re-checked.
type validationLimiter struct {
	slots chan struct{}
	wait  time.Duration
}

func newValidationLimiter(slots int, wait time.Duration) *validationLimiter {
	return &validationLimiter{slots: make(chan struct{}, max(slots, 1)), wait: wait}
}

// acquire takes a slot, waiting at most l.wait and never past ctx.
func (l *validationLimiter) acquire(ctx context.Context) error {
	select {
	case l.slots <- struct{}{}:
		return nil
	default:
	}
	if err := ctx.Err(); err != nil {
		return &validationWaitError{cause: err}
	}
	timer := time.NewTimer(l.wait)
	defer timer.Stop()
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &validationWaitError{cause: ctx.Err()}
	case <-timer.C:
		return &validationWaitError{cause: errValidationBusy, wait: l.wait}
	}
}

func (l *validationLimiter) release() { <-l.slots }

// errValidationBusy is the cause of a wait that ran out of time.
var errValidationBusy = errors.New("schema validation capacity is busy")

// validationWaitError reports a validation that never started: the
// wait for a slot ended by timeout (the node is busy; retryable,
// preferably on another node, and wraps errs.ErrUnavailable so it maps
// to 503) or by the request's context (the client went away or its
// deadline passed).
type validationWaitError struct {
	cause error // errValidationBusy or the context's error
	wait  time.Duration
}

func (e *validationWaitError) Error() string {
	if errors.Is(e.cause, errValidationBusy) {
		return fmt.Sprintf("schema: validation capacity busy, retry: no schema validation slot on this node freed up within %v, so the payload was not validated; retry, preferably through another node", e.wait)
	}
	return fmt.Sprintf("schema: payload not validated: %v while waiting for a schema validation slot", e.cause)
}

// Unwrap exposes the cause, and errs.ErrUnavailable for anything but a
// cancelled request (a cancellation is the client's own doing and maps
// to "client closed request").
func (e *validationWaitError) Unwrap() []error {
	if errors.Is(e.cause, context.Canceled) {
		return []error{e.cause}
	}
	return []error{e.cause, errs.ErrUnavailable}
}

// IsCapacityError reports whether err is a validation that never ran
// because the node's validation limit had no slot for it. Such an error
// says nothing about the payload, so a caller must not report it as an
// invalid payload.
func IsCapacityError(err error) bool {
	_, ok := errors.AsType[*validationWaitError](err)
	return ok
}

// SetValidationLimit replaces the node's validation limit: at most
// slots validations of large payloads (or on costly schemas) run at
// once, and one waits at most wait for a slot. The default is
// runtime.GOMAXPROCS(0) slots and a 5 s wait. It must be called before
// the registry is used.
func (r *JSONSchema) SetValidationLimit(slots int, wait time.Duration) {
	r.limiter = newValidationLimiter(slots, wait)
}

func defaultValidationLimiter() *validationLimiter {
	return newValidationLimiter(runtime.GOMAXPROCS(0), defaultValidationWait)
}
