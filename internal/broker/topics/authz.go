package topics

// Leader-side authorization of topic mutations. The HTTP ingress
// authorizes a mutation against its own replica before routing it, and
// that check alone left two holes: it ran before the per-name lock, so
// a request authorized while the name held one topic acted on whatever
// topic held the name once the lock was granted (a delete racing a
// delete-and-recreate), and it read a replica that may lag the leader.
// The Manager therefore re-checks owner-or-admin from the request's
// identity after taking the name lock and the leader barrier, against
// the record as it stands; a missing record under the lock is a 404,
// never a pass.
//
// No identity on the context means no check: security is disabled, or
// the caller is internal (the purge broadcast, startup reconciliation).
// A mutation forwarded from another node runs under the identity the
// forwarder named (see cluster.RPCServer), looked up in the leader's own
// replica.

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// ForbiddenError refuses a topic mutation the request's identity may not
// make. It matches errs.ErrForbidden (403).
type ForbiddenError struct {
	// Username is the refused identity.
	Username string
	// Msg says what was refused and why.
	Msg string
}

func (e *ForbiddenError) Error() string { return e.Msg }

// Is matches errs.ErrForbidden.
func (e *ForbiddenError) Is(target error) bool { return target == errs.ErrForbidden }

// authorizeManage enforces owner-or-admin on t for the request identity
// in ctx. Callers hold t's name lock and read t under it.
func authorizeManage(ctx context.Context, t topic.Topic) error {
	id, ok := security.IdentityFrom(ctx)
	if !ok || id.IsAdmin() {
		return nil
	}
	if t.Owner != "" && t.Owner == id.Username {
		return nil
	}
	return &ForbiddenError{
		Username: id.Username,
		Msg:      fmt.Sprintf("only the owner of topic %q or an admin may modify it", t.Name),
	}
}

// authorizeManageAny enforces owner-or-admin on at least one of ts:
// detaching a fan-out child is something either side's owner may do.
func authorizeManageAny(ctx context.Context, ts ...topic.Topic) error {
	id, ok := security.IdentityFrom(ctx)
	if !ok || id.IsAdmin() {
		return nil
	}
	for _, t := range ts {
		if authorizeManage(ctx, t) == nil {
			return nil
		}
	}
	return &ForbiddenError{
		Username: id.Username,
		Msg:      "only the owner of the parent or the child topic, or an admin, may modify this fan-out link",
	}
}

// authorizeCreate re-checks the create grant for name against the
// request identity: the ingress checked the same grant against its own
// copy of the user, and a forwarded create runs under the user record
// the leader holds.
func authorizeCreate(ctx context.Context, name string) error {
	id, ok := security.IdentityFrom(ctx)
	if !ok || id.Allowed(user.ActionCreate, name) {
		return nil
	}
	return &ForbiddenError{
		Username: id.Username,
		Msg:      fmt.Sprintf("%s has no create grant matching topic %q", id.Username, name),
	}
}
