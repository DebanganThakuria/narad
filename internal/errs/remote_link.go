package errs

import "errors"

// Remote children (a fan-out child whose records go to a topic on
// another cluster). Every conflict below wraps ErrRemoteChildConflict,
// which the HTTP and RPC error mappers answer with 409, so a new case
// needs no mapper change.
var (
	// ErrRemoteChildConflict is the 409 umbrella of remote child errors.
	ErrRemoteChildConflict = errors.New("remote child conflict")

	// ErrRemoteChildLocal reports a produce, consume or ack on a remote
	// child's stub: its records live on the remote.
	ErrRemoteChildLocal = remoteConflict("remote child lives on its remote")
	// ErrRemoteTargetLinked reports a second link from this cluster to
	// the same remote topic.
	ErrRemoteTargetLinked = remoteConflict("this cluster already links to that remote topic")
	// ErrRemoteChildLimit reports a parent with the maximum number of
	// remote children.
	ErrRemoteChildLimit = remoteConflict("parent has reached the maximum number of remote children")
	// ErrRemoteRetentionFloor reports a remote source whose retention is
	// below the floor, at attach or on a shrink.
	ErrRemoteRetentionFloor = remoteConflict("retention is below the floor for a parent with remote children")
	// ErrRemoteStubImmutable reports a change to a remote child's stub
	// through a topic update: pause and resume have their own routes.
	ErrRemoteStubImmutable = remoteConflict("a remote child's topic record cannot be changed")
	// ErrRemoteAwareDeleteRequired reports a raw topic delete or detach
	// that reached a leader for a stub, or a parent with remote
	// children: only the remote-aware delete runs the unshipped check.
	ErrRemoteAwareDeleteRequired = remoteConflict("use the remote-aware delete")
	// ErrRemoteChildStale reports a state change whose attach epoch no
	// longer matches the stub (it was deleted and attached again), or an
	// ingress whose view of the child is stale: re-read and retry.
	ErrRemoteChildStale = remoteConflict("remote child changed, retry")

	// ErrRemoteUnshipped reports a delete refused because records are
	// not yet on the remote. Returned only by the leader's mapper, with
	// the counts in the body.
	ErrRemoteUnshipped = remoteConflict("remote child has unshipped records")
)

// RemoteChildError returns an error that reads msg and matches sentinel
// (and so ErrRemoteChildConflict) under errors.Is. It lets a caller give
// an exact, client-facing message without the sentinel's text in front.
func RemoteChildError(sentinel error, msg string) error {
	return &remoteChildError{msg: msg, sentinel: sentinel}
}

type remoteChildError struct {
	msg      string
	sentinel error
}

func (e *remoteChildError) Error() string { return e.msg }
func (e *remoteChildError) Unwrap() error { return e.sentinel }

// remoteConflict makes a sentinel that wraps ErrRemoteChildConflict and
// reads msg.
func remoteConflict(msg string) error {
	return &remoteChildError{msg: msg, sentinel: ErrRemoteChildConflict}
}
