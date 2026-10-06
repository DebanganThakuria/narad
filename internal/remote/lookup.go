// Package remote is the outbound plane of remote replication: the
// node's credential cache (decrypted once per credential version into
// a ready Authorization header and a prebuilt HTTP client), the address
// guard and transport every request to a remote goes through, the
// checks that prove a remote is safe to send to, and the ingress-side
// service behind /v1/remotes.
//
// The send path is Lookup.Get and Entry.Do: one atomic pointer load,
// one map lookup, a header assignment and the client's Do. It never
// decrypts, never reads the metastore and never takes a lock.
package remote

import "errors"

// Lookup is the node's credential cache as the send path sees it.
type Lookup interface {
	// Get returns the entry for the named remote as this node last built
	// it. One atomic pointer load and one map lookup: it never decrypts,
	// never reads the metastore, never takes a lock, never blocks.
	Get(name string) (*Entry, error)
	// Changed returns a channel closed at the next publish (a create,
	// update, rotate, delete, re-encrypt or snapshot restore reached this
	// node). Read it again after each close.
	Changed() <-chan struct{}
}

// Get errors. Each maps to exactly one link state.
var (
	ErrRemoteMissing        = errors.New("remote: no such remote on this node")                  // remote_missing
	ErrCredentialUnreadable = errors.New("remote: stored credential does not open on this node") // credential_unreadable
	ErrNodeInsecure         = errors.New("remote: this node's posture forbids remotes")          // node_insecure
	ErrNotReady             = errors.New("remote: entry not built yet")                          // unavailable, transient
)
