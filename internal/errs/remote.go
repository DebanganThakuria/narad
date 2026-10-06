package errs

import "errors"

// Remotes registry and credentials. The leader's remote write mapper
// answers each with a fixed message and status and never appends
// err.Error(), so no wrapped detail (an address, a key version) reaches
// a client.
var (
	// ErrRemoteNotFound reports that no remote has the name (404).
	ErrRemoteNotFound = errors.New("remote not found")

	// ErrRemoteExists reports a create whose name is taken (409).
	ErrRemoteExists = errors.New("remote already exists")

	// ErrRemoteInUse reports a delete of a remote that remote children
	// still name, without force (409).
	ErrRemoteInUse = errors.New("remote is used by remote children")

	// ErrRemoteLimit reports a create beyond MaxRemotes (409).
	ErrRemoteLimit = errors.New("cluster holds the maximum number of remotes")

	// ErrRemoteChanged reports a sealed credential whose record moved
	// under it (URL, username, CA or credential version), or a URL,
	// username or CA change that came without a new credential (409).
	ErrRemoteChanged = errors.New("remote changed, retry")

	// ErrRemoteSaltRace reports two first creates that minted different
	// per-cluster salts; the loser's ciphertext is sealed under a key
	// nobody derives (409, retry).
	ErrRemoteSaltRace = errors.New("remote salt raced, retry")

	// ErrRemoteKeyExhausted reports a key version whose seal count has
	// reached its cap: rotate the cluster secret (412).
	ErrRemoteKeyExhausted = errors.New("encryption key seal budget exhausted; rotate the cluster secret")

	// ErrRemoteFeatureGate reports a member on an older release, or one
	// that did not answer, while remotes need every member (412).
	ErrRemoteFeatureGate = errors.New("a cluster member does not support remotes")

	// ErrRemotePosture reports a member whose security posture forbids
	// remotes: security off or legacy cluster auth on (412).
	ErrRemotePosture = errors.New("a cluster member's security posture forbids remotes")

	// ErrRemoteHopUnencrypted reports a remote write on a node whose
	// ingress-to-pod hop is not attested encrypted (412).
	ErrRemoteHopUnencrypted = errors.New("remote writes need an encrypted API hop")

	// ErrRemoteSecretWeak reports a cluster secret that fails the
	// strength rule, so nothing may be sealed under it (412).
	ErrRemoteSecretWeak = errors.New("cluster secret too weak to seal remote credentials")

	// ErrRemoteSecretMissing reports a node with no cluster secret at
	// all, so no key can be derived (412).
	ErrRemoteSecretMissing = errors.New("no cluster secret to seal remote credentials")

	// ErrRemoteCredentialUnreadable reports a stored credential this
	// node cannot open (unknown key version, bad tag, or a record that
	// no longer matches what it was sealed to).
	ErrRemoteCredentialUnreadable = errors.New("remote credential unreadable on this node")

	// ErrRemoteDestinationRefused reports a URL or a dial the port
	// list, the host allowlist or the address guard refused.
	ErrRemoteDestinationRefused = errors.New("remote destination refused")

	// ErrRemoteCheckFailed reports a failed attach, resume or test
	// check; the answer carries the per-node reports.
	ErrRemoteCheckFailed = errors.New("remote check failed")

	// ErrRemoteThrottled reports the per-node remote write limit or the
	// per-remote check limit (429).
	ErrRemoteThrottled = errors.New("remote operation throttled")
)
