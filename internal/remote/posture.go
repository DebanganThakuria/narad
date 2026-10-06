package remote

import (
	"errors"
	"strings"

	"github.com/debanganthakuria/narad/internal/errs"
)

// PostureAllowsRemotes is the posture a member must report before any
// remote write, attach or resume, and before its cache builds entries:
// security on (so its API has authorization) and legacy cluster auth
// off (whose replayable token would let a forged OpRemoteWrite
// through). Raft TLS is reported and warned about, not required (Q23).
func PostureAllowsRemotes(p Posture) bool {
	return p.SecurityEnabled && !p.LegacyClusterAuth
}

// HopAllowed reports whether this node may take a remote write, which
// carries a password in its body (Q3): only when the operator attests
// that the ingress-to-pod hop is encrypted (remotes.api_hop_encrypted).
// A later TLS listener on the node itself would also allow it.
func HopAllowed(p Posture) bool { return p.APIHopEncrypted }

// StartupInputs are what the startup rules look at.
type StartupInputs struct {
	HoldsRemotes  bool
	Posture       Posture
	ClusterSecret string
	// SecretCheck is the seal-time strength rule
	// (remotecred.CheckSecretStrength), run here only to warn.
	SecretCheck func(string) error
}

// StartupReport is what the startup rules found that does not stop the
// node: each item is logged and, where it has one, exported as a gauge.
type StartupReport struct {
	PlaintextRaft bool
	WeakSecret    bool
}

// Startup refusals. They name the rule and the fix.
var (
	ErrStartupSecurityOff = errors.New("this node's metastore holds remotes, so it must run with security.enabled: a node without API authorization must not hold outbound credentials")
	ErrStartupNoSecret    = errors.New("this node's metastore holds remotes, so NARAD_CLUSTER_SECRET is required: the remote passwords are sealed under a key derived from it")
)

// StartupCheck applies the startup rules of a node whose local
// metastore holds any remote: security off or no cluster secret refuses
// to start; plaintext Raft is warned about (Q23); a weak secret is
// warned about and does not stop the node, because a swapped secret
// already leaves every stored password unreadable and a refusal would
// take the broker down to protect nothing (every seal still refuses). A
// node without remotes is not checked at all.
func StartupCheck(in StartupInputs) (StartupReport, error) {
	var rep StartupReport
	if !in.HoldsRemotes {
		return rep, nil
	}
	if !in.Posture.SecurityEnabled {
		return rep, ErrStartupSecurityOff
	}
	if strings.TrimSpace(in.ClusterSecret) == "" {
		return rep, ErrStartupNoSecret
	}
	rep.PlaintextRaft = !in.Posture.RaftTLS
	if in.SecretCheck != nil {
		if err := in.SecretCheck(in.ClusterSecret); errors.Is(err, errs.ErrRemoteSecretWeak) {
			rep.WeakSecret = true
		}
	}
	return rep, nil
}

// AllowForceDelete is Q19: an admin may delete a remote that links
// still use (DELETE /v1/remotes/{name}?force=true). It is the fastest
// way to destroy this cluster's copy of a credential that may have
// leaked, and it loses nothing while retention lasts: the links hold in
// remote_missing. Revoking the user on the target stays the fence.
const AllowForceDelete = true
