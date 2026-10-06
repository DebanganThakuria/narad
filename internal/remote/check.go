package remote

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// CheckRequest asks a member to run the ch. 4.7 checks.
type CheckRequest struct {
	Remote            string          `json:"remote"`
	Topic             string          `json:"topic"`                   // topic on the remote
	Source            string          `json:"source,omitempty"`        // parent on this cluster
	SourceID          string          `json:"source_id,omitempty"`     // parent's topic.Topic.ID
	SourceSchema      json.RawMessage `json:"source_schema,omitempty"` // parent's current schema
	CredentialVersion uint64          `json:"credential_version"`      // the record's, as the leader reads it
}

// Check results.
const (
	ResultPass = "pass"
	ResultFail = "fail"
)

// NodeReport is one member's answer.
type NodeReport struct {
	Node              string   `json:"node"`
	Result            string   `json:"result"`          // "pass" or "fail"
	Class             string   `json:"class,omitempty"` // a topic.RemoteState value, or a check class (below)
	CredentialVersion uint64   `json:"credential_version"`
	TargetID          string   `json:"target_id,omitempty"`
	TargetServesIDs   bool     `json:"target_serves_ids"` // the target shows remote and parent_id
	RTTMs             *int64   `json:"rtt_ms,omitempty"`  // only with remotes.allowed_hosts set
	LaneCapacityPerS  *int64   `json:"lane_capacity_per_s,omitempty"`
	CertNotAfter      string   `json:"server_cert_not_after,omitempty"`
	Warnings          []string `json:"warnings"`
	Posture           Posture  `json:"posture"`
}

// Blind returns reports as an admin may see them without a host
// allowlist (blind by design): each member's node, result and class,
// with this cluster's own credential version and posture, and nothing
// the target answered (its ID, its certificate's expiry, warnings drawn
// from its answers, timings).
func Blind(reports []NodeReport) []NodeReport {
	out := make([]NodeReport, len(reports))
	for i, r := range reports {
		out[i] = NodeReport{Node: r.Node, Result: r.Result, Class: r.Class, CredentialVersion: r.CredentialVersion, Warnings: []string{}, Posture: r.Posture}
	}
	return out
}

// Failing names the members whose report failed.
func Failing(reports []NodeReport, credentialVersion uint64) []string {
	out := []string{}
	for _, r := range reports {
		if r.Result != ResultPass || r.CredentialVersion != credentialVersion {
			out = append(out, r.Node)
		}
	}
	return out
}

// Check classes beyond the link states.
const (
	ClassTargetSecurityOff  = "target_security_off"
	ClassAdminCredential    = "admin_credential"
	ClassTargetIsDelayChild = "target_is_delay_child"
	ClassTargetIsStub       = "target_is_stub"
	ClassTargetIsSource     = "target_is_source"
	ClassSchemaMismatch     = "schema_mismatch"
	ClassStale              = "stale"
	ClassOldRelease         = "old_release"
	ClassUnreachable        = "unreachable"
	// ClassTargetDisagreement: members passed but saw different target
	// IDs (a DNS name that resolves to two clusters, say).
	ClassTargetDisagreement = "target_disagreement"
)

// Posture is what a member reports about its own security settings.
type Posture struct {
	SecurityEnabled   bool `json:"security_enabled"`
	LegacyClusterAuth bool `json:"legacy_cluster_auth"`
	RaftTLS           bool `json:"raft_tls"`          // reported, not gated (Q23)
	APIHopEncrypted   bool `json:"api_hop_encrypted"` // reported; gates remote writes on the ingress only
}

// CheckError is a failed attach, resume or test: Status is 409, 412,
// 429, 502 or 503 as ch. 4.2 assigns them, and Reports name the members.
type CheckError struct {
	Status  int
	Class   string
	Reports []NodeReport
}

func (e *CheckError) Error() string {
	return fmt.Sprintf("remote check failed: %s", e.Class)
}

// classPriority orders failing classes by which one names the answer:
// cluster-state problems (412) first, since no retry of the remote side
// helps until they are fixed, then the throttle, then what the remote
// said, then transient answers.
var classPriority = map[string]int{
	ClassOldRelease:                          0,
	ClassUnreachable:                         1,
	topic.RemoteStateNodeInsecure:            2,
	topic.RemoteStateCredentialUnreadable:    3,
	topic.RemoteStateRemoteMissing:           4,
	ClassStale:                               5,
	ClassTargetDisagreement:                  6,
	topic.RemoteStateThrottled:               7,
	topic.RemoteStateDestinationRefused:      8,
	topic.RemoteStateTLSFailed:               9,
	ClassTargetSecurityOff:                   10,
	topic.RemoteStateAuthFailed:              11,
	topic.RemoteStateForbidden:               12,
	ClassAdminCredential:                     13,
	topic.RemoteStateTargetMissing:           14,
	ClassTargetIsSource:                      15,
	ClassTargetIsDelayChild:                  16,
	ClassTargetIsStub:                        17,
	topic.RemoteStateTargetHasRemoteChildren: 18,
	ClassSchemaMismatch:                      19,
	topic.RemoteStateNoBatchProduce:          20,
	topic.RemoteStateRedirectRefused:         21,
	topic.RemoteClassEdge:                    22,
	topic.RemoteStateUnavailable:             23,
}

// StatusForClass maps a failing check class to its HTTP status: 412
// for a member that cannot take part (older, unreachable, insecure,
// stale or unreadable credential, disagreeing), 429 for the check
// limiter, 502 for an answer that proves nothing, 503 for a transient
// failure, and 409 for everything the remote refused.
func StatusForClass(class string) int {
	switch class {
	case ClassOldRelease, ClassUnreachable, topic.RemoteStateNodeInsecure,
		topic.RemoteStateCredentialUnreadable, topic.RemoteStateRemoteMissing,
		ClassStale, ClassTargetDisagreement:
		return http.StatusPreconditionFailed
	case topic.RemoteStateThrottled:
		return http.StatusTooManyRequests
	case topic.RemoteStateRedirectRefused, topic.RemoteClassEdge:
		return http.StatusBadGateway
	case topic.RemoteStateUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusConflict
	}
}

// Verdict folds the members' reports: the agreed target ID when every
// member passed at credentialVersion, else a *CheckError.
func Verdict(reports []NodeReport, credentialVersion uint64) (targetID string, err error) {
	if len(reports) == 0 {
		return "", &CheckError{Status: http.StatusPreconditionFailed, Class: ClassUnreachable}
	}
	failing := make([]string, 0, len(reports))
	for _, r := range reports {
		switch {
		case r.Result != ResultPass:
			class := r.Class
			if class == "" {
				class = topic.RemoteStateUnknown
			}
			failing = append(failing, class)
		case r.CredentialVersion != credentialVersion:
			failing = append(failing, ClassStale)
		}
	}
	if len(failing) > 0 {
		sort.SliceStable(failing, func(i, j int) bool { return priorityOf(failing[i]) < priorityOf(failing[j]) })
		return "", &CheckError{Status: StatusForClass(failing[0]), Class: failing[0], Reports: reports}
	}
	targetID = reports[0].TargetID
	for _, r := range reports[1:] {
		if r.TargetID != targetID {
			return "", &CheckError{Status: http.StatusPreconditionFailed, Class: ClassTargetDisagreement, Reports: reports}
		}
	}
	return targetID, nil
}

func priorityOf(class string) int {
	if p, ok := classPriority[class]; ok {
		return p
	}
	return len(classPriority)
}
