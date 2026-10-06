package topic

import (
	"errors"
	"slices"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// RemoteLink makes a child topic a remote child: a stub with zero
// partitions whose fan-out cursors send each parent record to a topic on
// another Narad cluster (the remote) instead of committing it to local
// child partitions. It is set at attach, changed only by the field-scoped
// state op (pause, resume, target, lanes, skip), and never by a topic
// update.
type RemoteLink struct {
	// Name is the remote's name in this cluster's remotes registry.
	Name string `json:"name"`
	// Topic is the topic on the remote the records are produced to.
	Topic string `json:"topic"`
	// TargetID is the target topic's incarnation ID as the attach (or the
	// last resume that accepted the target) saw it. Empty when the target
	// does not serve IDs: recreate detection is then off for the link.
	TargetID string `json:"target_id,omitempty"`
	// From is where the link started: attach, unconsumed or earliest.
	From string `json:"from,omitempty"`
	// Lanes is how many ordered streams each parent partition ships on
	// (1..8). A key always maps to one lane.
	Lanes int `json:"lanes,omitempty"`
	// Paused stops sending; cursors keep their positions.
	Paused      bool   `json:"paused,omitempty"`
	PauseReason string `json:"pause_reason,omitempty"`
	// PausedBy and CreatedBy are admin usernames, shown to admins only.
	PausedBy   string `json:"paused_by,omitempty"`
	PausedAtMs int64  `json:"paused_at_ms,omitempty"`
	// Skip records, per parent partition, the offsets an admin accepted
	// to lose, ascending, at most MaxRemoteSkipsPerPartition of them
	// (the newest kept). A cursor drops a record only while it is blocked
	// on exactly one of those offsets, so a stale entry changes nothing.
	// Each skip is kept, not replaced by the next: a slab read again (a
	// re-read, a restart, a partition move) meets every skipped record
	// again and must still drop it.
	Skip      map[int][]int64 `json:"skip,omitempty"`
	CreatedBy string          `json:"created_by,omitempty"`
}

// MaxRemoteSkipsPerPartition bounds the skips a link keeps per parent
// partition. A slab blocks at most one record per lane (8 at most) at a
// time, so the newest 16 always cover the records a cursor can still
// meet; older ones lie behind it and change nothing.
const MaxRemoteSkipsPerPartition = 16

// Skipped reports whether an admin skipped the record at offset of the
// parent partition.
func (l RemoteLink) Skipped(partition int, offset int64) bool {
	_, found := slices.BinarySearch(l.Skip[partition], offset)
	return found
}

// WithSkip returns a copy of skip with offset added to the partition's
// set: kept ascending, each offset once, at most
// MaxRemoteSkipsPerPartition kept. The offset just added is always kept:
// the lowest of the others go first. Trimming the added one would answer
// the admin's skip and leave the lane blocked on it.
func WithSkip(skip map[int][]int64, partition int, offset int64) map[int][]int64 {
	out := make(map[int][]int64, len(skip)+1)
	for p, offs := range skip {
		out[p] = slices.Clone(offs)
	}
	offs := out[partition]
	if i, found := slices.BinarySearch(offs, offset); !found {
		offs = slices.Insert(offs, i, offset)
	}
	for len(offs) > MaxRemoteSkipsPerPartition {
		drop := 0
		if offs[0] == offset {
			drop = 1
		}
		offs = slices.Delete(offs, drop, drop+1)
	}
	out[partition] = offs
	return out
}

// IsRemoteChild reports whether the topic is a remote child's stub.
func (t Topic) IsRemoteChild() bool { return t.Remote != nil && t.IsChild() }

// EffectiveLanes is the link's lane count, 1 when unset.
func (l RemoteLink) EffectiveLanes() int {
	if l.Lanes < MinRemoteLanes {
		return MinRemoteLanes
	}
	return min(l.Lanes, MaxRemoteLanes)
}

// Remote child limits.
const (
	// MaxRemoteChildrenPerParent caps a parent's remote children; each
	// holds one cursor per parent partition and ships over the network.
	MaxRemoteChildrenPerParent = 16
	// MinRemoteSourceRetentionMs is the retention floor of a parent with
	// remote children (Q6): a remote down overnight is a normal event,
	// and the parent's log is the only buffer. Retention 0 (keep
	// forever) passes.
	MinRemoteSourceRetentionMs int64 = 24 * 60 * 60 * 1000
	// RemoteChainsAllowed is false (Q7): a remote child's target must not
	// itself have remote children, which makes every loop impossible.
	RemoteChainsAllowed = false

	// MinRemoteLanes and MaxRemoteLanes bound RemoteLink.Lanes.
	MinRemoteLanes = 1
	MaxRemoteLanes = 8

	// MaxRemotePauseReasonBytes caps the only free-text field of a stub.
	MaxRemotePauseReasonBytes = 256
)

// Start points of a remote child, per parent partition.
const (
	// RemoteFromAttach starts at the committed high watermark, the rule
	// local children follow.
	RemoteFromAttach = "attach"
	// RemoteFromUnconsumed starts at the parent's consumer ack frontier,
	// so everything not yet acked on this cluster is sent.
	RemoteFromUnconsumed = "unconsumed"
	// RemoteFromEarliest starts at the oldest retained offset.
	RemoteFromEarliest = "earliest"
)

// ValidRemoteFrom reports whether from names a start point ("" means
// attach).
func ValidRemoteFrom(from string) bool {
	switch from {
	case "", RemoteFromAttach, RemoteFromUnconsumed, RemoteFromEarliest:
		return true
	}
	return false
}

// Link states and check classes, in severity order (ch. 7.6). The
// user-facing words map onto them: unauthorized is auth_failed,
// rejected is rejected_record, remote unknown is remote_missing,
// unreachable is unavailable, TLS is tls_failed.
const (
	RemoteStateRemoteMissing           = "remote_missing"
	RemoteStateCredentialUnreadable    = "credential_unreadable"
	RemoteStateNodeInsecure            = "node_insecure"
	RemoteStateDestinationRefused      = "destination_refused"
	RemoteStateTargetHasRemoteChildren = "target_has_remote_children"
	RemoteStateTargetReplaced          = "target_replaced"
	RemoteStateAuthFailed              = "auth_failed"
	RemoteStateForbidden               = "forbidden"
	RemoteStateTargetMissing           = "target_missing"
	RemoteStateNoBatchProduce          = "no_batch_produce"
	RemoteStateRedirectRefused         = "redirect_refused"
	RemoteStateTLSFailed               = "tls_failed"
	RemoteStateRejectedRecord          = "rejected_record"
	RemoteStateRecordTooLarge          = "record_too_large"
	RemoteStateUnavailable             = "unavailable"
	RemoteStateThrottled               = "throttled"
	RemoteStateUnknown                 = "unknown"
	RemoteStatePaused                  = "paused"
	RemoteStateRunning                 = "running"
	// RemoteClassEdge is an answer from something in front of the target
	// (a load balancer, a WAF), not the target: treated as unavailable.
	RemoteClassEdge = "edge"
	// RemoteClassEncoding is a compressed chunk the target could not
	// decode: resent uncompressed at once.
	RemoteClassEncoding = "encoding"
)

// RemoteStateSeverity lists the link states worst first; a link shows
// its worst partition.
var RemoteStateSeverity = []string{
	RemoteStateRemoteMissing,
	RemoteStateCredentialUnreadable,
	RemoteStateNodeInsecure,
	RemoteStateDestinationRefused,
	RemoteStateTargetHasRemoteChildren,
	RemoteStateTargetReplaced,
	RemoteStateAuthFailed,
	RemoteStateForbidden,
	RemoteStateTargetMissing,
	RemoteStateNoBatchProduce,
	RemoteStateRedirectRefused,
	RemoteStateTLSFailed,
	RemoteStateRejectedRecord,
	RemoteStateRecordTooLarge,
	RemoteStateUnavailable,
	RemoteStateThrottled,
	RemoteStateUnknown,
	RemoteStatePaused,
	RemoteStateRunning,
}

// RemoteStateRank is state's position in RemoteStateSeverity (0 is the
// worst). An unlisted state ranks with unknown.
func RemoteStateRank(state string) int {
	for i, s := range RemoteStateSeverity {
		if s == state {
			return i
		}
	}
	return RemoteStateRank(RemoteStateUnknown)
}

// WorseRemoteState returns the more severe of a and b; "" is ignored.
func WorseRemoteState(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case RemoteStateRank(b) < RemoteStateRank(a):
		return b
	}
	return a
}

// RemoteBlock names the one record a remote cursor is stuck on.
type RemoteBlock struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	State     string `json:"state"`
}

// ValidateRemotePauseReason enforces the pause reason rule: at most
// MaxRemotePauseReasonBytes of printable UTF-8. The error names the rule,
// never the value.
func ValidateRemotePauseReason(reason string) error {
	if len(reason) > MaxRemotePauseReasonBytes {
		return errors.New("reason must be at most " + strconv.Itoa(MaxRemotePauseReasonBytes) + " bytes")
	}
	if !utf8.ValidString(reason) {
		return errors.New("reason must be valid UTF-8")
	}
	for _, r := range reason {
		if !unicode.IsPrint(r) {
			return errors.New("reason must be printable text without control characters")
		}
	}
	return nil
}
