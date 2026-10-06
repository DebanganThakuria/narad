package metrics

import "time"

// RecordMemberHeartbeat sets narad_member_heartbeat_failures to the
// current streak of consecutive failed member heartbeats (0 after a
// success) and, on a success, stamps
// narad_member_heartbeat_last_success_timestamp_seconds with
// succeededAt. Safe on a nil receiver, which records nothing.
func (m *Metrics) RecordMemberHeartbeat(consecutiveFailures int, succeededAt time.Time) {
	if m == nil {
		return
	}
	m.MemberHeartbeatFailures.Set(float64(consecutiveFailures))
	if consecutiveFailures == 0 && !succeededAt.IsZero() {
		m.MemberHeartbeatLastSuccess.Set(float64(succeededAt.UnixMilli()) / 1000)
	}
}
