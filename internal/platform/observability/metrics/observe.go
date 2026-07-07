package metrics

import "strconv"

// Every method here tolerates a nil receiver so callers can thread a
// possibly-nil *Metrics through hot paths without guarding each site.

// IncError bumps the cross-cutting errors_total counter so callers don't
// need to remember the label order.
func (m *Metrics) IncError(component, kind string) {
	if m == nil {
		return
	}
	m.ErrorsTotal.WithLabelValues(component, kind).Inc()
}

// IncCorruptSkipped records that a consumer skipped one permanently-
// unreadable (corrupt) offset on the given partition — a lost record.
func (m *Metrics) IncCorruptSkipped(topic string, partition int) {
	if m == nil {
		return
	}
	m.CorruptSkippedTotal.WithLabelValues(topic, strconv.Itoa(partition)).Inc()
}

// IncAckRejected bumps the ack_rejected_total counter.
func (m *Metrics) IncAckRejected(reason string) {
	if m == nil || reason == "" {
		return
	}
	m.AckRejected.WithLabelValues(reason).Inc()
}

// IncAckExtended bumps the ack_extended_total counter.
func (m *Metrics) IncAckExtended(topic string) {
	if m == nil {
		return
	}
	m.AckExtendedTotal.WithLabelValues(topic).Inc()
}

// IncNack bumps the nack_total counter.
func (m *Metrics) IncNack(topic string) {
	if m == nil {
		return
	}
	m.NackTotal.WithLabelValues(topic).Inc()
}
