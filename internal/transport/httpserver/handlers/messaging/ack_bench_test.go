package messaging

import "testing"

// BenchmarkAckQueryParse measures the per-request raw-query walk on the
// ack hot path — a plain ack (the overwhelmingly common case) and the
// lease variants.
func BenchmarkAckQueryParse(b *testing.B) {
	queries := map[string]string{
		"plain_ack": "receipt_handle=3%3A128%3A991234567",
		"extend":    "receipt_handle=3%3A128%3A991234567&extend=true",
		"nack":      "receipt_handle=3%3A128%3A991234567&extend=0",
	}
	for name, raw := range queries {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				if _, found, mode, err := ackParamsFromRawQuery(raw); err != nil || !found || mode == ackModeInvalid {
					b.Fatal("unexpected parse result")
				}
			}
		})
	}
}
