package node

import "testing"

// BenchmarkZZWP18EncodeConsumeRequest is the requester's side of a
// single-record probe, the request every remote consume sends.
func BenchmarkZZWP18EncodeConsumeRequest(b *testing.B) {
	req := ConsumeRequest{Topic: "orders", LocalOnly: true}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeConsumeRequest(req); err != nil {
			b.Fatal(err)
		}
	}
}
