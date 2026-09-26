package topic

import (
	"encoding/json"
	"testing"
)

// BenchmarkWP1BMessageAppendJSON measures the consume-response encoder
// per key shape: no key, the common printable-ASCII key, and a
// non-ASCII UTF-8 key that needs the escaping path.
func BenchmarkWP1BMessageAppendJSON(b *testing.B) {
	base := Message{
		Topic:         "orders",
		Partition:     3,
		Offset:        12345,
		Payload:       json.RawMessage(`{"a":1,"b":"hello","c":[1,2,3],"d":true,"e":null}`),
		Timestamp:     1790000000,
		ReceiptHandle: "3:12345:991234567",
	}
	for _, tc := range []struct{ name, key string }{
		{"nokey", ""},
		{"ascii", "customer-000123"},
		{"ascii-long", "tenant-42/region-eu-west-1/customer-0000000000123456"},
		{"utf8", "clé-ünïcode-客户"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			m := base
			m.Key = tc.key
			buf := make([]byte, 0, 512)
			b.ReportAllocs()
			for b.Loop() {
				buf = m.AppendJSON(buf[:0])
			}
		})
	}
}
