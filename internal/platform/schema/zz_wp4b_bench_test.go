package schema

import (
	"context"
	"testing"
)

// The steady load driver's payload and schema: the realistic per-produce
// shape on a schema topic.
var wp4bDriverPayload = []byte(`{"id":"lc-1790424109845303000-t1-0000123456","topic":"lc-1790424109845303000-t1","sequence":123456,"key":"k-42","run_id":"lc-1790424109845303000"}`)

const wp4bDriverSchema = `{"type":"object","properties":{"id":{"type":"string"},"topic":{"type":"string"},"sequence":{"type":"integer"},"key":{"type":"string"},"run_id":{"type":"string"}},"required":["id","topic","sequence","key","run_id"],"additionalProperties":false}`

// BenchmarkWP4BValidate is Validate on the payload shapes the produce
// path sees: the load driver's flat record, the edge benchmark's nested
// event, a payload the schema rejects, and a syntax error.
func BenchmarkWP4BValidate(b *testing.B) {
	for _, tc := range []struct {
		name, schema string
		payload      []byte
		wantErr      bool
	}{
		{"driver", wp4bDriverSchema, wp4bDriverPayload, false},
		{"nested", benchSchema, benchPayload, false},
		{"rejected", benchSchema, []byte(`{"id":"not-an-int","sku":"ABC-123"}`), true},
		{"malformed", benchSchema, []byte(`{"id":1,"sku":"ABC-123",}`), true},
	} {
		r := NewJSONSchema()
		if err := r.Load(context.Background(), "t", 1, []byte(tc.schema)); err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.payload)))
			for i := 0; i < b.N; i++ {
				if err := r.Validate(context.Background(), "t", tc.payload); (err != nil) != tc.wantErr {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/parallel", func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := r.Validate(context.Background(), "t", tc.payload); (err != nil) != tc.wantErr {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
