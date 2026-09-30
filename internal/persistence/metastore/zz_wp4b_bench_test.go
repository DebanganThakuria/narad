package metastore

import (
	"fmt"
	"testing"
)

// BenchmarkWP4BDomainVersionRead is the lock-free per-key version read
// the produce, consume and routing caches do several times per request,
// for a key with its own cell and for one without (it reads the floor).
func BenchmarkWP4BDomainVersionRead(b *testing.B) {
	v := newMetadataDomainVersions()
	for i := 0; i < 1000; i++ {
		v.bumpTopic(fmt.Sprintf("topic-%04d", i))
	}
	for _, tc := range []struct{ name, key string }{{"known", "topic-0500"}, {"unknown", "never-seen"}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var sink uint64
				for pb.Next() {
					sink += v.topicVersion(tc.key)
				}
				_ = sink
			})
		})
	}
}

// BenchmarkWP4BDomainVersionNewKey is the FSM-apply cost of the first
// bump of a new name (a topic create) when the table already holds keys
// names.
func BenchmarkWP4BDomainVersionNewKey(b *testing.B) {
	for _, keys := range []int{100, 10000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			v := newMetadataDomainVersions()
			for i := 0; i < keys; i++ {
				v.bumpTopic(fmt.Sprintf("topic-%06d", i))
			}
			names := make([]string, b.N)
			for i := range names {
				names[i] = fmt.Sprintf("new-%09d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v.bumpTopic(names[i])
			}
		})
	}
}
