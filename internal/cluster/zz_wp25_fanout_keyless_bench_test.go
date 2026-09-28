package cluster

import (
	"fmt"
	"testing"
)

// BenchmarkZZWP25FanoutCommitKeyless is one 4096-record slab commit
// with instant local child commits (the bucketing and dispatch cost
// only), for keyed and keyless slabs. The parent has one partition, so
// a 1-partition child keeps a keyless record's index and a 12-partition
// child places keyless records round-robin; both read the parent's
// partition count once per slab.
func BenchmarkZZWP25FanoutCommitKeyless(b *testing.B) {
	for _, c := range []int{1, 12} {
		for _, keyed := range []bool{true, false} {
			kind := "keyless"
			if keyed {
				kind = "keyed"
			}
			b.Run(fmt.Sprintf("C=%d/%s", c, kind), func(b *testing.B) {
				env := zzWP11BSetup(b, c, "node-self", nil, &zzWP11BBroker{}, true)
				slab := zzWP11BSlab(4096, 64, 1)
				if !keyed {
					for i := range slab {
						slab[i].Key = ""
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if !env.runner.commitBatch(b.Context(), env.key, slab) {
						b.Fatal("commitBatch failed")
					}
				}
			})
		}
	}
}
