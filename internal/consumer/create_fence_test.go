package consumer

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestDropWaitsForShardCreate pins the create fence. A shard create
// reads the partition's persisted state by path and stores the shard
// afterwards; a drop that runs in between (the retire of a topic
// incarnation, a move's install) must wait for the store and delete
// that shard. Without the fence the drop found nothing to delete and
// returned, the create stored its shard after it with the frontier it
// read from the copy that is gone, and the partition's next consumer
// was served from that frontier instead of the files now at the path.
func TestDropWaitsForShardCreate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		drop func(f *InFlight)
	}{
		{"DropTopic", func(f *InFlight) { f.DropTopic(testTopic) }},
		{"DropPartition", func(f *InFlight) { f.DropPartition(testTopic, testPart) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := newClockedInFlight(8, 8)
			read := make(chan struct{})
			release := make(chan struct{})
			var first sync.Once
			f.SetCommittedRecovery(func(string, int) (int64, bool) {
				parked := false
				first.Do(func() { parked = true })
				if !parked {
					// The files at the path after the drop: none.
					return 0, false
				}
				// The first create reads the frontier of the copy the
				// drop retires, and is descheduled before its store.
				close(read)
				<-release
				return 19, true
			})

			created := make(chan error, 1)
			go func() {
				_, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
				created <- err
			}()
			<-read

			dropped := make(chan struct{})
			go func() {
				tc.drop(f)
				close(dropped)
			}()
			select {
			case <-dropped:
				t.Errorf("%s returned while a shard create that read before it had not stored its shard", tc.name)
			case <-time.After(100 * time.Millisecond):
			}
			close(release)
			if err := <-created; err != nil {
				t.Fatalf("ReserveNext: %v", err)
			}
			<-dropped

			if committed, ok := f.CommittedOffset(testTopic, testPart); ok {
				t.Errorf("a shard with the retired copy's frontier %d outlived %s", committed, tc.name)
			}
			r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
			if err != nil {
				t.Fatalf("ReserveNext: %v", err)
			}
			if !r.Reserved || r.Offset != 0 {
				t.Errorf("first reserve after %s = %+v, want offset 0 (the copy at the path has no consumer state)", tc.name, r)
			}
		})
	}
}
