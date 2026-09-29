package cluster

import (
	"context"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A requester reads the reply's shape from what it asked for (see
// consumeFrom), so an owner asked for a batch that is also a replay
// answers the one record a replay reads in the batch shape, not bare.
func TestServerBatchReplayAnswersInBatchShape(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	owner.fill(t, 1, 5, []byte(`{"k":"v"}`))
	res := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{
		Topic: "orders", Partition: 0, HasPartition: true, Offset: 2, HasOffset: true, Max: 10,
	})
	msgs := zzWP18Batch(t, res)
	if len(msgs) != 1 || msgs[0].Partition != 0 || msgs[0].Offset != 2 {
		t.Fatalf("batch replay = %+v, want the one record at offset 2", msgs)
	}
	if res.ContentType != nodewire.ContentTypeJSON {
		t.Fatalf("content type %q, want %q", res.ContentType, nodewire.ContentTypeJSON)
	}
}
