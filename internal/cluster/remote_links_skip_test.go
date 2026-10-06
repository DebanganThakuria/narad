package cluster

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A skip is accepted only for the record the link's cursor is blocked
// on: a mistyped partition or offset is refused with 409 naming the
// blocked record and stores nothing, so it cannot drop a record later
// with no admin decision.
func TestRemoteChildSkipNeedsTheCursorBlockedOnThatRecord(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	s := rg.src
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	// Nothing is blocked yet: no skip is accepted.
	if res := s.write(t, nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": 0, "offset": 10}); res.Status != http.StatusConflict {
		t.Fatalf("skip with nothing blocked: %d %s, want 409", res.Status, res.Body)
	}
	s.start()
	defer s.stop()
	good1 := s.produce(t, 0, 10, 2, 0)
	s.producePayload(t, 0, "bad", []byte(`{"not_seq":true}`))
	good2 := s.produce(t, 0, 10, 2, 100)
	rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 20*time.Second)

	for _, c := range []struct{ partition, offset int }{{0, 9}, {0, 100}} {
		res := s.write(t, nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": c.partition, "offset": c.offset})
		if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "blocked on offset 10") {
			t.Fatalf("skip of %d/%d: %d %s, want 409 naming offset 10", c.partition, c.offset, res.Status, res.Body)
		}
	}
	if stub, _ := s.store.GetTopic(context.Background(), "orders-to-b"); len(stub.Remote.Skip) != 0 {
		t.Fatalf("a refused skip was stored: %v", stub.Remote.Skip)
	}
	if res := s.write(t, nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": 0, "offset": 10}); res.Status != http.StatusOK {
		t.Fatalf("skip of the blocked record: %d %s", res.Status, res.Body)
	}
	rg.waitDelivered(t, append(good1, good2...), 20*time.Second)
}
