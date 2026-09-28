package cluster

import (
	"context"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// TestZZWP20BatchCancelReleasesEveryRecord checks that a cancel racing a
// forwarded batch's reply gives back every record the reply carried.
// The delivery record held only the first handle, so the others stayed
// hidden until their leases lapsed (30 s here) for a requester that
// never read them.
func TestZZWP20BatchCancelReleasesEveryRecord(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	owner.fill(t, 1, 5, []byte(`{"k":"v"}`))
	replies := make(chan clusterwire.StreamFrame, 1)
	payload := encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 5})
	if !owner.server.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 21, Payload: payload}, func(f clusterwire.StreamFrame) { replies <- f }) {
		t.Fatal("HandleStreamRequest returned false")
	}
	reply := <-replies
	res, err := nodewire.DecodeResponse(reply.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(zzWP18Batch(t, res)); got != 5 {
		t.Fatalf("batch carried %d records, want 5", got)
	}

	// The requester stopped waiting as the reply went out.
	owner.server.HandleStreamCancel(0, 21)

	again := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 5})
	if again.Status != http.StatusOK {
		t.Fatalf("after the cancel, a batch consume = %d, want the 5 records back", again.Status)
	}
	if got := len(zzWP18Batch(t, again)); got != 5 {
		t.Fatalf("after the cancel, %d of the 5 records are available again, want all 5", got)
	}
	// A second cancel is a no-op: the record was taken.
	owner.server.HandleStreamCancel(0, 21)
}

// TestZZWP20BatchCancelSkipsRecordsLeftOut checks that the records a
// forwarded batch left out of its reply (the encoded bound) are given
// back at once, and that a cancel racing the reply then releases the
// records it carried: every reserved record is consumable again.
func TestZZWP20BatchCancelSkipsRecordsLeftOut(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	// 48 KiB of NUL bytes is 288 KiB escaped: the 8 MiB reply bound
	// leaves most of a 4 MiB raw batch out.
	owner.fillKeyed(t, 60, "", make([]byte, 48<<10))
	replies := make(chan clusterwire.StreamFrame, 1)
	payload := encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 100})
	owner.server.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 22, Payload: payload}, func(f clusterwire.StreamFrame) { replies <- f })
	res, err := nodewire.DecodeResponse((<-replies).Payload)
	if err != nil {
		t.Fatal(err)
	}
	sent := len(zzWP18Batch(t, res))
	if sent < 2 || sent >= 60 {
		t.Fatalf("reply carried %d records, want the encoded bound to leave some out", sent)
	}
	owner.server.HandleStreamCancel(0, 22)

	taken := 0
	for range 60 {
		more := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 100})
		if more.Status == http.StatusNoContent {
			break
		}
		taken += len(zzWP18Batch(t, more))
	}
	if taken != 60 {
		t.Fatalf("after the cancel, %d of 60 records were consumable again, want all", taken)
	}
}
