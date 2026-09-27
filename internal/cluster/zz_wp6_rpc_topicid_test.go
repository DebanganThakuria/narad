package cluster

import (
	"context"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type zzWP6RecordingBroker struct {
	broker.Broker
	got []ingress.ProduceRecord
}

func (b *zzWP6RecordingBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	b.got = append(b.got, records...)
	return make([]int64, len(records)), nil
}

// The owner checks each record against the incarnation it was accepted
// under, so the commit-batch handler must hand it the record's topic ID
// from the wire, and none for a record from an older peer.
func TestZZWP6CommitBatchHandlerPassesTopicID(t *testing.T) {
	payload, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{
		{Topic: "orders", TopicID: "incarnation-2", Payload: []byte("a")},
		{Topic: "orders", Payload: []byte("b")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	br := &zzWP6RecordingBroker{}
	s := &RPCServer{broker: br}
	res := roundTripRPC(t, s, payload)
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Status)
	}
	if len(br.got) != 2 || br.got[0].TopicID != "incarnation-2" || br.got[1].TopicID != "" {
		t.Fatalf("owner saw %+v, want topic ids [incarnation-2, \"\"]", br.got)
	}
}
