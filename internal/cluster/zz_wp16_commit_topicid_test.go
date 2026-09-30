package cluster

import (
	"context"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP16RecordingBroker records what the commit handlers hand the
// owner.
type zzWP16RecordingBroker struct {
	broker.Broker
	got []ingress.ProduceRecord
}

func (b *zzWP16RecordingBroker) CommitAcceptedProduce(_ context.Context, record ingress.ProduceRecord) (int64, error) {
	b.got = append(b.got, record)
	return 0, nil
}

// produce-accept#0: the single-record commit op is kept for peers that
// send it, and the owner checks a record against the incarnation it was
// accepted under whichever op carried it, so the handler must hand the
// owner the record's topic ID from the wire.
func TestZZWP16CommitHandlerPassesTopicID(t *testing.T) {
	for _, id := range []string{"incarnation-2", ""} {
		payload, err := nodewire.EncodeCommitProduceRequest(nodewire.CommitProduceRequest{Topic: "orders", TopicID: id, Payload: []byte("a")})
		if err != nil {
			t.Fatal(err)
		}
		br := &zzWP16RecordingBroker{}
		res := roundTripRPC(t, &RPCServer{broker: br}, payload)
		if res.Status != http.StatusOK {
			t.Fatalf("status = %d, want 200", res.Status)
		}
		if len(br.got) != 1 || br.got[0].TopicID != id {
			t.Fatalf("owner saw %+v, want one record with topic id %q", br.got, id)
		}
	}
}
