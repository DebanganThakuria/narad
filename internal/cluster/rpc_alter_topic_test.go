package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// laggingCapsBroker answers GetTopic from a replica that has not applied
// the last cap change yet, while UpdateTopicCaps works on the record as
// the Manager reads it under the topic lock after the leader barrier.
type laggingCapsBroker struct {
	broker.Broker
	stale, stored topic.Topic
}

func (b *laggingCapsBroker) GetTopic(context.Context, string) (topic.Topic, error) {
	return b.stale, nil
}

func (b *laggingCapsBroker) UpdateTopicCaps(_ context.Context, _ string, inFlight, ackedAhead *int64) (topic.Topic, error) {
	if inFlight != nil {
		b.stored.MaxInFlightPerPartition = *inFlight
	}
	if ackedAhead != nil {
		b.stored.MaxAckedAheadPerPartition = *ackedAhead
	}
	return b.stored, nil
}

// A forwarded alter that sets one cap leaves the other to the record the
// Manager reads under the topic lock. Carrying it forward from the
// leader's own unlocked read wrote a stale value back: a PATCH that
// reached a just-elected leader (also through a 3.0.1 follower, which
// forwards the body as the client sent it) undid the previous leader's
// committed cap change.
func TestForwardedAlterOfOneCapKeepsTheOtherCapCommittedBefore(t *testing.T) {
	br := &laggingCapsBroker{
		stale:  topic.Topic{Name: "orders", MaxInFlightPerPartition: 1000, MaxAckedAheadPerPartition: 1000},
		stored: topic.Topic{Name: "orders", MaxInFlightPerPartition: 1000, MaxAckedAheadPerPartition: 10},
	}
	s := NewRPCServer(br, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	payload, err := nodewire.EncodeTopicBodyRequest(nodewire.OpAlterTopic, nodewire.TopicBodyRequest{Topic: "orders", Body: []byte(`{"max_in_flight_per_partition":50}`)})
	if err != nil {
		t.Fatal(err)
	}
	if res := s.handleAlterTopic(payload); res.Status != http.StatusOK {
		t.Fatalf("forwarded alter: status %d body %s", res.Status, res.Body)
	}
	if br.stored.MaxInFlightPerPartition != 50 || br.stored.MaxAckedAheadPerPartition != 10 {
		t.Fatalf("caps after the forwarded alter = in-flight %d, acked-ahead %d; want 50 and the committed 10",
			br.stored.MaxInFlightPerPartition, br.stored.MaxAckedAheadPerPartition)
	}
}

// A forwarded topic write the leader refuses because the topic changed
// under it (deleted and recreated, or grown, after the check) answers
// 409 with the reason, as the HTTP layer does, not an opaque 500.
func TestTopicChangedIsAConflictOverRPC(t *testing.T) {
	s := &RPCServer{}
	status, msg := s.brokerErrorStatus("alter topic", fmt.Errorf("%w: topic \"orders\" was recreated", errs.ErrTopicChanged))
	if status != http.StatusConflict || !strings.Contains(msg, "was recreated") {
		t.Fatalf("brokerErrorStatus(topic changed) = %d %q, want 409 with the reason", status, msg)
	}
}
