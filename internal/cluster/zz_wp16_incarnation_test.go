package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP16MismatchBroker refuses every commit as an owner whose replica
// holds another incarnation of the topic does.
type zzWP16MismatchBroker struct {
	broker.Broker
}

func (zzWP16MismatchBroker) CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error) {
	return nil, fmt.Errorf("%w: orders is incarnation %q here, the records were accepted for %q",
		brokermsg.ErrTopicIncarnationMismatch, "incarnation-1", "incarnation-2")
}

// zzWP16LevelRecorder is a slog handler that records the level of every
// line.
type zzWP16LevelRecorder struct {
	mu     sync.Mutex
	levels []slog.Level
}

func (r *zzWP16LevelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *zzWP16LevelRecorder) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *zzWP16LevelRecorder) WithGroup(string) slog.Handler            { return r }
func (r *zzWP16LevelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.levels = append(r.levels, rec.Level)
	r.mu.Unlock()
	return nil
}

// An owner refusing records accepted for another incarnation of the
// topic is an expected, retriable outcome of a delete and recreate, not
// an internal failure: it answers with its own status, which the
// dispatcher recognizes, and does not log at error level. It used to be
// an opaque 500 with an error line per refused commit.
func TestZZWP16IncarnationMismatchIsAPreconditionFailure(t *testing.T) {
	payload, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{
		{Topic: "orders", TopicID: "incarnation-2", Payload: []byte("a")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rec := &zzWP16LevelRecorder{}
	res := roundTripRPC(t, &RPCServer{broker: zzWP16MismatchBroker{}, logger: slog.New(rec)}, payload)
	if res.Status != http.StatusPreconditionFailed {
		t.Fatalf("status = %d (%s), want 412", res.Status, res.Body)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, level := range rec.levels {
		if level >= slog.LevelError {
			t.Fatalf("the refusal was logged at %s", level)
		}
	}
}

// A remote owner whose replica still holds the old incarnation refuses
// records accepted for the new one until it catches up. That says
// nothing about the owner's health: the records stay on their own
// partition and are checked against the current incarnation again, and
// commit there once the owner accepts them. They used to count as a
// failing owner, so past the reroute grace they were moved to a sibling
// partition.
func TestZZWP16OwnerIncarnationMismatchIsRetriedInPlace(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%v", local), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			zzWP6RegisterMembers(t, store)
			owner := "node-remote"
			if local {
				owner = "node-self"
			}
			zzWP6SeedIncarnation(t, store, "incarnation-2", owner, "node-self")
			manager := newDispatchIngressManager(t)
			for range 2 {
				if _, err := manager.AcceptProduceWithTopicID(ctx, "orders", "incarnation-2", "k", 0, []byte(`{"id":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			const refusals = 2
			var mu sync.Mutex
			var attempts int
			var landed []int // partitions of committed records
			refuse := func() bool {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				return attempts <= refusals
			}
			peer := fakePeerClient{commitProduceBatchFn: func(_ context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
				if refuse() {
					return nodewire.Response{Status: http.StatusPreconditionFailed, Body: []byte(`{"error":"topic incarnation mismatch"}` + "\n")}, nil
				}
				mu.Lock()
				defer mu.Unlock()
				for _, r := range req.Records {
					landed = append(landed, r.TargetPartition)
				}
				return nodewire.Response{Status: http.StatusOK}, nil
			}}
			committer := &zzWP16MismatchCommitter{refuse: refuse, landed: func(records []ingress.ProduceRecord) {
				mu.Lock()
				defer mu.Unlock()
				for _, r := range records {
					landed = append(landed, r.TargetPartition)
				}
			}}
			d := NewProduceDispatcher(manager, store, "node-self", committer, peer, nil, ProduceDispatcherConfig{})
			now := time.Unix(1_000_000, 0)
			d.now = func() time.Time { return now }
			var passErrs []error
			for range refusals + 1 {
				if _, err := d.DispatchAvailable(ctx); err != nil {
					passErrs = append(passErrs, err)
				}
				// Well past the reroute grace between passes.
				now = now.Add(2 * produceDispatchRerouteGrace)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(landed) != 2 || landed[0] != 0 || landed[1] != 0 {
				t.Fatalf("records committed into partitions %v, want both into 0 (their own)", landed)
			}
			if attempts != refusals+1 {
				t.Fatalf("%d commit attempts to partition 0, want %d", attempts, refusals+1)
			}
			if next, _ := manager.LoadProduceCheckpoint(); next != 2 {
				t.Fatalf("checkpoint = %d, want 2", next)
			}
			if len(passErrs) > 0 {
				t.Fatalf("passes reported %v, want none: a refusal for another incarnation is retried, not failed", passErrs)
			}
		})
	}
}

// zzWP16MismatchCommitter is this node's broker for the dispatcher: it
// refuses a commit to partition 0 while refuse says so, as an owner
// whose replica holds another incarnation, and records every commit.
type zzWP16MismatchCommitter struct {
	refuse func() bool
	landed func([]ingress.ProduceRecord)
}

func (c *zzWP16MismatchCommitter) CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (int64, error) {
	offsets, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{record})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (c *zzWP16MismatchCommitter) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	if records[0].TargetPartition == 0 && c.refuse() {
		return nil, fmt.Errorf("%w: orders is incarnation %q here, the records were accepted for %q",
			brokermsg.ErrTopicIncarnationMismatch, "incarnation-1", records[0].TopicID)
	}
	c.landed(records)
	return make([]int64, len(records)), nil
}
