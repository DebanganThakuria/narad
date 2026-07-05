package cluster

import (
	"context"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// produceDispatchStuckKey identifies a destination partition independently of
// where its owner currently lives, so a partition stays recognisably "stuck"
// across owner-address changes.
type produceDispatchStuckKey struct {
	topic     string
	partition int
}

// rerouteFailedBuckets handles destinations whose commit just failed even
// though their owner still resolves per membership. Every such destination
// is marked stuck with an incremented consecutive-pass count. A destination
// still within its produceDispatchRerouteAfterPasses grace keeps its records
// uncommitted so they retry on the original partition next pass (a transient
// blip must not scatter records across partitions). Beyond the grace the
// destination is treated as dead: its failed records are rerouted to a
// live-owner partition of the same topic and marked done on success, so the
// checkpoint keeps advancing. The failed commit attempt that got us here
// doubles as the recovery probe — one success and the destination leaves the
// stuck set, sending new records back to their original partition.
func (d *ProduceDispatcher) rerouteFailedBuckets(ctx context.Context, failed map[produceDispatchTarget][]ingress.ProduceRecord, prevStuck, nextStuck map[produceDispatchStuckKey]int, done map[uint64]bool) {
	for target, recs := range failed {
		key := produceDispatchStuckKey{topic: target.topic, partition: target.partition}
		nextStuck[key] = prevStuck[key] + 1
		if prevStuck[key] < produceDispatchRerouteAfterPasses {
			continue
		}
		alt, ok := d.rerouteTarget(target.topic, target.partition, prevStuck, nextStuck)
		if !ok {
			continue
		}
		rerouted := make([]ingress.ProduceRecord, len(recs))
		for i, rec := range recs {
			rec.TargetPartition = alt.partition
			rerouted[i] = rec
		}
		err := d.commitBatch(ctx, alt, rerouted)
		if err != nil {
			altKey := produceDispatchStuckKey{topic: alt.topic, partition: alt.partition}
			if nextStuck[altKey] == 0 {
				nextStuck[altKey] = prevStuck[altKey] + 1
			}
			continue
		}
		for _, rec := range recs {
			done[rec.WAL.Seq] = true
		}
		d.logger.Warn("rerouting produce records for stuck partition owner",
			"topic", target.topic, "from_partition", target.partition,
			"to_partition", alt.partition, "records", len(rerouted))
	}
}

// rerouteTarget picks a live-owner partition of the same topic to stand in
// for a partition whose owner cannot take commits. It mirrors the
// accept-time dead-owner skip (messaging.Engine.pickProducePartition,
// exercised by TestProduceSkipsDeadOwnerPartition): walk the topic's
// partitions circularly starting just past fromPartition and return the
// first whose owner resolves as alive — reusing dispatchTargetsForTopic's
// membership-backed liveness — and that is not itself a stuck destination.
// Returns false when no such partition exists (single-partition topic, or
// every other owner dead/stuck); the caller then falls back to the pinned
// checkpoint + probe + lookahead-horizon behavior.
func (d *ProduceDispatcher) rerouteTarget(topicName string, fromPartition int, prevStuck, curStuck map[produceDispatchStuckKey]int) (produceDispatchTarget, bool) {
	if d.store == nil {
		return produceDispatchTarget{}, false
	}
	t, err := d.store.GetTopic(context.Background(), topicName)
	if err != nil || t.Partitions <= 1 {
		return produceDispatchTarget{}, false
	}
	targets, err := d.dispatchTargetsForTopic(topicName)
	if err != nil {
		return produceDispatchTarget{}, false
	}
	for i := 1; i < t.Partitions; i++ {
		candidate := (fromPartition + i) % t.Partitions
		cached, ok := targets.byPartition[candidate]
		if !ok || cached.err != nil {
			continue
		}
		key := produceDispatchStuckKey{topic: topicName, partition: candidate}
		if prevStuck[key] > 0 || curStuck[key] > 0 {
			continue
		}
		return cached.target, true
	}
	return produceDispatchTarget{}, false
}
