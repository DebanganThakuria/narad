package cluster

// dispatchDestKey identifies one destination partition by (topic,
// partition), independent of its owner's address, so a destination's
// queue and failure state carry across owner-address changes.
type dispatchDestKey struct {
	topic     string
	partition int
}

// rerouteFor picks the sibling partition that takes the records of a
// destination whose owner cannot take commits, or reports that there is
// none. The answer is memoized for the current read: records of one
// destination all go the same way, and the first pass after an owner
// dies can read a full window of them. A memoized answer can name a
// partition that started failing later in the same read; its commit
// then fails and its records move on from there.
func (d *ProduceDispatcher) rerouteFor(st *produceDispatchState, key dispatchDestKey) (*dispatchDest, bool) {
	if st.rerouteEpoch != st.readEpoch || st.rerouteMemo == nil {
		st.rerouteMemo = map[dispatchDestKey]int{}
		st.rerouteEpoch = st.readEpoch
	}
	partition, ok := st.rerouteMemo[key]
	if !ok {
		partition = -1
		if target, found := d.rerouteTarget(st, key.topic, key.partition); found {
			partition = target.partition
		}
		st.rerouteMemo[key] = partition
	}
	if partition < 0 {
		return nil, false
	}
	return st.dest(dispatchDestKey{topic: key.topic, partition: partition}), true
}

// rerouteTarget picks a live-owner partition of the same topic to stand in
// for a partition whose owner cannot take commits. It mirrors the
// accept-time dead-owner skip (messaging.Engine.pickProducePartition,
// exercised by TestProduceSkipsDeadOwnerPartition): walk the topic's
// partitions circularly starting just past fromPartition and return the
// first whose owner resolves as alive — reusing dispatchTargetsForTopic's
// membership-backed liveness — and whose commits are neither failing nor
// hanging.
// Returns false when no such partition exists (single-partition topic, or
// every other owner dead or failing); the caller then keeps a probe on the
// original partition and the checkpoint waits for it.
func (d *ProduceDispatcher) rerouteTarget(st *produceDispatchState, topicName string, fromPartition int) (produceDispatchTarget, bool) {
	if d.store == nil {
		return produceDispatchTarget{}, false
	}
	info := d.topicInfo(st, topicName)
	if !info.exists || info.partitions <= 1 {
		return produceDispatchTarget{}, false
	}
	targets, err := d.dispatchTargetsForTopic(topicName)
	if err != nil {
		return produceDispatchTarget{}, false
	}
	for i := 1; i < info.partitions; i++ {
		candidate := (fromPartition + i) % info.partitions
		cached, ok := targets.byPartition[candidate]
		if !ok || cached.err != nil {
			continue
		}
		if dest, tracked := st.dests[dispatchDestKey{topic: topicName, partition: candidate}]; tracked && (dest.failing() || dest.unresolved || dest.slow) {
			continue
		}
		return cached.target, true
	}
	return produceDispatchTarget{}, false
}
