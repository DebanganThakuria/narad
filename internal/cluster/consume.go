package cluster

import (
	"context"
	"net/http"
	"sort"
	"strconv"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type consumePartitionCandidate struct {
	partition int
	addr      string
	local     bool
	backlog   int64
	order     int
}

func (rt *Router) consumePartitionCandidates(ctx context.Context, topicName string) []consumePartitionCandidate {
	assignments, err := rt.store.ListAssignments(topicName)
	if err != nil || len(assignments) == 0 {
		return nil
	}

	backlogByPartition := rt.backlogByPartition(ctx, topicName)
	candidates := make([]consumePartitionCandidate, 0, len(assignments))
	for i, assignment := range assignments {
		candidate, ok := rt.consumePartitionCandidate(assignment, backlogByPartition, i)
		if !ok {
			continue
		}
		candidates = append(candidates, candidate)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].backlog != candidates[j].backlog {
			return candidates[i].backlog > candidates[j].backlog
		}
		if candidates[i].partition != candidates[j].partition {
			return candidates[i].partition < candidates[j].partition
		}
		return candidates[i].order < candidates[j].order
	})
	return candidates
}

func (rt *Router) consumePartitionCandidate(assignment metastore.Assignment, backlogByPartition map[int]int64, order int) (consumePartitionCandidate, bool) {
	if assignment.OwnerID == rt.selfID {
		return consumePartitionCandidate{
			partition: assignment.Partition,
			local:     true,
			backlog:   backlogByPartition[assignment.Partition],
			order:     order,
		}, true
	}
	addr := rt.ownerAddr(assignment.Topic, assignment.Partition)
	if addr == "" {
		return consumePartitionCandidate{}, false
	}
	return consumePartitionCandidate{
		partition: assignment.Partition,
		addr:      addr,
		backlog:   backlogByPartition[assignment.Partition],
		order:     order,
	}, true
}

func (rt *Router) backlogByPartition(ctx context.Context, topicName string) map[int]int64 {
	if rt.snapshots == nil {
		return nil
	}
	snapshots, err := rt.snapshots.Snapshot(ctx)
	if err != nil {
		return nil
	}
	backlog := make(map[int]int64)
	for _, snapshot := range snapshots {
		if snapshot.Topic != topicName {
			continue
		}
		for _, partitionSnapshot := range snapshot.Partitions {
			value := max(partitionSnapshot.LogEndOffset-partitionSnapshot.CommittedOffset, 0)
			backlog[partitionSnapshot.Partition] = value
		}
		break
	}
	return backlog
}

func (rt *Router) forwardConsumeProbe(ctx context.Context, w http.ResponseWriter, r *http.Request, partition int, addr string) (bool, bool) {
	fwd := r.Clone(ctx)
	q := fwd.URL.Query()
	q.Set("partition", strconv.Itoa(partition))
	q.Set("wait", "0s")
	fwd.URL.RawQuery = q.Encode()
	probe := rt.forwardProbe(fwd, addr, nil)
	if probe.code == http.StatusNoContent {
		return false, false
	}
	copyHeader(w.Header(), probe.header)
	w.WriteHeader(probe.code)
	if len(probe.body) > 0 {
		_, _ = w.Write(probe.body)
	}
	return true, true
}
