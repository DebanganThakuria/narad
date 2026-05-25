package cluster

import (
	"context"
	"net/http"
	"sort"
	"strconv"
)

type consumePartitionCandidate struct {
	partition int
	addr      string
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
		addr := rt.ownerAddr(topicName, assignment.Partition)
		if addr == "" {
			continue
		}
		candidates = append(candidates, consumePartitionCandidate{
			partition: assignment.Partition,
			addr:      addr,
			backlog:   backlogByPartition[assignment.Partition],
			order:     i,
		})
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
			value := partitionSnapshot.LogEndOffset - partitionSnapshot.CommittedOffset
			if value < 0 {
				value = 0
			}
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
