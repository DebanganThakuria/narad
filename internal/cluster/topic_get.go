package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// RouteGetTopic merges per-partition stats from every partition owner into
// details: locally-owned partitions come from the details the caller already
// computed, remote ones are fetched from their owners. Any partition whose
// owner cannot be resolved fails the whole call with ErrNotPartitionOwner —
// partial stats would silently under-report the topic.
func (rt *Router) RouteGetTopic(ctx context.Context, r *http.Request, topicName string, details topic.Details) (topic.Details, error) {
	assignments, err := rt.store.ListAssignments(topicName)
	if err != nil {
		return topic.Details{}, err
	}
	if len(assignments) == 0 {
		return details, nil
	}

	localByPartition := make(map[int]topic.PartitionStats, len(details.Partitions))
	for _, partitionStats := range details.Partitions {
		localByPartition[partitionStats.Index] = partitionStats
	}

	merged := make([]topic.PartitionStats, 0, len(assignments))
	for _, assignment := range assignments {
		addr := rt.ownerAddr(topicName, assignment.Partition)
		if assignment.OwnerID == rt.selfID {
			partitionStats, ok := localByPartition[assignment.Partition]
			if !ok {
				return topic.Details{}, errs.ErrNotPartitionOwner
			}
			partitionStats.OwnerNode = assignment.OwnerID
			merged = append(merged, partitionStats)
			continue
		}
		if addr == "" {
			return topic.Details{}, errs.ErrNotPartitionOwner
		}
		partitionStats, err := rt.fetchTopicPartitionStats(ctx, topicName, addr, assignment.Partition)
		if err != nil {
			return topic.Details{}, err
		}
		partitionStats.OwnerNode = assignment.OwnerID
		merged = append(merged, partitionStats)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Index < merged[j].Index
	})
	details.Partitions = merged
	return details, nil
}

func (rt *Router) fetchTopicPartitionStats(ctx context.Context, topicName, addr string, partition int) (topic.PartitionStats, error) {
	stats, err := rt.peer.TopicPartitionStats(ctx, addr, topicName, partition)
	if err != nil {
		return topic.PartitionStats{}, fmt.Errorf("topic get: %w", err)
	}
	if stats.Index != partition {
		return topic.PartitionStats{}, fmt.Errorf("topic get returned partition %d, want %d", stats.Index, partition)
	}
	return stats, nil
}
