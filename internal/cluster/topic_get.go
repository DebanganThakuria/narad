package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

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
			merged = append(merged, partitionStats)
			continue
		}
		if addr == "" {
			return topic.Details{}, errs.ErrNotPartitionOwner
		}
		if partitionStats, ok := localByPartition[assignment.Partition]; ok && assignment.OwnerID == rt.selfID {
			merged = append(merged, partitionStats)
			continue
		}
		partitionStats, err := rt.fetchTopicPartitionStats(ctx, r, addr, assignment.Partition)
		if err != nil {
			return topic.Details{}, err
		}
		merged = append(merged, partitionStats)
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Index < merged[j].Index
	})
	details.Partitions = merged
	return details, nil
}

func (rt *Router) fetchTopicPartitionStats(ctx context.Context, r *http.Request, addr string, partition int) (topic.PartitionStats, error) {
	fwd := r.Clone(ctx)
	q := fwd.URL.Query()
	q.Set("partition", strconv.Itoa(partition))
	fwd.URL.RawQuery = q.Encode()
	probe := rt.forwardProbe(fwd, addr, nil)
	if probe.code < http.StatusOK || probe.code >= http.StatusMultipleChoices {
		return topic.PartitionStats{}, fmt.Errorf("topic get returned status %d", probe.code)
	}
	var details topic.Details
	if err := json.Unmarshal(probe.body, &details); err != nil {
		return topic.PartitionStats{}, err
	}
	if len(details.Partitions) != 1 {
		return topic.PartitionStats{}, fmt.Errorf("topic get returned %d partitions, want 1", len(details.Partitions))
	}
	if details.Partitions[0].Index != partition {
		return topic.PartitionStats{}, fmt.Errorf("topic get returned partition %d, want %d", details.Partitions[0].Index, partition)
	}
	return details.Partitions[0], nil
}
