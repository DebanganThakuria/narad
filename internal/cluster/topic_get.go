package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// topicStatsConcurrency bounds how many remote partition stats RPCs a
// single topic GET keeps in flight. A 108-partition topic spread over
// a few nodes used to cost one sequential round trip per remote
// partition; now it costs a handful of concurrent rounds.
const topicStatsConcurrency = 16

// RouteGetTopic merges per-partition stats from every partition owner into
// details: locally-owned partitions come from the details the caller already
// computed, remote ones are fetched from their owners, concurrently. Any
// partition whose owner cannot be resolved fails the whole call with
// ErrNotPartitionOwner; partial stats would silently under-report the topic.
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

	// Resolve every owner before issuing any RPC, so an unresolvable
	// partition fails fast without a fan-out of wasted calls.
	merged := make([]topic.PartitionStats, len(assignments))
	remoteAddr := make([]string, len(assignments))
	for i, assignment := range assignments {
		if assignment.OwnerID == rt.selfID {
			partitionStats, ok := localByPartition[assignment.Partition]
			if !ok {
				return topic.Details{}, errs.ErrNotPartitionOwner
			}
			partitionStats.OwnerNode = assignment.OwnerID
			merged[i] = partitionStats
			continue
		}
		addr := rt.ownerAddr(topicName, assignment.Partition)
		if addr == "" {
			return topic.Details{}, errs.ErrNotPartitionOwner
		}
		remoteAddr[i] = addr
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(topicStatsConcurrency)
	for i, assignment := range assignments {
		if remoteAddr[i] == "" {
			continue
		}
		g.Go(func() error {
			partitionStats, err := rt.fetchTopicPartitionStats(gctx, topicName, remoteAddr[i], assignment.Partition)
			if err != nil {
				return err
			}
			partitionStats.OwnerNode = assignment.OwnerID
			merged[i] = partitionStats
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return topic.Details{}, err
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
