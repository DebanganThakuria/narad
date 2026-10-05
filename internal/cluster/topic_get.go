package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// topicStatsConcurrency bounds how many remote partition stats RPCs a
// single topic GET keeps in flight. A 108-partition topic spread over
// a few nodes used to cost one sequential round trip per remote
// partition; now it costs a handful of concurrent rounds.
const topicStatsConcurrency = 16

// topicStatsTimeout bounds one remote partition stats RPC of a topic
// GET. An owner that does not answer in time is reported unreachable
// for that partition instead of holding up, or failing, the answer.
const topicStatsTimeout = 2 * time.Second

// RouteGetTopic merges per-partition stats from every partition owner into
// details: locally-owned partitions come from the details the caller already
// computed, remote ones are fetched from their owners, concurrently.
//
// It reports what it can when some owners are down (audit M15): when a
// node dies nearly every multi-partition topic has a partition on it,
// and the operator needs the other partitions' stats exactly then. The
// result has exactly one entry per partition index in [0,
// Topic.Partitions), in index order, each stamped with its owner and a
// status. A partition whose stats could not be had keeps a placeholder
// with zero stats, status topic.PartitionOwnerUnavailable and the
// owner's liveness: dead (a member marked dead, not asked), unreachable
// (its stats RPC failed or took longer than topicStatsTimeout), unknown
// (no member with an address) or unassigned (no assignment row), and
// details.Partial is set. Assignment rows at or beyond the partition
// count (left by an earlier incarnation) are ignored. Only a failure to
// read this node's own metadata is an error, wrapping
// errs.ErrUnavailable (a 503).
func (rt *Router) RouteGetTopic(ctx context.Context, _ *http.Request, topicName string, details topic.Details) (topic.Details, error) {
	assignments, err := rt.store.ListAssignments(topicName)
	if err != nil {
		return topic.Details{}, fmt.Errorf("%w: read the topic's partition owners: %v", errs.ErrUnavailable, err)
	}
	members, err := rt.store.ListMembers()
	if err != nil {
		return topic.Details{}, fmt.Errorf("%w: read the cluster members: %v", errs.ErrUnavailable, err)
	}
	memberByID := make(map[string]metastore.Member, len(members))
	for _, member := range members {
		memberByID[strings.TrimSpace(member.ID)] = member
	}

	partitions := details.Topic.Partitions
	local := make(map[int]topic.PartitionStats, len(details.Partitions))
	for _, stats := range details.Partitions {
		local[stats.Index] = stats
	}
	owners := make([]string, partitions)
	assigned := make([]bool, partitions)
	for _, assignment := range assignments {
		if assignment.Partition < 0 || assignment.Partition >= partitions {
			continue
		}
		owners[assignment.Partition] = assignment.OwnerID
		assigned[assignment.Partition] = true
	}

	merged := make([]topic.PartitionStats, partitions)
	remoteAddr := make([]string, partitions)
	for i := range merged {
		ownerID := owners[i]
		var liveness string
		switch {
		case !assigned[i]:
			liveness = topic.OwnerUnassigned
		case ownerID == rt.selfID:
			stats, ok := local[i]
			if !ok {
				liveness = topic.OwnerUnknown
				break
			}
			stats.OwnerNode = ownerID
			stats.Status = topic.PartitionStatusOK
			merged[i] = stats
			continue
		default:
			member, known := memberByID[strings.TrimSpace(ownerID)]
			switch {
			case !known || strings.TrimSpace(member.Addr) == "":
				liveness = topic.OwnerUnknown
			case member.Status == metastore.MemberDead:
				liveness = topic.OwnerDead
			default:
				remoteAddr[i] = strings.TrimSpace(member.Addr)
				continue
			}
		}
		merged[i] = unavailablePartition(i, ownerID, liveness)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, topicStatsConcurrency)
	for i, addr := range remoteAddr {
		if addr == "" {
			continue
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			statsCtx, cancel := context.WithTimeout(ctx, topicStatsTimeout)
			defer cancel()
			stats, err := rt.fetchTopicPartitionStats(statsCtx, topicName, addr, i)
			if err != nil {
				merged[i] = unavailablePartition(i, owners[i], topic.OwnerUnreachable)
				return
			}
			stats.OwnerNode = owners[i]
			stats.Status = topic.PartitionStatusOK
			stats.OwnerLiveness = ""
			merged[i] = stats
		})
	}
	wg.Wait()

	details.Partitions = merged
	details.Partial = false
	for _, stats := range merged {
		if stats.Status != topic.PartitionStatusOK {
			details.Partial = true
			break
		}
	}
	return details, nil
}

// unavailablePartition is the placeholder of a partition whose stats
// could not be had: its index and owner, zero stats, and why.
func unavailablePartition(partition int, ownerID, liveness string) topic.PartitionStats {
	return topic.PartitionStats{
		Index:         partition,
		OwnerNode:     ownerID,
		Status:        topic.PartitionOwnerUnavailable,
		OwnerLiveness: liveness,
	}
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
