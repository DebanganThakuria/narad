package topics

import (
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Get handles GET /v1/topics/{topic}. The caller needs any grant on the
// topic (or ownership, or admin): the response carries per-partition
// sizes, high watermarks and owner nodes.
//
// Every partition carries a status. In a cluster the stats of remote
// partitions come from their owners, and when some owners are down the
// answer is still a 200 (audit M15): the partitions that could be read
// carry their stats and status "ok", every other one a zero placeholder
// with status "owner_unavailable" and its owner's liveness, and the
// body carries "partial": true. Only a failure to read the cluster's
// own metadata is an error (a 503).
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if !s.AuthorizeTopicRead(w, r, topicName) {
			return
		}
		partitionQuery := r.URL.Query().Get("partition")
		d, err := s.Deps.Broker.GetTopicDetails(r.Context(), topicName)
		if err != nil {
			s.WriteBrokerError(w, "get topic", err)
			return
		}
		// Validate the partition query against the local view before the
		// cluster merge: GetTopicDetails always returns exactly
		// Topic.Partitions positional entries, so this preserves the
		// pre-merge 400 semantics for out-of-range indices.
		partition := -1
		if partitionQuery != "" {
			p, err := strconv.Atoi(partitionQuery)
			if err != nil || p < 0 || p >= len(d.Partitions) {
				s.WriteError(w, http.StatusBadRequest, "invalid partition")
				return
			}
			partition = p
		}
		// Merge owner stats before any slicing: a ?partition= query
		// landing on a non-owner node must report the owner's stats, not
		// the zero-valued local placeholder. Without a router (single-node
		// mode) the local details are already complete.
		if s.Deps.Router != nil {
			d, err = s.Deps.Router.RouteGetTopic(r.Context(), r, topicName, d)
			if err != nil {
				s.WriteBrokerError(w, "get topic", err)
				return
			}
		}
		// A node's own describe is complete: every partition it reports
		// is real.
		for i := range d.Partitions {
			if d.Partitions[i].Status == "" {
				d.Partitions[i].Status = topic.PartitionStatusOK
			}
		}
		if partition >= 0 {
			// Select by Index, not position, which every source shares.
			stats, ok := partitionStatsByIndex(d.Partitions, partition)
			if !ok {
				s.WriteError(w, http.StatusInternalServerError, "partition stats unavailable")
				return
			}
			d.Partitions = []topic.PartitionStats{stats}
			d.Partial = stats.Status != topic.PartitionStatusOK
		}
		s.WriteJSON(w, http.StatusOK, d)
	}
}

// partitionStatsByIndex returns the stats entry whose Index matches
// partition. Both the local GetTopicDetails slice and the router-merged
// one are positional, but a linear scan by Index does not depend on it.
func partitionStatsByIndex(stats []topic.PartitionStats, partition int) (topic.PartitionStats, bool) {
	for _, ps := range stats {
		if ps.Index == partition {
			return ps, true
		}
	}
	return topic.PartitionStats{}, false
}
