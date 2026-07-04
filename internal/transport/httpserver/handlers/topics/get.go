package topics

import (
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Get handles GET /v1/topics/{topic}.
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
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
		if partition >= 0 {
			// Select by Index, not position: the merged slice carries one
			// entry per assignment and may not be positional mid-rebalance.
			stats, ok := partitionStatsByIndex(d.Partitions, partition)
			if !ok {
				s.WriteError(w, http.StatusInternalServerError, "partition stats unavailable")
				return
			}
			d.Partitions = []topic.PartitionStats{stats}
		}
		s.WriteJSON(w, http.StatusOK, d)
	}
}

// partitionStatsByIndex returns the stats entry whose Index matches
// partition. The local GetTopicDetails slice is positional, but the
// router-merged slice carries one entry per assignment, so a linear
// scan by Index is the only ordering both share.
func partitionStatsByIndex(stats []topic.PartitionStats, partition int) (topic.PartitionStats, bool) {
	for _, ps := range stats {
		if ps.Index == partition {
			return ps, true
		}
	}
	return topic.PartitionStats{}, false
}
