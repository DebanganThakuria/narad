package topics

import (
	"errors"
	"net/http"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Delete handles DELETE /v1/topics/{topic}. The request is forwarded to
// the leader, which deletes topic metadata locally and then asks other
// nodes to purge their local runtime and disk state for the topic.
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDeleteTopic(r.Context(), w, r, topicName) {
				return
			}
		}
		if err := s.Deps.Broker.DeleteTopic(r.Context(), topicName); err != nil {
			purgeErr, isPurge := errors.AsType[brokertopics.PurgeError](err)
			if !isPurge || s.Deps.Router == nil {
				s.WriteBrokerError(w, "delete topic", err)
				return
			}
			// The metastore record is already gone; only this node's local
			// file purge failed (the orphan is reclaimed by the startup
			// sweep). Still broadcast so the partition owners purge their
			// copies; if that succeeds the topic is deleted everywhere that
			// matters, so report success rather than a misleading 500.
			if broadcastErr := s.Deps.Router.BroadcastDeleteTopic(r.Context(), topicName); broadcastErr != nil {
				s.WriteBrokerError(w, "broadcast delete topic", broadcastErr)
				return
			}
			s.Deps.Logger.Warn("topic metadata deleted but local purge failed; orphan files left for startup sweep",
				"topic", topicName, "err", purgeErr)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if s.Deps.Router != nil {
			if err := s.Deps.Router.BroadcastDeleteTopic(r.Context(), topicName); err != nil {
				s.WriteBrokerError(w, "broadcast delete topic", err)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
