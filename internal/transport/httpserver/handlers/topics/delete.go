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
//
// The metadata delete is the commit point. Once it stands the topic is
// gone for every client, so a failure purging files afterwards, local
// or on a remote member, is logged and answered 204 rather than 5xx:
// a 5xx would make the client retry a delete that already happened and
// get a 404, and the leftover directories are reclaimed by the owning
// node's startup orphan sweep either way. The forwarded (RPC) path has
// treated purge failures this way from the start; this keeps the
// leader-direct path consistent.
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if !s.AuthorizeTopicManage(w, r, topicName) {
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDeleteTopic(r.Context(), w, r, topicName) {
				return
			}
		}
		if err := s.Deps.Broker.DeleteTopic(r.Context(), topicName); err != nil {
			purgeErr, ok := errors.AsType[brokertopics.PurgeError](err)
			if !ok {
				s.WriteBrokerError(w, "delete topic", err)
				return
			}
			s.Deps.Logger.Warn("topic deleted but local purge failed; the startup orphan sweep reclaims the directory",
				"topic", topicName, "err", purgeErr.Err)
		}
		if s.Deps.Router != nil {
			if err := s.Deps.Router.BroadcastDeleteTopic(r.Context(), topicName); err != nil {
				s.Deps.Logger.Warn("topic deleted but purge broadcast failed on some members; their startup orphan sweep reclaims the directories",
					"topic", topicName, "err", err)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
