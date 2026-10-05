package messaging

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// drainingProduceMessage is the 503 a node being decommissioned answers
// to a client produce.
const drainingProduceMessage = "this node is being decommissioned and takes no new produce; send it to another node"

// refuseWhileDraining answers a client produce with 503 and Retry-After
// while this node is being decommissioned, and reports whether it did.
// A draining node's ingress WAL must empty before the node leaves Raft,
// and every record accepted here is one more to hand off. Only client
// produce is refused: owner-side commits and produce forwarded by a peer
// router still run (refusing a forwarded keyed record would reroute it).
func refuseWhileDraining(s *handlers.Set, w http.ResponseWriter) bool {
	if s.Deps.Draining == nil || !s.Deps.Draining() {
		return false
	}
	w.Header().Set("Retry-After", "1")
	s.WriteError(w, http.StatusServiceUnavailable, drainingProduceMessage)
	return true
}
