package messaging

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// drainingProduceMessage is the 503 a node being decommissioned answers
// to a client produce.
const drainingProduceMessage = "this node is being decommissioned and takes no new produce; send it to another node"

// admitProduce admits a client produce through the node's drain gate,
// or answers it with 503 and Retry-After while this node is being
// decommissioned and reports false. A request it admits must call
// s.Deps.Drain.Done once answered. A draining node's ingress WAL must
// empty before the node leaves Raft, and every record accepted here is
// one more to hand off; the gate's count of admitted requests is how the
// node tells that none is still on its way in. Only client produce is
// gated: owner-side commits and produce forwarded by a peer router still
// run (refusing a forwarded keyed record would reroute it), and neither
// writes this node's ingress WAL.
//
// Handlers call it after Authorize (which reads no body) and before they
// read the body: a principal that may not produce gets its 403, not a
// retryable 503 that tells it the node is draining.
func admitProduce(s *handlers.Set, w http.ResponseWriter) bool {
	if s.Deps.Drain.Admit() {
		return true
	}
	w.Header().Set("Retry-After", "1")
	s.WriteError(w, http.StatusServiceUnavailable, drainingProduceMessage)
	return false
}
