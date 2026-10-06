package cluster

import "net/http"

// markForwardUndecided tells a writer that records outcomes (the HTTP
// handlers' audit writer) that the answer about to be written carries
// no decision of the leader's: the forward may have been applied there
// before its reply was lost, or the client went away while the leader
// ran it. The mutation is then audited as unknown rather than failed.
func markForwardUndecided(w http.ResponseWriter) {
	if m, ok := w.(interface{ MarkUndecided() }); ok {
		m.MarkUndecided()
	}
}
