package handlers

import (
	"context"
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
)

// RemoteWriter runs a remote write on the leader (in process or
// forwarded). *cluster.RemotePlane implements it; handlers does not
// import cluster, so the interface is structural, like Router.
type RemoteWriter interface {
	RemoteWrite(ctx context.Context, req nodewire.RemoteWriteRequest) (nodewire.Response, error)
}

// RemoteDeps are the remote-replication collaborators of the handlers.
type RemoteDeps struct {
	Writer  RemoteWriter    // nil: every remote route answers 501
	Service *remote.Service // package A's ingress-side service; nil until wired
}

// headerNoStore marks an answer that must not be cached: every answer
// of the remotes API and of remote writes.
var headerNoStore = []string{"no-store"}

// SetNoStore sets Cache-Control: no-store on w.
func SetNoStore(w http.ResponseWriter) { w.Header()["Cache-Control"] = headerNoStore }

// WriteRemoteResponse writes a leader answer with Cache-Control: no-store.
func (s *Set) WriteRemoteResponse(w http.ResponseWriter, res nodewire.Response) {
	SetNoStore(w)
	status := res.Status
	if status == 0 {
		// Every leader answer carries its status; one without is broken.
		s.Deps.Logger.Error("remote write answer without a status")
		status = http.StatusInternalServerError
	}
	if len(res.Body) == 0 {
		w.WriteHeader(status)
		return
	}
	h := w.Header()
	if res.ContentType == nodewire.ContentTypeJSON || res.ContentType == "" {
		h["Content-Type"] = headerJSON
	} else {
		h.Set("Content-Type", res.ContentType)
	}
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	if _, err := w.Write(res.Body); err != nil {
		s.Deps.Logger.Error("write remote response", "err", err)
	}
}
