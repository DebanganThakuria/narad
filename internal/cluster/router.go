// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

// Router forwards HTTP requests to the pod that owns the target partition.
// All read methods hit the local bbolt replica (fast, ms-stale).
type Router struct {
	store      *metastore.Store
	selfID     string
	partitions partition.Manager
}

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager) *Router {
	return &Router{store: store, selfID: selfID, partitions: mgr}
}

// RouteProduce forwards a produce request to the owner of the partition
// the key hashes to. body is the already-read request body bytes.
// Returns true if the request was forwarded (caller must return).
func (rt *Router) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	t, err := rt.store.GetTopic(ctx, topicName)
	if err != nil {
		return false
	}
	p := rt.partitions.Pick(topicName, key, t.Partitions)
	addr := rt.ownerAddr(topicName, p)
	if addr == "" {
		return false
	}
	rt.forward(w, r, addr, body)
	return true
}

// RouteConsume forwards a consume request to the owner of a partition.
// pinnedPartition is set when the caller already chose a partition (replay
// or pinned consume); nil causes the router to pick one at random.
// Returns true if forwarded.
func (rt *Router) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) bool {
	var p int
	if pinnedPartition != nil {
		p = *pinnedPartition
	} else {
		t, err := rt.store.GetTopic(ctx, topicName)
		if err != nil || t.Partitions == 0 {
			return false
		}
		p = rand.Intn(t.Partitions)
	}

	addr := rt.ownerAddr(topicName, p)
	if addr == "" {
		return false
	}

	// Pin the partition in the forwarded URL so the remote pod only
	// consumes from the partition it owns.
	fwd := r.Clone(ctx)
	q := fwd.URL.Query()
	q.Set("partition", strconv.Itoa(p))
	fwd.URL.RawQuery = q.Encode()

	rt.forward(w, fwd, addr, nil)
	return true
}

// RouteAck forwards an ack request to the owner of the given partition.
// body is the already-read request body bytes.
// Returns true if forwarded.
func (rt *Router) RouteAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, partition int, body []byte) bool {
	addr := rt.ownerAddr(topicName, partition)
	if addr == "" {
		return false
	}
	rt.forward(w, r, addr, body)
	return true
}

// ownerAddr returns the API address of the pod that owns (topicName, partition),
// or "" if this pod is the owner or the assignment/member cannot be resolved.
func (rt *Router) ownerAddr(topicName string, p int) string {
	a, err := rt.store.GetAssignment(topicName, p)
	if err != nil || a.OwnerID == rt.selfID {
		return ""
	}
	m, err := rt.store.GetMember(a.OwnerID)
	if err != nil || m.Status == metastore.MemberDead {
		return ""
	}
	return m.Addr
}

// forward proxies r to http://addr, optionally replacing the body.
func (rt *Router) forward(w http.ResponseWriter, r *http.Request, addr string, body []byte) {
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	target, _ := url.Parse("http://" + addr)
	httputil.NewSingleHostReverseProxy(target).ServeHTTP(w, r)
}
