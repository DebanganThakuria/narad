// Package cluster provides the routing layer that proxies requests to the
// pod that owns the target partition.
package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

// Router forwards HTTP requests to the pod that owns the target partition.
// All read methods hit the local bbolt replica (fast, ms-stale).
type Router struct {
	store      *metastore.Store
	selfID     string
	partitions partition.Manager
	snapshots  metrics.SnapshotProvider
}

// NewRouter constructs a Router. selfID is this pod's member ID (os.Hostname()).
func NewRouter(store *metastore.Store, selfID string, mgr partition.Manager, snapshots metrics.SnapshotProvider) *Router {
	return &Router{store: store, selfID: selfID, partitions: mgr, snapshots: snapshots}
}

// RouteProduce forwards a produce request to the first alive partition owner
// starting from the key-hashed partition and walking forward circularly.
// body is the already-read request body bytes. Returns true if forwarded.
func (rt *Router) RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool {
	t, err := rt.store.GetTopic(ctx, topicName)
	if err != nil || t.Partitions == 0 {
		return false
	}
	start := rt.partitions.Pick(topicName, key, t.Partitions)
	// TODO if the partition owner node is down. Chose the next partition and write to it
	for i := 0; i < t.Partitions; i++ {
		p := (start + i) % t.Partitions
		addr := rt.ownerAddr(topicName, p)
		if addr == "" {
			continue
		}
		fwd := r.Clone(ctx)
		q := fwd.URL.Query()
		q.Set("partition", strconv.Itoa(p))
		fwd.URL.RawQuery = q.Encode()
		rt.forward(w, fwd, addr, body)
		return true
	}
	return false
}

// RouteConsume forwards a consume request to the owner of a partition.
// pinnedPartition is set when the caller already chose a partition (replay
// or pinned consume); nil causes the router to walk candidate partitions once
// with non-blocking probes.
// Returns true if forwarded.
func (rt *Router) RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int, _ time.Duration) bool {
	if pinnedPartition != nil {
		addr := rt.ownerAddr(topicName, *pinnedPartition)
		if addr == "" {
			return false
		}
		fwd := r.Clone(ctx)
		q := fwd.URL.Query()
		q.Set("partition", strconv.Itoa(*pinnedPartition))
		fwd.URL.RawQuery = q.Encode()
		rt.forward(w, fwd, addr, nil)
		return true
	}

	candidates := rt.consumePartitionCandidates(ctx, topicName)
	for _, candidate := range candidates {
		if candidate.addr == "" {
			continue
		}
		_, forwarded := rt.forwardConsumeProbe(ctx, w, r, candidate.partition, candidate.addr)
		if forwarded {
			return true
		}
	}
	return false
}

type consumePartitionCandidate struct {
	partition int
	addr      string
	backlog   int64
	order     int
}

func (rt *Router) consumePartitionCandidates(ctx context.Context, topicName string) []consumePartitionCandidate {
	assignments, err := rt.store.ListAssignments(topicName)
	if err != nil || len(assignments) == 0 {
		return nil
	}

	backlogByPartition := rt.backlogByPartition(ctx, topicName)
	candidates := make([]consumePartitionCandidate, 0, len(assignments))
	for i, assignment := range assignments {
		addr := rt.ownerAddr(topicName, assignment.Partition)
		if addr == "" {
			continue
		}
		candidates = append(candidates, consumePartitionCandidate{
			partition: assignment.Partition,
			addr:      addr,
			backlog:   backlogByPartition[assignment.Partition],
			order:     i,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].backlog != candidates[j].backlog {
			return candidates[i].backlog > candidates[j].backlog
		}
		if candidates[i].partition != candidates[j].partition {
			return candidates[i].partition < candidates[j].partition
		}
		return candidates[i].order < candidates[j].order
	})
	return candidates
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

func (rt *Router) RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool {
	leaderAddr := strings.TrimSpace(rt.store.LeaderAddr())
	if leaderAddr == "" {
		return false
	}
	memberAddr := rt.memberAddrByClusterAddr(leaderAddr)
	if memberAddr == "" {
		return false
	}
	fwd := r.Clone(ctx)
	rt.forward(w, fwd, memberAddr, body)
	return true
}

func (rt *Router) backlogByPartition(ctx context.Context, topicName string) map[int]int64 {
	if rt.snapshots == nil {
		return nil
	}
	snapshots, err := rt.snapshots.Snapshot(ctx)
	if err != nil {
		return nil
	}
	backlog := make(map[int]int64)
	for _, snapshot := range snapshots {
		if snapshot.Topic != topicName {
			continue
		}
		for _, partitionSnapshot := range snapshot.Partitions {
			value := partitionSnapshot.LogEndOffset - partitionSnapshot.CommittedOffset
			if value < 0 {
				value = 0
			}
			backlog[partitionSnapshot.Partition] = value
		}
		break
	}
	return backlog
}

func (rt *Router) forwardConsumeProbe(ctx context.Context, w http.ResponseWriter, r *http.Request, partition int, addr string) (bool, bool) {
	fwd := r.Clone(ctx)
	q := fwd.URL.Query()
	q.Set("partition", strconv.Itoa(partition))
	q.Set("wait", "0s")
	fwd.URL.RawQuery = q.Encode()
	probe := httptestResponseRecorder{header: make(http.Header)}
	rt.forward(&probe, fwd, addr, nil)
	if probe.code == 0 {
		probe.code = http.StatusOK
	}
	if probe.code == http.StatusNoContent {
		return false, false
	}
	copyHeader(w.Header(), probe.header)
	w.WriteHeader(probe.code)
	if len(probe.body) > 0 {
		_, _ = w.Write(probe.body)
	}
	return true, true
}

type httptestResponseRecorder struct {
	header http.Header
	body   []byte
	code   int
}

func (r *httptestResponseRecorder) Header() http.Header {
	return r.header
}

func (r *httptestResponseRecorder) Write(body []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	r.body = append(r.body, body...)
	return len(body), nil
}

func (r *httptestResponseRecorder) WriteHeader(statusCode int) {
	r.code = statusCode
}

func copyHeader(dst, src http.Header) {
	for key := range dst {
		dst.Del(key)
	}
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// ownerAddr returns the API address of the pod that owns (topicName, partition),
// or "" if this pod is the owner or the assignment/member cannot be resolved.
func (rt *Router) ownerAddr(topicName string, p int) string {
	a, err := rt.store.GetAssignment(topicName, p)
	if err != nil {
		return ""
	}
	if a.OwnerID == rt.selfID {
		return ""
	}
	m, err := rt.store.GetMember(a.OwnerID)
	if err == nil && m.Status != metastore.MemberDead {
		return m.Addr
	}
	if a.FollowerID == "" || a.FollowerID == rt.selfID {
		return ""
	}
	fm, err := rt.store.GetMember(a.FollowerID)
	if err != nil || fm.Status == metastore.MemberDead {
		return ""
	}
	return fm.Addr
}

func clusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
	clusterAddr = strings.TrimSpace(clusterAddr)
	peerAddr = strings.TrimSpace(peerAddr)
	if clusterAddr == "" || peerAddr == "" {
		return false
	}
	if clusterAddr == peerAddr {
		return true
	}
	if strings.HasPrefix(clusterAddr, ":") {
		return strings.HasSuffix(peerAddr, clusterAddr)
	}
	if strings.HasPrefix(peerAddr, ":") {
		return strings.HasSuffix(clusterAddr, peerAddr)
	}
	return false
}

func (rt *Router) memberAddrByClusterAddr(clusterAddr string) string {
	members, err := rt.store.ListMembers()
	if err != nil {
		return ""
	}
	for _, member := range members {
		if member.Status == metastore.MemberDead {
			continue
		}
		if strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) && clusterAddrMatchesPeer(clusterAddr, member.Addr) {
			return ""
		}
		if clusterAddrMatchesPeer(clusterAddr, member.Addr) {
			return member.Addr
		}
	}
	return ""
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
