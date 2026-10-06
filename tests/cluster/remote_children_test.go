//go:build cluster

package cluster

// Remote children between two real 3-process clusters: cluster A ships
// a parent topic to a topic on cluster B through B's public batch
// produce API, behind a TLS front (Narad's API serves plain HTTP; a
// remote must be https). Everything else is the real server: the
// remotes API with the sealed password, the attach from a follower,
// the fan-out engine on every owner, the children listing, and the
// remote-aware deletes. Also, on one cluster, the plain deletes that
// every delete of this release sends through the remote plane.

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tlsFront is a TLS reverse proxy in front of one cluster's API, with a
// switch that makes it answer as an unreachable target behind a load
// balancer would.
type tlsFront struct {
	srv   *httptest.Server
	caPEM string
	down  atomic.Bool
	port  string
}

func newTLSFront(t *testing.T, backend string) *tlsFront {
	t.Helper()
	target, err := url.Parse(backend)
	if err != nil {
		t.Fatal(err)
	}
	f := &tlsFront{}
	proxy := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(target) }}
	f.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.down.Load() {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "<html>down</html>")
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	f.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.srv.Certificate().Raw}))
	u, _ := url.Parse(f.srv.URL)
	f.port = u.Port()
	return f
}

// randomSecret is 32 random bytes in base64, as `openssl rand -base64
// 32` makes a cluster secret.
func randomSecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// offloadRecord is a produced record's payload.
type offloadRecord struct {
	Seq int    `json:"seq"`
	Key string `json:"key"`
}

// offloadProducer produces keyed batches to cluster A's parent, round
// robin over the nodes, and remembers every record a 202 accepted.
type offloadProducer struct {
	c        *cluster
	mu       sync.Mutex
	accepted []offloadRecord
	next     int
}

// batch produces n records and waits for the 202; safe from any
// goroutine (it reports instead of failing).
func (p *offloadProducer) batch(n int) error {
	p.mu.Lock()
	start := p.next
	p.next += n
	p.mu.Unlock()
	msgs := make([]map[string]any, 0, n)
	recs := make([]offloadRecord, 0, n)
	for seq := start; seq < start+n; seq++ {
		r := offloadRecord{Seq: seq, Key: fmt.Sprintf("k-%02d", seq%17)}
		recs = append(recs, r)
		msgs = append(msgs, map[string]any{"key": r.Key, "payload": r})
	}
	node := (start / n) % 3
	// A timeout or a 5xx is ambiguous: retrying may duplicate, which the
	// target tolerates; a record counts once a 202 covers it.
	if _, _, err := p.c.apiTry(node, http.MethodPost, "/v1/topics/orders/produce/batch", map[string]any{"messages": msgs}, 30*time.Second, http.StatusAccepted); err != nil {
		return err
	}
	p.mu.Lock()
	p.accepted = append(p.accepted, recs...)
	p.mu.Unlock()
	return nil
}

func (p *offloadProducer) mustBatch(t *testing.T, n int) {
	t.Helper()
	if err := p.batch(n); err != nil {
		t.Fatal(err)
	}
}

func (p *offloadProducer) all() []offloadRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]offloadRecord(nil), p.accepted...)
}

// remoteChildListing is the remote child's row in A's children listing.
type remoteChildListing struct {
	Name        string `json:"name"`
	LagMessages int64  `json:"lag_messages"`
	LagComplete bool   `json:"lag_complete"`
	State       string `json:"state"`
	Remote      *struct {
		Name  string `json:"name"`
		Topic string `json:"topic"`
	} `json:"remote"`
}

func listRemoteChild(t *testing.T, c *cluster, via int) (remoteChildListing, bool) {
	t.Helper()
	_, body := c.apiWant(via, http.MethodGet, "/v1/topics/orders/children", nil, 30*time.Second, http.StatusOK)
	var listing struct {
		Children []remoteChildListing `json:"children"`
	}
	if err := json.Unmarshal(body, &listing); err != nil {
		t.Fatalf("decode children listing: %v (%s)", err, body)
	}
	for _, ch := range listing.Children {
		if ch.Name == "orders-to-b" {
			return ch, true
		}
	}
	return remoteChildListing{}, false
}

func waitRemoteChild(t *testing.T, c *cluster, what string, timeout time.Duration, cond func(remoteChildListing) bool) remoteChildListing {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last remoteChildListing
	for {
		if ch, ok := listRemoteChild(t, c, 0); ok {
			last = ch
			if cond(ch) {
				return ch
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("remote child never reached %s within %s; last listing row %+v", what, timeout, last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// landed is every record B's topic holds: by "seq", the partitions it
// is stored in (a duplicate adds one) and the key stored with it.
func landed(t *testing.T, c *cluster, topicName string) (partitions map[int][]int, keys map[int]string, total int) {
	t.Helper()
	partitions, keys = map[int][]int{}, map[int]string{}
	for _, s := range replicaStats(t, c, 0, topicName) {
		for off := s.OldestOffset; off < s.HighWatermark; off++ {
			path := fmt.Sprintf("/v1/topics/%s/consume?partition=%d&offset=%d", topicName, s.Index, off)
			_, body := c.apiWant(0, http.MethodGet, path, nil, 30*time.Second, http.StatusOK)
			var msg struct {
				Partition int           `json:"partition"`
				Key       string        `json:"key"`
				Payload   offloadRecord `json:"payload"`
			}
			if err := json.Unmarshal(body, &msg); err != nil {
				t.Fatalf("decode replay %s: %v (%s)", path, err, body)
			}
			partitions[msg.Payload.Seq] = append(partitions[msg.Payload.Seq], msg.Partition)
			keys[msg.Payload.Seq] = msg.Key
			total++
		}
	}
	return partitions, keys, total
}

// The hot-topic offload playbook (design ch. 9.1) under a steady
// producer: a remote created through the API, a remote child attached
// through a follower from the earliest retained record, and then:
// at least once (every accepted record lands on B), keyed placement
// (a key's records share one partition on B and keep their key), lag
// to zero in the listing, a delete refused while records are unshipped
// and forced after, and the remote's own delete once no link names it.
func TestRemoteChildOffloadAcrossClusters(t *testing.T) {
	cb := newCluster(t, clusterOptions{})
	cb.startAll()
	cb.waitAllReady(90 * time.Second)
	cb.waitAdmin(30 * time.Second)
	front := newTLSFront(t, cb.url(0))

	ca := newCluster(t, clusterOptions{env: map[string]string{
		// Remotes need a cluster secret of at least 32 random bytes.
		"NARAD_CLUSTER_SECRET":            randomSecret(t),
		"NARAD_REMOTES_ALLOWED_PORTS":     front.port,
		"NARAD_REMOTES_ALLOW_ADDRESSES":   "127.0.0.0/8",
		"NARAD_REMOTES_API_HOP_ENCRYPTED": "true",
	}})
	ca.startAll()
	ca.waitAllReady(90 * time.Second)
	ca.waitAdmin(30 * time.Second)
	replicaAwaitMembers(t, ca)

	// B: the target topic and a produce-only replicator user.
	replPass := rand.Text()
	cb.apiWant(0, http.MethodPost, "/v1/topics", map[string]any{"name": "orders", "partitions": 3}, 30*time.Second, http.StatusCreated)
	cb.apiWant(0, http.MethodPost, "/v1/users", map[string]any{
		"username": "repl-from-a", "password": replPass,
		"grants": []map[string]any{{"action": "produce", "patterns": []string{"orders"}}},
	}, 30*time.Second, http.StatusCreated)
	replicaOwners(t, cb, "orders", 3)

	// A: the parent, with retention above the 72h attach warning.
	ca.apiWant(0, http.MethodPost, "/v1/topics", map[string]any{"name": "orders", "partitions": 3, "retention_ms": 4 * 24 * 3600 * 1000}, 30*time.Second, http.StatusCreated)
	replicaOwners(t, ca, "orders", 3)
	ca.apiWant(1, http.MethodPost, "/v1/remotes", map[string]any{
		"name": "b", "url": front.srv.URL, "username": "repl-from-a", "password": replPass, "ca_pem": front.caPEM,
	}, 30*time.Second, http.StatusCreated)

	p := &offloadProducer{c: ca}
	for range 10 {
		p.mustBatch(t, 20)
	}
	follower := ca.follower()
	status, body := ca.apiWant(follower, http.MethodPost, "/v1/topics/orders/children", map[string]any{
		"child": "orders-to-b", "remote": "b", "remote_topic": "orders", "from": "earliest",
	}, 60*time.Second, http.StatusCreated)
	t.Logf("attach through %s: %d %s", ca.nodes[follower].id, status, truncate(body, 300))

	// A steady producer while the link ships.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := p.batch(20); err != nil {
				t.Errorf("steady producer: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	want := p.all()

	// Lag to zero in the listing, with the link running.
	ch := waitRemoteChild(t, ca, "lag 0 and running", 120*time.Second, func(ch remoteChildListing) bool {
		return ch.LagMessages == 0 && ch.LagComplete && ch.State == "running"
	})
	if ch.Remote == nil || ch.Remote.Name != "b" || ch.Remote.Topic != "orders" {
		t.Fatalf("listing row %+v, want the remote named", ch)
	}

	// At least once, keyed placement, keys kept.
	parts, keys, total := landed(t, cb, "orders")
	keyPartition := map[string]int{}
	for _, r := range want {
		ps, ok := parts[r.Seq]
		if !ok {
			t.Fatalf("record %d (key %s) was accepted on A and never reached B (%d of %d records landed)", r.Seq, r.Key, len(parts), len(want))
		}
		if keys[r.Seq] != r.Key {
			t.Fatalf("record %d arrived with key %q, want %q", r.Seq, keys[r.Seq], r.Key)
		}
		for _, part := range ps {
			if prev, seen := keyPartition[r.Key]; seen && prev != part {
				t.Fatalf("key %s landed on B partitions %d and %d", r.Key, prev, part)
			}
			keyPartition[r.Key] = part
		}
	}
	dups := total - len(want)
	t.Logf("%d records accepted on A, %d stored on B (%d duplicates), %d keys", len(want), total, dups, len(keyPartition))
	if dups > 500 {
		t.Fatalf("%d duplicates without a fault, want at most one slab", dups)
	}

	// B goes away: new records cannot ship, and a delete without force
	// is refused with the counts; forced, it goes ahead.
	front.down.Store(true)
	for range 2 {
		p.mustBatch(t, 20)
	}
	waitRemoteChild(t, ca, "lag above 0", 60*time.Second, func(ch remoteChildListing) bool { return ch.LagMessages > 0 })
	status, body = ca.apiWant(2, http.MethodDelete, "/v1/topics/orders/children/orders-to-b", nil, 60*time.Second, http.StatusConflict)
	var refused struct {
		Lag     int64             `json:"lag_messages"`
		Backlog map[string]uint64 `json:"dispatch_backlog"`
	}
	if err := json.Unmarshal(body, &refused); err != nil || (refused.Lag == 0 && len(refused.Backlog) == 0) {
		t.Fatalf("unforced delete: %d %s, want 409 with the unshipped counts", status, body)
	}
	ca.apiWant(2, http.MethodDelete, "/v1/topics/orders/children/orders-to-b?force=true", nil, 60*time.Second, http.StatusNoContent)
	if _, ok := listRemoteChild(t, ca, 0); ok {
		t.Fatal("the remote child is still listed after the forced delete")
	}
	ca.apiWant(0, http.MethodGet, "/v1/topics/orders-to-b", nil, 30*time.Second, http.StatusNotFound)
	front.down.Store(false)

	ca.apiWant(0, http.MethodDelete, "/v1/remotes/b", nil, 30*time.Second, http.StatusNoContent)
}
