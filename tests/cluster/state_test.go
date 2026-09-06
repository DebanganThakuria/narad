//go:build cluster

package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

// ---- metadata churn ------------------------------------------------------------

var churnSeq atomic.Int64

// churn performs rounds of control-plane writes through node via: each
// round creates a topic, alters its retention, grows its partitions,
// registers a schema, creates a user, changes its grants, deletes the
// user and deletes the topic. Every write is a Raft entry (topic
// creation and partition growth add assignment entries too), so one
// round is at least eight entries. It returns the number of writes made.
//
// Topic deletes run in the background: a delete fans a purge out to
// every member not yet marked dead, and a member that was just killed
// holds that fan-out for its whole budget (see the findings in the PR),
// which would otherwise serialise the churn behind it. Retried writes
// accept the "already happened" answer (409 on create, 404 on delete):
// a write can land and its response still be lost to an election.
func (c *cluster) churn(via, rounds int) int {
	c.t.Helper()
	writes := 0
	do := func(method, path string, body any, want ...int) {
		c.apiWant(via, method, path, body, 30*time.Second, want...)
		writes++
	}
	var deletes sync.WaitGroup
	var deleteMu sync.Mutex
	var deleteErrs []error
	var slowestDelete time.Duration
	for range rounds {
		seq := churnSeq.Add(1)
		topic := fmt.Sprintf("churn-%d", seq)
		user := fmt.Sprintf("churn-user-%d", seq)
		do(http.MethodPost, "/v1/topics", map[string]any{"name": topic, "partitions": 3}, http.StatusCreated, http.StatusConflict)
		do(http.MethodPatch, "/v1/topics/"+topic, map[string]any{"retention_ms": 3_600_000}, http.StatusOK)
		do(http.MethodPatch, "/v1/topics/"+topic, map[string]any{"partitions": 4}, http.StatusOK)
		do(http.MethodPatch, "/v1/topics/"+topic, map[string]any{"schema": json.RawMessage(`{"type":"object"}`)}, http.StatusOK)
		do(http.MethodPost, "/v1/users", map[string]any{
			"username": user, "password": "churn-password-1",
			"grants": []map[string]any{{"action": "produce", "patterns": []string{topic}}},
		}, http.StatusCreated, http.StatusConflict)
		do(http.MethodPut, "/v1/users/"+user+"/grants", map[string]any{
			"grants": []map[string]any{{"action": "consume", "patterns": []string{topic}}},
		}, http.StatusOK, http.StatusNoContent)
		do(http.MethodDelete, "/v1/users/"+user, nil, http.StatusNoContent, http.StatusNotFound)
		writes++
		deletes.Go(func() {
			started := time.Now()
			_, _, err := c.apiTry(via, http.MethodDelete, "/v1/topics/"+topic, nil, 60*time.Second, http.StatusNoContent, http.StatusNotFound)
			took := time.Since(started)
			deleteMu.Lock()
			if err != nil {
				deleteErrs = append(deleteErrs, err)
			}
			slowestDelete = max(slowestDelete, took)
			deleteMu.Unlock()
		})
	}
	deletes.Wait()
	if len(deleteErrs) > 0 {
		c.t.Fatalf("churn topic deletes failed: %v", errors.Join(deleteErrs...))
	}
	if slowestDelete > 2*time.Second {
		c.t.Logf("churn: slowest topic delete took %s (a purge fan-out waiting on a member that is down but not yet marked dead)", slowestDelete.Round(time.Millisecond))
	}
	return writes
}

// ---- raft on-disk state ----------------------------------------------------------

type snapshotMeta struct {
	ID    string `json:"ID"`
	Index uint64 `json:"Index"`
	Term  uint64 `json:"Term"`
}

// snapshotsOf lists the completed Raft snapshots under dataDir, newest
// first. It reads the files a running node writes, so it works on live
// and dead nodes alike.
func snapshotsOf(dataDir string) []snapshotMeta {
	entries, err := os.ReadDir(filepath.Join(dataDir, "metastore", "snapshots"))
	if err != nil {
		return nil
	}
	var out []snapshotMeta
	for _, e := range entries {
		if !e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dataDir, "metastore", "snapshots", e.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var m snapshotMeta
		if json.Unmarshal(body, &m) == nil {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index > out[j].Index })
	return out
}

func newestSnapshot(dataDir string) (snapshotMeta, bool) {
	snaps := snapshotsOf(dataDir)
	if len(snaps) == 0 {
		return snapshotMeta{}, false
	}
	return snaps[0], true
}

// snapshotInProgress reports whether the node at dataDir is mid-snapshot:
// hashicorp/raft's file store writes into a "<id>.tmp" directory and
// renames it into place when the snapshot is complete.
func snapshotInProgress(dataDir string) bool {
	entries, err := os.ReadDir(filepath.Join(dataDir, "metastore", "snapshots"))
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			return true
		}
	}
	return false
}

// lastLogIndex opens a STOPPED node's Raft log read-only and returns its
// last index: the point the node's log reached before it was killed.
func lastLogIndex(dataDir string) (uint64, error) {
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(dataDir, "metastore", "raft.db"),
		BoltOptions: &bolt.Options{Timeout: 5 * time.Second, ReadOnly: true},
	})
	if err != nil {
		return 0, err
	}
	defer store.Close()
	return store.LastIndex()
}

// firstLogIndex is the same for the first retained index.
func firstLogIndex(dataDir string) (uint64, error) {
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(dataDir, "metastore", "raft.db"),
		BoltOptions: &bolt.Options{Timeout: 5 * time.Second, ReadOnly: true},
	})
	if err != nil {
		return 0, err
	}
	defer store.Close()
	return store.FirstIndex()
}

var _ raft.LogStore = (*raftboltdb.BoltStore)(nil)

// ---- convergence -------------------------------------------------------------------

// clusterView is what the test compares across nodes: the metadata every
// replica must agree on once caught up. Per-partition counters (high
// watermarks, sizes) are deliberately excluded; they move under load.
type clusterView struct {
	Topics  []topicView               `json:"topics"`
	Users   []userView                `json:"users"`
	Schemas map[string]any            `json:"schemas"`
	Members map[string]memberView     `json:"members"`
	Owners  map[string]map[int]string `json:"owners"`
}

type topicView struct {
	Name        string `json:"name"`
	Partitions  int    `json:"partitions"`
	RetentionMs int64  `json:"retention_ms"`
}

type userView struct {
	Username string `json:"username"`
	Grants   any    `json:"grants"`
}

type memberView struct {
	Status   string `json:"status"`
	Draining bool   `json:"draining"`
}

// view reads node i's metadata through the public API. Every read here
// is served from the node's own replica (topics, users, schema history)
// or its own assignment view (owners), so equal views mean the replicas
// agree, not that one node forwarded to another.
func (c *cluster) view(i int) (clusterView, error) {
	v := clusterView{Schemas: map[string]any{}, Members: map[string]memberView{}, Owners: map[string]map[int]string{}}
	status, body, err := c.api(i, http.MethodGet, "/v1/topics?limit=1000", nil)
	if err != nil || status != http.StatusOK {
		return v, fmt.Errorf("list topics: status %d err %v", status, err)
	}
	var listed struct {
		Topics []topicView `json:"topics"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return v, err
	}
	v.Topics = listed.Topics
	sort.Slice(v.Topics, func(a, b int) bool { return v.Topics[a].Name < v.Topics[b].Name })

	for _, t := range v.Topics {
		status, body, err := c.api(i, http.MethodGet, "/v1/topics/"+url.PathEscape(t.Name)+"/schema", nil)
		if err != nil || status != http.StatusOK {
			return v, fmt.Errorf("schema %s: status %d err %v", t.Name, status, err)
		}
		var raw any
		if err := json.Unmarshal(body, &raw); err != nil {
			return v, err
		}
		v.Schemas[t.Name] = raw

		status, body, err = c.api(i, http.MethodGet, "/v1/topics/"+url.PathEscape(t.Name), nil)
		if err == nil && status == http.StatusMisdirectedRequest {
			// The merged per-partition stats need every owner to answer;
			// an owner that is up but cut off from Raft refuses, and the
			// whole GET fails with 421 (see the findings in the PR). The
			// metadata itself is fine, so record the owners as unknown
			// and keep comparing the rest.
			v.Owners[t.Name] = map[int]string{-1: "unknown (421)"}
			continue
		}
		if err != nil || status != http.StatusOK {
			return v, fmt.Errorf("topic %s: status %d err %v: %s", t.Name, status, err, truncate(body, 200))
		}
		var details struct {
			Stats []struct {
				Index int    `json:"index"`
				Owner string `json:"owner_node"`
			} `json:"partition_stats"`
		}
		if err := json.Unmarshal(body, &details); err != nil {
			return v, err
		}
		owners := map[int]string{}
		for _, s := range details.Stats {
			owners[s.Index] = s.Owner
		}
		v.Owners[t.Name] = owners
	}

	status, body, err = c.api(i, http.MethodGet, "/v1/users", nil)
	if err != nil || status != http.StatusOK {
		return v, fmt.Errorf("list users: status %d err %v", status, err)
	}
	if err := json.Unmarshal(body, &v.Users); err != nil {
		return v, err
	}
	sort.Slice(v.Users, func(a, b int) bool { return v.Users[a].Username < v.Users[b].Username })

	status, body, err = c.api(i, http.MethodGet, "/v1/cluster/members", nil)
	if err != nil || status != http.StatusOK {
		return v, fmt.Errorf("members: status %d err %v", status, err)
	}
	var members struct {
		Members []struct {
			ID       string `json:"id"`
			Status   string `json:"status"`
			Draining bool   `json:"draining"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &members); err != nil {
		return v, err
	}
	for _, m := range members.Members {
		v.Members[m.ID] = memberView{Status: m.Status, Draining: m.Draining}
	}
	return v, nil
}

// waitConverged waits until every running node reports the same view as
// node reference, and fails with the first difference otherwise.
func (c *cluster) waitConverged(reference int, timeout time.Duration) clusterView {
	c.t.Helper()
	return c.waitConvergedExcept(reference, timeout)
}

// waitConvergedExcept is waitConverged ignoring the excluded nodes (a
// node that is running but deliberately cut off from the cluster).
func (c *cluster) waitConvergedExcept(reference int, timeout time.Duration, exclude ...int) clusterView {
	c.t.Helper()
	excluded := map[int]bool{}
	for _, e := range exclude {
		excluded[e] = true
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ref, err := c.view(reference)
		if err == nil {
			same := true
			for i := range c.nodes {
				if i == reference || !c.isRunning(i) || excluded[i] {
					continue
				}
				other, err := c.view(i)
				if err != nil {
					lastErr = fmt.Errorf("%s: %w", c.nodes[i].id, err)
					same = false
					break
				}
				if diff := viewDiff(ref, other); diff != "" {
					lastErr = fmt.Errorf("%s differs from %s: %s", c.nodes[i].id, c.nodes[reference].id, diff)
					same = false
					break
				}
			}
			if same {
				c.t.Logf("views converged: %d topics, %d users, %d members", len(ref.Topics), len(ref.Users), len(ref.Members))
				return ref
			}
		} else {
			lastErr = fmt.Errorf("%s: %w", c.nodes[reference].id, err)
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("views did not converge within %s: %v", timeout, lastErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// viewDiff names the first difference between two views, or "".
func viewDiff(a, b clusterView) string {
	if !reflect.DeepEqual(a.Topics, b.Topics) {
		return fmt.Sprintf("topics %v vs %v", a.Topics, b.Topics)
	}
	if !reflect.DeepEqual(a.Users, b.Users) {
		return fmt.Sprintf("users %v vs %v", a.Users, b.Users)
	}
	if !reflect.DeepEqual(a.Schemas, b.Schemas) {
		return "schema histories differ"
	}
	if !reflect.DeepEqual(a.Owners, b.Owners) {
		return fmt.Sprintf("owners %v vs %v", a.Owners, b.Owners)
	}
	if !reflect.DeepEqual(a.Members, b.Members) {
		return fmt.Sprintf("members %v vs %v", a.Members, b.Members)
	}
	return ""
}

// ---- ownership and serving ----------------------------------------------------------

// assertServesOwnPartitions creates a fresh topic, checks node i owns at
// least one of its partitions, produces to node i directly, and consumes
// and acks through node i until a message from a partition node i OWNS
// has been served: proof the restarted node took its partitions back and
// serves them, rather than only forwarding to peers.
func (c *cluster) assertServesOwnPartitions(i int) {
	c.t.Helper()
	n := c.nodes[i]
	topic := fmt.Sprintf("own-%s-%d", n.id, time.Now().UnixNano()%1_000_000)
	c.apiWant(i, http.MethodPost, "/v1/topics", map[string]any{"name": topic, "partitions": 6, "visibility_timeout_ms": 5000}, 20*time.Second, http.StatusCreated)

	// Wait until every partition is assigned and node i has some.
	var owners map[int]string
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, body := c.apiWant(i, http.MethodGet, "/v1/topics/"+topic, nil, 20*time.Second, http.StatusOK)
		var details struct {
			Stats []struct {
				Index int    `json:"index"`
				Owner string `json:"owner_node"`
			} `json:"partition_stats"`
		}
		_ = json.Unmarshal(body, &details)
		owners = map[int]string{}
		mine := 0
		for _, s := range details.Stats {
			owners[s.Index] = s.Owner
			if s.Owner == n.id {
				mine++
			}
		}
		if len(owners) == 6 && mine > 0 {
			break
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s owns no partition of %s after 20s: %v", n.id, topic, owners)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 36 keys spread over 6 partitions: at least one lands on node i.
	for k := range 36 {
		c.apiWant(i, http.MethodPost, "/v1/topics/"+topic+"/produce?key=k"+fmt.Sprint(k), []byte(fmt.Sprintf(`{"n":%d}`, k)), 20*time.Second, http.StatusAccepted)
	}
	servedOwn := 0
	servedAny := 0
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && servedOwn == 0 {
		status, body, err := c.api(i, http.MethodGet, "/v1/topics/"+topic+"/consume?wait=500ms", nil)
		if err != nil || status != http.StatusOK {
			continue
		}
		var msg struct {
			Partition     int    `json:"partition"`
			ReceiptHandle string `json:"receipt_handle"`
		}
		if json.Unmarshal(body, &msg) != nil {
			continue
		}
		servedAny++
		if owners[msg.Partition] == n.id {
			servedOwn++
		}
		c.apiWant(i, http.MethodPost, "/v1/topics/"+topic+"/ack?receipt_handle="+url.QueryEscape(msg.ReceiptHandle), nil, 10*time.Second, http.StatusNoContent, http.StatusGone)
	}
	if servedOwn == 0 {
		c.t.Fatalf("%s served %d messages of %s but none from a partition it owns (%v)", n.id, servedAny, topic, owners)
	}
	c.t.Logf("%s serves consumes from its own partitions (%d served, %d from own partitions)", n.id, servedAny, servedOwn)
}

// memberOwned returns how many partitions node id owns per node via's
// /v1/cluster/members.
func (c *cluster) memberOwned(via int, id string) int {
	c.t.Helper()
	_, body := c.apiWant(via, http.MethodGet, "/v1/cluster/members", nil, 20*time.Second, http.StatusOK)
	var members struct {
		Members []struct {
			ID    string `json:"id"`
			Owned int    `json:"owned_partitions"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &members); err != nil {
		c.t.Fatalf("decode members: %v", err)
	}
	for _, m := range members.Members {
		if m.ID == id {
			return m.Owned
		}
	}
	return 0
}
