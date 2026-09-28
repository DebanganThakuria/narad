//go:build cluster

package cluster

// TestReplicaChildKeepsKeylessCopiesApart checks the replica pattern's
// placement promise for keyless records on a real 3-process cluster.
// A fan-out child created with `parent` has partition p placed away from
// the owner of the parent's partition p, which keeps a record's two
// copies apart only while the record lands on the same partition index
// in both topics. A keyed record gets there by its key's hash; a
// keyless record, which is stored without a key and placed round-robin
// in the parent, gets there because fan-out keeps its parent
// partition's index. Placing it round-robin in the child as well put
// about a third of the keyless records' two copies on one node.
//
// Everything is the real server: plain HTTP produce without ?key=,
// create-as-child placement, the fan-out runner, and replay consumes to
// find where each copy lives. The keyed records are a control.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type replicaStat struct {
	Index         int    `json:"index"`
	OldestOffset  int64  `json:"oldest_offset"`
	HighWatermark int64  `json:"high_watermark"`
	OwnerNode     string `json:"owner_node"`
}

// replicaStats returns node via's merged partition stats for topicName.
func replicaStats(t *testing.T, c *cluster, via int, topicName string) []replicaStat {
	t.Helper()
	_, body := c.apiWant(via, http.MethodGet, "/v1/topics/"+topicName, nil, 30*time.Second, http.StatusOK)
	var got struct {
		Stats []replicaStat `json:"partition_stats"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode topic %s: %v", topicName, err)
	}
	sort.Slice(got.Stats, func(i, j int) bool { return got.Stats[i].Index < got.Stats[j].Index })
	return got.Stats
}

// replicaOwners waits until every node agrees every partition of topicName
// has an owner, and returns owner by partition index.
func replicaOwners(t *testing.T, c *cluster, topicName string, partitions int) []string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var owners []string
		settled := true
		for node := range 3 {
			stats := replicaStats(t, c, node, topicName)
			if len(stats) != partitions {
				settled = false
				break
			}
			cur := make([]string, partitions)
			for _, s := range stats {
				if s.OwnerNode == "" {
					settled = false
				}
				cur[s.Index] = s.OwnerNode
			}
			if !settled {
				break
			}
			if owners == nil {
				owners = cur
			} else {
				for p := range cur {
					if cur[p] != owners[p] {
						settled = false
					}
				}
			}
		}
		if settled {
			return owners
		}
		if time.Now().After(deadline) {
			t.Fatalf("ownership of %s never settled on every node", topicName)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// replicaAwaitAnchored waits until the fan-out cursors of parent->child have
// all anchored, so every record produced afterwards is fanned out.
func replicaAwaitAnchored(t *testing.T, c *cluster, parent, child string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, body := c.apiWant(0, http.MethodGet, "/v1/topics/"+parent+"/children", nil, 30*time.Second, http.StatusOK)
		var listing struct {
			Children []struct {
				Name        string `json:"name"`
				LagComplete bool   `json:"lag_complete"`
			} `json:"children"`
		}
		if err := json.Unmarshal(body, &listing); err != nil {
			t.Fatalf("decode children of %s: %v", parent, err)
		}
		for _, ch := range listing.Children {
			if ch.Name == child && ch.LagComplete {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("fan-out cursors %s->%s never anchored: %s", parent, child, body)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// replicaAwaitMembers waits until every node sees all three members alive, so
// the topics below are spread over the whole cluster rather than placed
// on whichever node registered first.
func replicaAwaitMembers(t *testing.T, c *cluster) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := true
		for node := range 3 {
			_, body := c.apiWant(node, http.MethodGet, "/v1/cluster/members", nil, 30*time.Second, http.StatusOK)
			var got struct {
				Members []struct {
					Status string `json:"status"`
				} `json:"members"`
			}
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode members: %v (%s)", err, body)
			}
			alive := 0
			for _, m := range got.Members {
				if m.Status == "alive" {
					alive++
				}
			}
			if alive != 3 {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the three members never all showed alive")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func replicaCommitted(t *testing.T, c *cluster, topicName string) int64 {
	var total int64
	for _, s := range replicaStats(t, c, 0, topicName) {
		total += s.HighWatermark
	}
	return total
}

type replicaRecord struct {
	Seq  int    `json:"seq"`
	Kind string `json:"kind"`
}

// replicaLocate replays every retained offset of every partition of
// topicName and returns, per record ("kind/seq"), the partition it is
// stored in and the key the server stored with it.
func replicaLocate(t *testing.T, c *cluster, topicName string) (partition map[string]int, key map[string]string) {
	t.Helper()
	partition, key = map[string]int{}, map[string]string{}
	for _, s := range replicaStats(t, c, 0, topicName) {
		for off := s.OldestOffset; off < s.HighWatermark; off++ {
			path := fmt.Sprintf("/v1/topics/%s/consume?partition=%d&offset=%d", topicName, s.Index, off)
			_, body := c.apiWant(0, http.MethodGet, path, nil, 30*time.Second, http.StatusOK)
			var msg struct {
				Partition int           `json:"partition"`
				Offset    int64         `json:"offset"`
				Key       string        `json:"key"`
				Payload   replicaRecord `json:"payload"`
			}
			if err := json.Unmarshal(body, &msg); err != nil {
				t.Fatalf("decode replay %s: %v (%s)", path, err, body)
			}
			if msg.Partition != s.Index || msg.Offset != off {
				t.Fatalf("replay %s answered partition %d offset %d", path, msg.Partition, msg.Offset)
			}
			id := fmt.Sprintf("%s/%d", msg.Payload.Kind, msg.Payload.Seq)
			if _, dup := partition[id]; dup {
				t.Fatalf("%s holds %s twice", topicName, id)
			}
			partition[id] = s.Index
			key[id] = msg.Key
		}
	}
	return partition, key
}

func TestReplicaChildKeepsKeylessCopiesApart(t *testing.T) {
	c := newCluster(t, clusterOptions{})
	c.startAll()
	c.waitAllReady(120 * time.Second)
	c.waitAdmin(60 * time.Second)
	replicaAwaitMembers(t, c)

	const (
		parent     = "replica-parent"
		child      = "replica-child"
		partitions = 3
		perKind    = 30
	)

	// The documented replica recipe: parent first, then the child in one
	// call with `parent`, inheriting the partition count.
	c.apiWant(0, http.MethodPost, "/v1/topics", map[string]any{"name": parent, "partitions": partitions},
		30*time.Second, http.StatusOK, http.StatusCreated)
	parentOwners := replicaOwners(t, c, parent, partitions)
	c.apiWant(0, http.MethodPost, "/v1/topics", map[string]any{"name": child, "parent": parent},
		30*time.Second, http.StatusOK, http.StatusCreated)
	childOwners := replicaOwners(t, c, child, partitions)
	t.Logf("parent owners by partition: %v", parentOwners)
	t.Logf("child  owners by partition: %v", childOwners)

	// Precondition: placement did its part. Child partition p is off the
	// node owning parent partition p, so any co-location below is the
	// record placement, not the partition placement.
	for p := range partitions {
		if parentOwners[p] == childOwners[p] {
			t.Fatalf("placement not anti-affine at partition %d: both on %s", p, parentOwners[p])
		}
	}
	replicaAwaitAnchored(t, c, parent, child)

	// Keyless produces, the way a client that sets no ?key= sends them,
	// through each node in turn as a load balancer would.
	for i := range perKind {
		body := []byte(fmt.Sprintf(`{"seq":%d,"kind":"keyless"}`, i))
		c.apiWant(i%3, http.MethodPost, "/v1/topics/"+parent+"/produce", body,
			30*time.Second, http.StatusOK, http.StatusCreated, http.StatusAccepted)
	}
	// Control: keyed records, with keys in the key-<n> form older
	// releases invented for a keyless produce.
	for i := range perKind {
		key := "key-" + strconv.FormatUint(uint64(i+1), 36)
		body := []byte(fmt.Sprintf(`{"seq":%d,"kind":"keyed"}`, i))
		c.apiWant(i%3, http.MethodPost, "/v1/topics/"+parent+"/produce?key="+key, body,
			30*time.Second, http.StatusOK, http.StatusCreated, http.StatusAccepted)
	}

	const total = 2 * perKind
	deadline := time.Now().Add(60 * time.Second)
	for replicaCommitted(t, c, parent) < total || replicaCommitted(t, c, child) < total {
		if time.Now().After(deadline) {
			t.Fatalf("parent/child never reached %d records: parent=%d child=%d",
				total, replicaCommitted(t, c, parent), replicaCommitted(t, c, child))
		}
		time.Sleep(200 * time.Millisecond)
	}

	parentPart, parentKey := replicaLocate(t, c, parent)
	childPart, _ := replicaLocate(t, c, child)
	if len(parentPart) != total || len(childPart) != total {
		t.Fatalf("located %d parent and %d child records, want %d each", len(parentPart), len(childPart), total)
	}

	colocated := map[string][]string{}
	stored := map[string]int{} // records of each kind stored with a key
	for id, pp := range parentPart {
		cp, ok := childPart[id]
		if !ok {
			t.Fatalf("record %s missing from the child", id)
		}
		kind, _, _ := strings.Cut(id, "/")
		if parentKey[id] != "" {
			stored[kind]++
		}
		if kind == "keyless" && cp != pp {
			t.Errorf("keyless record %s is in parent partition %d but child partition %d", id, pp, cp)
		}
		if parentOwners[pp] == childOwners[cp] {
			colocated[kind] = append(colocated[kind],
				fmt.Sprintf("%s(parent p%d, child p%d, both on %s)", id, pp, cp, parentOwners[pp]))
		}
	}
	sort.Strings(colocated["keyless"])
	t.Logf("records stored with a key: keyless=%d/%d keyed=%d/%d", stored["keyless"], perKind, stored["keyed"], perKind)
	t.Logf("co-located copies: keyless=%d/%d keyed=%d/%d", len(colocated["keyless"]), perKind, len(colocated["keyed"]), perKind)

	if stored["keyless"] != 0 {
		t.Errorf("%d of %d keyless records were stored with a key; a keyless produce must stay keyless", stored["keyless"], perKind)
	}
	for _, kind := range []string{"keyless", "keyed"} {
		if n := len(colocated[kind]); n != 0 {
			show := colocated[kind]
			if len(show) > 6 {
				show = show[:6]
			}
			t.Errorf("%d of %d %s records have their parent copy and replica copy on the same node: %v", n, perKind, kind, show)
		}
	}
}
