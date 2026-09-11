package e2e

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// TestAckedAheadSurvivesRestart pins the durability contract for
// out-of-order acks: a message acked while an older one is still
// leased is not redelivered after the broker restarts. Before, the
// acked-ahead set lived only in memory and every restart replayed it.
func TestAckedAheadSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	metastoreDir := filepath.Join(t.TempDir(), "metastore")
	opts := []envOption{withDataDir(dataDir), withMetastoreDir(metastoreDir), withOffsetPersistence()}

	first := newTestEnv(t, opts...)
	const topicName = "ahead-restart"
	first.createTopic(topicName, 3, 0) // one key pins one partition
	for _, p := range []string{`{"n":0}`, `{"n":1}`, `{"n":2}`} {
		first.produce(topicName, "k", p)
	}
	// Lease all three; ack 1 and 2, leave 0 leased (the frontier hole).
	for i := range 3 {
		msg := first.consume("/v1/topics/" + topicName + "/consume")
		if msg.Offset != int64(i) {
			t.Fatalf("consume #%d returned offset %d", i, msg.Offset)
		}
		if i > 0 {
			first.ack(topicName, msg.ReceiptHandle)
		}
	}
	// Graceful close flushes the frontier (-1) and the set {1, 2}.
	first.close()

	second := newTestEnv(t, opts...)
	if !second.awaitPartitionAssignments(topicName, 3) {
		t.Fatal("topic assignments did not come back after the restart")
	}
	// Leases evaporate on restart: 0 comes back at once. 1 and 2 must
	// not: they were acked.
	msg := second.consume("/v1/topics/" + topicName + "/consume")
	if msg.Offset != 0 {
		t.Fatalf("first consume after restart returned offset %d, want 0 (the only unacked message)", msg.Offset)
	}
	second.ack(topicName, msg.ReceiptHandle)
	resp := second.get("/v1/topics/" + topicName + "/consume?wait=300ms")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("consume after the hole was acked: got %d body=%s, want 204 (1 and 2 were acked before the restart)", resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()

	// And the frontier now covers all three: a third restart hands out
	// nothing at all.
	time.Sleep(50 * time.Millisecond)
	second.close()
	third := newTestEnv(t, opts...)
	if !third.awaitPartitionAssignments(topicName, 3) {
		t.Fatal("topic assignments did not come back after the second restart")
	}
	resp = third.get("/v1/topics/" + topicName + "/consume?wait=300ms")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("consume after the second restart: got %d body=%s, want 204", resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()
}
