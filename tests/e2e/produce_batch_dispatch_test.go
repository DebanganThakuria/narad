package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestBatchedDispatchDeliversBacklogAcrossPartitions fires a burst of async
// (202) produces across many partitions so the ingress WAL accrues a
// backlog, then drains every message back out. It exercises the real
// group-commit dispatcher end-to-end: interleaved WAL records must be
// grouped into per-partition batches, committed (one fsync per batch), and
// become consumable — with no loss and no message left undelivered.
func TestBatchedDispatchDeliversBacklogAcrossPartitions(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	const (
		partitions = 6
		total      = 300
	)
	env.createTopic("batch-dispatch", partitions, 0, int64(0))

	// Fire async produces (202) WITHOUT waiting for visibility, so many
	// records sit in the WAL at once and the dispatcher must batch the
	// interleaved stream rather than commit one record at a time.
	for i := range total {
		resp := env.rawPost("/v1/topics/batch-dispatch/produce?key="+url.QueryEscape(fmt.Sprintf("k-%d", i)), fmt.Sprintf(`{"n":%d}`, i))
		expectStatus(t, resp, http.StatusAccepted)
		_ = resp.Body.Close()
	}

	// Drain: consume+ack across all partitions until every produced message
	// is collected or we time out (commit must keep pace with accept).
	seen := make(map[int]bool, total)
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < total && time.Now().Before(deadline) {
		progressed := false
		for p := range partitions {
			resp := env.get("/v1/topics/batch-dispatch/consume?partition=" + strconv.Itoa(p))
			if resp.StatusCode == http.StatusNoContent {
				_ = resp.Body.Close()
				continue
			}
			expectOK(t, resp)
			msg := readJSON[topic.Message](t, resp)
			var payload struct {
				N int `json:"n"`
			}
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				t.Fatalf("unmarshal payload %s: %v", msg.Payload, err)
			}
			seen[payload.N] = true
			env.ack("batch-dispatch", msg.ReceiptHandle)
			progressed = true
		}
		if !progressed {
			time.Sleep(20 * time.Millisecond)
		}
	}

	if len(seen) != total {
		t.Fatalf("delivered %d/%d distinct messages; commit did not keep up or records were lost", len(seen), total)
	}
}

// TestBatchedDispatchPreservesPartitionOrder produces an interleaved burst
// keyed so every record lands on one partition, then verifies the partition
// delivers them in produce order — grouping/parallel commit must not reorder
// within a partition.
func TestBatchedDispatchPreservesPartitionOrder(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("batch-order", 3, 0, int64(0))

	// A fixed key pins all records to a single partition.
	const total = 100
	before := topicNextOffsets(t, env, "batch-order")
	for i := range total {
		resp := env.rawPost("/v1/topics/batch-order/produce?key=fixed-key", fmt.Sprintf(`{"n":%d}`, i))
		expectStatus(t, resp, http.StatusAccepted)
		resp.Body.Close()
	}
	_, part := waitForAnyVisibleOffset(t, env, "batch-order", before)

	want := 0
	deadline := time.Now().Add(20 * time.Second)
	for want < total && time.Now().Before(deadline) {
		resp := env.get("/v1/topics/batch-order/consume?partition=" + strconv.Itoa(part))
		if resp.StatusCode == http.StatusNoContent {
			_ = resp.Body.Close()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		expectOK(t, resp)
		msg := readJSON[topic.Message](t, resp)
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			t.Fatalf("unmarshal payload %s: %v", msg.Payload, err)
		}
		if payload.N != want {
			t.Fatalf("partition %d out of order: got n=%d, want %d", part, payload.N, want)
		}
		env.ack("batch-order", msg.ReceiptHandle)
		want++
	}
	if want != total {
		t.Fatalf("consumed %d/%d in order", want, total)
	}
}
