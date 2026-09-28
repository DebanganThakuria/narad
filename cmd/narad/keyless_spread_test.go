package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
)

// TestKeylessProduceSpreadsEachTopicWhenTopicsInterleave drives the real
// HTTP produce path (handler, AcceptProduce, the node's partitioner, the
// ingress WAL and the dispatcher) with one client that writes three
// topics of 3 partitions in a fixed rotation, without keys. Each topic
// must spread its messages evenly over all its partitions. A round-robin
// cursor shared by every topic on the node gave topic A every third
// value, so every A message landed on one partition.
func TestKeylessProduceSpreadsEachTopicWhenTopicsInterleave(t *testing.T) {
	env := newCLITestEnv(t)
	ctx := context.Background()

	const (
		partitions = 3
		rounds     = 30
	)
	topics := []string{"spread-a", "spread-b", "spread-c"}
	for _, name := range topics {
		if _, err := env.broker.CreateTopic(ctx, brokertopics.CreateOpts{Name: name, Partitions: partitions}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if err := waitForAssignments(env.store, name); err != nil {
			t.Fatalf("assignments of %s: %v", name, err)
		}
	}

	client := env.server.Client()
	for i := range rounds {
		for _, name := range topics {
			resp, err := client.Post(env.server.URL+"/v1/topics/"+name+"/produce", "application/json",
				strings.NewReader(fmt.Sprintf(`{"round":%d}`, i)))
			if err != nil {
				t.Fatalf("produce %s round %d: %v", name, i, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("produce %s round %d: status %d", name, i, resp.StatusCode)
			}
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for _, name := range topics {
		for {
			details, err := env.broker.GetTopicDetails(ctx, name)
			if err != nil {
				t.Fatalf("details of %s: %v", name, err)
			}
			counts := make([]int64, partitions)
			var total int64
			for _, p := range details.Partitions {
				counts[p.Index] = p.HighWatermark
				total += p.HighWatermark
			}
			if total == rounds {
				for p, n := range counts {
					if n != rounds/partitions {
						t.Fatalf("topic %s per-partition counts %v: partition %d holds %d, want %d each",
							name, counts, p, n, rounds/partitions)
					}
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("topic %s: %d of %d messages committed (per partition %v)", name, total, rounds, counts)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
