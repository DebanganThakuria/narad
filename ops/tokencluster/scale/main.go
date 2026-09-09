// Command scale measures what token storage actually costs.
//
// It creates N sparse topics, parks one consumer per topic on a single
// node, and reads the heap on an OWNER node before and after. Every
// parked consumer leaves one token at each remote owner, so the delta
// divided by N is the real per-token cost including map overhead,
// the per-topic dispatch state it forces into existence, and the
// inbound RPC bookkeeping — not just the size of the struct.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var client = &http.Client{Timeout: 90 * time.Second}

func metric(node, name string) float64 {
	resp, err := client.Get(node + "/metrics")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
			return v
		}
	}
	return 0
}

func snapshot(nodes []string, label string) {
	fmt.Printf("  %-22s", label)
	for i, n := range nodes {
		fmt.Printf(" n%d=%5.1fMB/%5dg", i+1, metric(n, "go_memstats_heap_inuse_bytes")/(1<<20),
			int(metric(n, "go_goroutines")))
	}
	fmt.Println()
}

func main() {
	var nodesCSV string
	var topics, parallel int
	flag.StringVar(&nodesCSV, "nodes", "http://127.0.0.1:17942,http://127.0.0.1:17943,http://127.0.0.1:17944,http://127.0.0.1:17945", "")
	flag.IntVar(&topics, "topics", 2000, "sparse topics to create")
	flag.IntVar(&parallel, "parallel", 64, "concurrent topic creations")
	flag.Parse()
	nodes := strings.Split(nodesCSV, ",")

	fmt.Printf("scale: %d sparse topics, one parked consumer each\n\n", topics)
	snapshot(nodes, "baseline")

	// --- create topics ---
	t0 := time.Now()
	var created atomic.Int64
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := range topics {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			body := strings.NewReader(fmt.Sprintf(`{"name":"sc-%d","partitions":3}`, i))
			req, _ := http.NewRequest("POST", nodes[i%len(nodes)]+"/v1/topics", body)
			req.Header.Set("content-type", "application/json")
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode < 300 {
					created.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("\n  created %d topics in %.1fs\n\n", created.Load(), time.Since(t0).Seconds())
	time.Sleep(5 * time.Second)
	snapshot(nodes, "topics, no tokens")

	// --- park one consumer per topic, all on node 1 ---
	ctx, cancel := context.WithCancel(context.Background())
	var parked atomic.Int64
	var pwg sync.WaitGroup
	for i := range topics {
		pwg.Add(1)
		go func(i int) {
			defer pwg.Done()
			url := fmt.Sprintf("%s/v1/topics/sc-%d/consume?wait=25s", nodes[0], i)
			req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
			parked.Add(1)
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}(i)
		if i%200 == 0 {
			time.Sleep(150 * time.Millisecond) // spread the registration burst
		}
	}

	// Let every consumer register its tokens with the other three nodes.
	time.Sleep(12 * time.Second)
	fmt.Printf("\n  %d consumers parked; each holds a token on every remote owner\n\n", parked.Load())
	snapshot(nodes, "tokens registered")

	base := metric(nodes[1], "go_memstats_heap_inuse_bytes")
	cancel()
	pwg.Wait()
	time.Sleep(10 * time.Second)
	snapshot(nodes, "after consumers left")

	fmt.Printf("\n  owner-side heap with %d tokens: %.1f MB\n", topics, base/(1<<20))
	fmt.Printf("  extrapolated 10k topics: %.1f MB\n", base/(1<<20)*10000/float64(topics))
	fmt.Printf("  extrapolated 50k topics: %.1f MB\n", base/(1<<20)*50000/float64(topics))
	fmt.Println("\n  (extrapolation is linear and therefore generous: it carries the\n" +
		"   fixed baseline into every multiple. Read the per-token delta below.)")
}
