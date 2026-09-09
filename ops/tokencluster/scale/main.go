// Command scale answers what thousands of sparse topics actually cost.
//
// It runs INSIDE the cluster's docker network on purpose. Driving this
// from the host through Colima's userspace port-forwarder wedged the
// forwarder and took the Docker daemon down at ~2000 concurrent
// connections — not an OOM, and nothing to do with narad. Talking to
// narad-1:7942 directly removes that hop entirely.
//
// It reports goroutines, heap and CPU at each step, so the question
// "which of these scale linearly with topics" is answered with numbers
// rather than inference.
package main

import (
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

var client = &http.Client{Timeout: 120 * time.Second}

func metric(node, name string) float64 {
	resp, err := client.Get("http://" + node + ":7942/metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, _ := strconv.ParseFloat(strings.TrimSpace(line[len(name)+1:]), 64)
			return v
		}
	}
	return -1
}

type sample struct {
	goroutines, heapMB, cpu float64
}

func take(node string) sample {
	return sample{
		goroutines: metric(node, "go_goroutines"),
		heapMB:     metric(node, "go_memstats_heap_inuse_bytes") / (1 << 20),
		cpu:        metric(node, "process_cpu_seconds_total"),
	}
}

func createTopics(nodes []string, from, to, parallel int) (int, time.Duration) {
	t0 := time.Now()
	var ok atomic.Int64
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := from; i < to; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			body := strings.NewReader(fmt.Sprintf(`{"name":"sc-%d","partitions":3}`, i))
			req, _ := http.NewRequest("POST", "http://"+nodes[i%len(nodes)]+":7942/v1/topics", body)
			req.Header.Set("content-type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				ok.Add(1)
			}
		}(i)
	}
	wg.Wait()
	return int(ok.Load()), time.Since(t0)
}

// touch opens each topic's partition logs by consuming once with no
// wait. Creating a topic is only metadata; the logs (and whatever they
// cost) do not exist until something reads or writes them, and that
// distinction is most of the answer.
func touch(nodes []string, from, to, parallel int) {
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := from; i < to; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			for _, n := range nodes {
				resp, err := client.Get(fmt.Sprintf("http://%s:7942/v1/topics/sc-%d/consume?wait=0", n, i))
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}(i)
	}
	wg.Wait()
}

func report(label string, nodes []string, prev map[string]sample, window float64) map[string]sample {
	cur := map[string]sample{}
	fmt.Printf("  %-26s", label)
	for _, n := range nodes {
		s := take(n)
		cur[n] = s
		cpu := ""
		if p, ok := prev[n]; ok && window > 0 {
			cpu = fmt.Sprintf(" cpu=%4.1f%%", (s.cpu-p.cpu)/window*100)
		}
		fmt.Printf(" %s:%5.0fg/%6.1fMB%s", strings.TrimPrefix(n, "narad-"), s.goroutines, s.heapMB, cpu)
	}
	fmt.Println()
	return cur
}

func main() {
	var nodesCSV, steps string
	var parallel int
	flag.StringVar(&nodesCSV, "nodes", "narad-1,narad-2,narad-3,narad-4", "")
	flag.StringVar(&steps, "steps", "1000,5000,10000", "cumulative topic counts")
	flag.IntVar(&parallel, "parallel", 96, "")
	flag.Parse()
	nodes := strings.Split(nodesCSV, ",")

	fmt.Println("scale: sparse topics, 3 partitions each")
	fmt.Println()
	prev := report("baseline", nodes, nil, 0)

	created := 0
	for s := range strings.SplitSeq(steps, ",") {
		target, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			continue
		}
		n, took := createTopics(nodes, created, target, parallel)
		created = target
		fmt.Printf("\n  +%d topics in %.1fs (%.0f/s)\n", n, took.Seconds(), float64(n)/took.Seconds())

		time.Sleep(4 * time.Second)
		report(fmt.Sprintf("%d topics, logs closed", created), nodes, nil, 0)

		// Open every partition log: this is where per-log cost appears.
		touch(nodes, created-n, created, parallel)
		time.Sleep(4 * time.Second)
		before := report(fmt.Sprintf("%d topics, logs OPEN", created), nodes, nil, 0)

		// Idle CPU over a fixed window at this size.
		time.Sleep(20 * time.Second)
		prev = report(fmt.Sprintf("%d topics, idle 20s", created), nodes, before, 20)
	}
	_ = prev
	fmt.Println("\n  goroutines per open log = (goroutines at OPEN - baseline) / (topics * 3 partitions owned locally)")
}
