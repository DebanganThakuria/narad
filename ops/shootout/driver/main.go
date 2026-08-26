// Command driver pushes an identical workload at whichever broker the
// shootout compose file has up, and reports produce and consume+ack
// throughput in a comparable JSON shape.
//
// Fairness rules (see ../README.md for the full table):
//   - every produce waits for the system's strongest per-message
//     confirmation its idiomatic client offers (HTTP 202, JetStream
//     PubAck, publisher confirm, acks=all, XADD reply);
//   - every consume is followed by an explicit ack/commit;
//   - each system runs its DEFAULT durable configuration — what that
//     confirmation actually promises differs per system and is
//     reported in the durability field, not hidden.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type target interface {
	name() string
	durability() string
	setup(ctx context.Context) error
	produceOne(ctx context.Context, payload []byte) error
	// consumeSome fetches up to batch messages and acks each; returns
	// how many were acked.
	consumeSome(ctx context.Context, batch int) (int, error)
	close()
}

type result struct {
	System            string  `json:"system"`
	Count             int     `json:"count"`
	PayloadBytes      int     `json:"payload_bytes"`
	Workers           int     `json:"workers"`
	ProduceMsgsPerSec float64 `json:"produce_msgs_per_sec"`
	ProduceP50Ms      float64 `json:"produce_p50_ms"`
	ProduceP99Ms      float64 `json:"produce_p99_ms"`
	ProduceFailed     int64   `json:"produce_failed"`
	ConsumeMsgsPerSec float64 `json:"consume_msgs_per_sec"`
	Consumed          int64   `json:"consumed"`
	Durability        string  `json:"durability"`
}

func main() {
	var (
		system  = flag.String("system", "", "narad | nats | rabbitmq | redis | kafka")
		count   = flag.Int("count", 50000, "messages to produce")
		size    = flag.Int("size", 256, "payload bytes")
		workers = flag.Int("workers", 16, "concurrent workers (produce and consume)")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	var t target
	var err error
	switch *system {
	case "narad":
		t, err = newNarad("http://127.0.0.1:17942")
	case "nats":
		t, err = newNATS("nats://127.0.0.1:14222")
	case "rabbitmq":
		t, err = newRabbit("amqp://guest:guest@127.0.0.1:15672/")
	case "redis":
		t, err = newRedis("127.0.0.1:16379")
	case "kafka":
		t, err = newKafka("localhost:19092")
	case "pulsar":
		t, err = newPulsar("pulsar://127.0.0.1:16650")
	default:
		fmt.Fprintln(os.Stderr, "unknown -system", *system)
		os.Exit(2)
	}
	if err != nil {
		fatal("connect %s: %v", *system, err)
	}
	defer t.close()
	if err := t.setup(ctx); err != nil {
		fatal("setup %s: %v", *system, err)
	}

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	// Produce phase: workers pull from a shared counter, each op waits
	// for the per-message confirmation.
	lat := make([]time.Duration, *count)
	var idx, failed int64
	pstart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1) - 1
				if i >= int64(*count) {
					return
				}
				t0 := time.Now()
				if err := t.produceOne(ctx, payload); err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				lat[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	pelapsed := time.Since(pstart)
	okCount := int64(*count) - failed

	// Consume phase: drain everything we produced.
	var consumed int64
	cstart := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			empties := 0
			for ctx.Err() == nil && atomic.LoadInt64(&consumed) < okCount {
				n, err := t.consumeSome(ctx, 64)
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				if n == 0 {
					empties++
					if empties >= 10 {
						return
					}
					continue
				}
				empties = 0
				atomic.AddInt64(&consumed, int64(n))
			}
		}()
	}
	wg.Wait()
	celapsed := time.Since(cstart)

	valid := lat[:0]
	for _, l := range lat {
		if l > 0 {
			valid = append(valid, l)
		}
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i] < valid[j] })
	pct := func(p float64) float64 {
		if len(valid) == 0 {
			return 0
		}
		return float64(valid[int(p*float64(len(valid)-1))].Microseconds()) / 1000
	}

	r := result{
		System:            t.name(),
		Count:             *count,
		PayloadBytes:      *size,
		Workers:           *workers,
		ProduceMsgsPerSec: float64(okCount) / pelapsed.Seconds(),
		ProduceP50Ms:      pct(0.50),
		ProduceP99Ms:      pct(0.99),
		ProduceFailed:     failed,
		ConsumeMsgsPerSec: float64(consumed) / celapsed.Seconds(),
		Consumed:          consumed,
		Durability:        t.durability(),
	}
	out, _ := json.Marshal(r)
	fmt.Println(string(out))
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %d produce failures\n", failed)
	}
	if consumed < okCount {
		fmt.Fprintf(os.Stderr, "WARNING: consumed %d of %d\n", consumed, okCount)
		os.Exit(1)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL "+format+"\n", args...)
	os.Exit(1)
}
