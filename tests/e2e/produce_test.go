package e2e

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestProduce_AssignsOffsetAndPartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "produce-basic", Partitions: 4})

	got := mustProduce(t, env, "produce-basic", "", map[string]string{"hello": "world"})
	if got.Offset != 0 {
		t.Errorf("first offset: got %d want 0", got.Offset)
	}
	if got.Partition < 0 || got.Partition > 3 {
		t.Errorf("partition: got %d want in [0,4)", got.Partition)
	}
}

func TestProduce_KeyPinsToOnePartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "key-pin", Partitions: 8})

	first := mustProduce(t, env, "key-pin", "stable-key", map[string]int{"i": 1})
	for i := 2; i <= 5; i++ {
		got := mustProduce(t, env, "key-pin", "stable-key", map[string]int{"i": i})
		if got.Partition != first.Partition {
			t.Errorf("produce %d landed on partition %d; want %d (key pinning)",
				i, got.Partition, first.Partition)
		}
	}
}

// TestProduce_OffsetsAreMonotonicPerPartition confirms that within
// each partition, successive produces get strictly increasing offsets
// starting at 0.
func TestProduce_OffsetsAreMonotonicPerPartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "monotonic", Partitions: 3})

	perPartition := make(map[int][]int64)
	for i := range 20 {
		got := mustProduce(t, env, "monotonic", "", map[string]int{"i": i})
		perPartition[got.Partition] = append(perPartition[got.Partition], got.Offset)
	}

	for p, offsets := range perPartition {
		for i, off := range offsets {
			if off != int64(i) {
				t.Errorf("partition %d offset[%d]: got %d want %d", p, i, off, i)
			}
		}
	}
}

func TestProduce_AcceptsArrayMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "arr"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/arr/produce",
		map[string]any{"message": []int{1, 2, 3}})
	expectStatus(t, resp, http.StatusAccepted)
}

func TestProduce_AcceptsStringMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "str"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/str/produce",
		map[string]any{"message": "hello"})
	expectStatus(t, resp, http.StatusAccepted)
}

// TestProduce_AcceptsEmptyStringMessage documents current behaviour:
// an empty JSON string ("") is technically valid JSON, so the produce
// handler accepts it. If we ever want to reject zero-content payloads
// the policy should live in handlers/produce.go's Validate, not here.
func TestProduce_AcceptsEmptyStringMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "empty-str"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/empty-str/produce",
		map[string]any{"message": ""})
	expectStatus(t, resp, http.StatusAccepted)
}

func TestProduce_RejectsMissingMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "no-msg"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/no-msg/produce",
		map[string]any{"key": "k"})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestProduce_IgnoresUnknownFields(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "extra"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/extra/produce",
		map[string]any{"message": map[string]string{"v": "1"}, "garbage": true})
	expectStatus(t, resp, http.StatusAccepted)
}

func TestProduce_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/bad-json/produce",
		[]byte("{not valid"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestProduce_NotFoundForUnknownTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/never-created/produce",
		map[string]any{"message": map[string]string{"x": "1"}})
	expectStatus(t, resp, http.StatusNotFound)
}

// TestProduce_RejectsOversizedBody confirms the 1MiB request cap. The
// MaxBytesReader wrapping the body fires while reading and the handler
// maps the size-limit error to 413 Request Entity Too Large.
func TestProduce_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "big"})

	// Construct a > 1MiB JSON body. We escape one quote inside the
	// string so the result remains valid JSON, then pad with 'A's.
	huge := strings.Builder{}
	huge.WriteString(`{"message":"`)
	huge.WriteString(strings.Repeat("A", (1<<20)+1024))
	huge.WriteString(`"}`)

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/big/produce",
		[]byte(huge.String()))
	expectStatus(t, resp, http.StatusRequestEntityTooLarge)
}

// TestProduce_ConcurrentProducersReturnUniqueMessageIDs stress-tests the
// HTTP accept path under concurrent pressure. Every accepted produce
// should return a unique ingress message ID.
//
// Run with -race for additional coverage.
func TestProduce_ConcurrentProducersReturnUniqueMessageIDs(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "race", Partitions: 4})

	const (
		writers     = 10
		perWriter   = 50
		totalExpect = writers * perWriter
	)

	type slot struct {
		id string
		p  int
	}
	results := make(chan slot, totalExpect)

	var wg sync.WaitGroup
	var failed atomic.Int32
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range perWriter {
				resp := jsonReq(t, http.MethodPost,
					env.Server.URL+"/v1/topics/race/produce",
					map[string]any{"message": map[string]int{"w": w, "i": i}})
				if resp.StatusCode != http.StatusAccepted {
					failed.Add(1)
					_ = resp.Body.Close()
					continue
				}
				var pr produceResult
				decodeJSON(t, resp, &pr)
				results <- slot{id: pr.MessageID, p: pr.Partition}
			}
		}(w)
	}
	wg.Wait()
	close(results)

	if failed.Load() > 0 {
		t.Fatalf("%d produce calls failed under concurrency", failed.Load())
	}

	seen := make(map[string]struct{}, totalExpect)
	for s := range results {
		if s.id == "" {
			t.Fatal("empty message_id returned to producer")
		}
		if s.p < 0 || s.p >= 4 {
			t.Fatalf("partition=%d, want in [0,4)", s.p)
		}
		if _, dup := seen[s.id]; dup {
			t.Fatalf("duplicate message_id %q returned to two producers", s.id)
		}
		seen[s.id] = struct{}{}
	}
	if len(seen) != totalExpect {
		t.Errorf("unique results: got %d want %d", len(seen), totalExpect)
	}
}
