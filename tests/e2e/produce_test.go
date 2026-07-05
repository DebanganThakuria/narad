package e2e

import (
	"fmt"
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

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/arr/produce", []byte(`[1,2,3]`))
	expectStatus(t, resp, http.StatusAccepted)
	_ = resp.Body.Close()
}

func TestProduce_AcceptsStringMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "str"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/str/produce", []byte(`hello`))
	expectStatus(t, resp, http.StatusAccepted)
	_ = resp.Body.Close()
}

// TestProduce_AcceptsEmptyStringMessage documents that a non-empty raw
// body is accepted even if the logical payload is an empty JSON string.
func TestProduce_AcceptsEmptyStringMessage(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "empty-str"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/empty-str/produce", []byte(`""`))
	expectStatus(t, resp, http.StatusAccepted)
	_ = resp.Body.Close()
}

func TestProduce_RejectsEmptyBody(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "no-msg"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/no-msg/produce?key=k", nil)
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestProduce_IgnoresUnknownQueryParams(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "extra"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/extra/produce?garbage=true", []byte(`{"v":"1"}`))
	expectStatus(t, resp, http.StatusAccepted)
	_ = resp.Body.Close()
}

func TestProduce_AcceptsInvalidJSONWithoutSchema(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/bad-json/produce",
		[]byte("{not valid"))
	expectStatus(t, resp, http.StatusAccepted)
}

func TestProduce_NotFoundForUnknownTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/never-created/produce", []byte(`{"x":"1"}`))
	expectStatus(t, resp, http.StatusNotFound)
}

// TestProduce_RejectsOversizedBody confirms the 1MiB request cap. The
// MaxBytesReader wrapping the body fires while reading and the handler
// maps the size-limit error to 413 Request Entity Too Large.
func TestProduce_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "big"})

	huge := []byte(strings.Repeat("A", (1<<20)+1024))
	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/big/produce", huge)
	expectStatus(t, resp, http.StatusRequestEntityTooLarge)
}

// TestProduce_ConcurrentProducersAcceptedAndVisible stress-tests the HTTP
// accept path under concurrent pressure. Every accepted produce should
// eventually become visible even though the hot response has no body.
//
// Run with -race for additional coverage.
func TestProduce_ConcurrentProducersAcceptedAndVisible(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "race", Partitions: 4})

	const (
		writers     = 10
		perWriter   = 50
		totalExpect = writers * perWriter
	)

	before := topicNextOffsets(t, env, "race")
	var wg sync.WaitGroup
	var failed atomic.Int32
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				resp := rawReq(t, http.MethodPost,
					env.Server.URL+"/v1/topics/race/produce",
					fmt.Appendf(nil, `{"w":%d,"i":%d}`, w, i))
				if resp.StatusCode != http.StatusAccepted {
					failed.Add(1)
				}
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()

	if failed.Load() > 0 {
		t.Fatalf("%d produce calls failed under concurrency", failed.Load())
	}
	waitForVisibleDelta(t, env, "race", before, totalExpect)
}
