package e2e

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/topic"
)

func TestProduceAndConsume(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("prodcon", 2, 2, topic.Retention{})

	off, part := env.produce("prodcon", "k1", `{"val": 1}`)
	if off != 0 {
		t.Fatalf("first offset: got %d, want 0", off)
	}
	if part < 0 || part >= 2 {
		t.Fatalf("partition: got %d, want 0..1", part)
	}

	// Consume the message
	resp := env.get("/v1/topics/prodcon/consume?partition=" + strconv.Itoa(part))
	expectOK(t, resp)

	msg := readJSON[struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}](t, resp)

	if msg.Topic != "prodcon" {
		t.Fatalf("msg topic: got %q, want prodcon", msg.Topic)
	}
	if msg.Offset != 0 {
		t.Fatalf("msg offset: got %d, want 0", msg.Offset)
	}
}

func TestConsumeLongPoll(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("longpoll", 1, 2, topic.Retention{})

	go func() {
		time.Sleep(200 * time.Millisecond)
		env.produce("longpoll", "k", `{"msg": "late"}`)
	}()

	resp := env.get("/v1/topics/longpoll/consume?wait=2s")
	expectOK(t, resp)
}

func TestConsumeWithExplicitOffset(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("replay", 1, 2, topic.Retention{})

	env.produce("replay", "k1", `{"n": 1}`)
	env.produce("replay", "k2", `{"n": 2}`)

	// Replay from offset 0
	resp := env.get("/v1/topics/replay/consume?partition=0&offset=0")
	expectOK(t, resp)

	var msg struct {
		Offset int64 `json:"offset"`
	}
	msg = readJSON[struct {
		Offset int64 `json:"offset"`
	}](t, resp)

	if msg.Offset != 0 {
		t.Fatalf("replay offset: got %d, want 0", msg.Offset)
	}
}

func TestConsumeEmptyTopic(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("empty", 1, 2, topic.Retention{})

	// Immediate consume on empty topic with no wait returns 204 (no message yet).
	resp := env.get("/v1/topics/empty/consume")
	expectStatus(t, resp, 204)
}

func TestAck(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("ack-topic", 1, 2, topic.Retention{})
	off, part := env.produce("ack-topic", "k", `{"x": 1}`)

	resp := env.post("/v1/topics/ack-topic/ack", map[string]any{
		"partition": part,
		"offset":    off,
	})
	expectStatus(t, resp, http.StatusNoContent)

	// Next consume should skip the acked message and return 204 (no wait).
	resp = env.get("/v1/topics/ack-topic/consume")
	expectStatus(t, resp, http.StatusNoContent)
}

func TestConsumeMultipleThenAck(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("multi-ack", 1, 2, topic.Retention{})
	env.produce("multi-ack", "k", `{"seq": 0}`)
	env.produce("multi-ack", "k", `{"seq": 1}`)

	// Consume first
	resp := env.get("/v1/topics/multi-ack/consume?partition=0")
	expectOK(t, resp)
	msg1 := readJSON[struct {
		Offset int64 `json:"offset"`
	}](t, resp)

	if msg1.Offset != 0 {
		t.Fatalf("first offset: got %d, want 0", msg1.Offset)
	}

	// Ack first
	resp = env.post("/v1/topics/multi-ack/ack", map[string]any{"partition": 0, "offset": 0})
	expectStatus(t, resp, http.StatusNoContent)

	// Consume second
	resp = env.get("/v1/topics/multi-ack/consume?partition=0")
	expectOK(t, resp)
	msg2 := readJSON[struct {
		Offset int64 `json:"offset"`
	}](t, resp)

	if msg2.Offset != 1 {
		t.Fatalf("second offset: got %d, want 1", msg2.Offset)
	}
}

func TestProduceNonExistentTopic(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.post("/v1/topics/no-such/produce", map[string]any{
		"message": json.RawMessage(`{}`),
	})
	expectNotFound(t, resp)
}

func TestProduceInvalidJSON(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("produce-bad", 1, 2, topic.Retention{})

	// Send a raw body with an unquoted string, which is not valid JSON.
	resp := env.rawPost("/v1/topics/produce-bad/produce", `{"key": "k", "message": not-json}`)
	expectBadRequest(t, resp)
}

