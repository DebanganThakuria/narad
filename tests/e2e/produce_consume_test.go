package e2e

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestProduceAndConsume(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("prodcon", 3, 0)

	off, part := env.produce("prodcon", "k1", `{"val": 1}`)
	if off != 0 {
		t.Fatalf("first offset: got %d, want 0", off)
	}
	if part < 0 || part >= 2 {
		t.Fatalf("partition: got %d, want 0..1", part)
	}

	resp := env.get("/v1/topics/prodcon/consume?partition=" + strconv.Itoa(part))
	expectOK(t, resp)

	msg := readJSON[topic.Message](t, resp)
	if msg.Topic != "prodcon" {
		t.Fatalf("msg topic: got %q, want prodcon", msg.Topic)
	}
	if msg.Offset != 0 {
		t.Fatalf("msg offset: got %d, want 0", msg.Offset)
	}
}

func TestConsumeLongPoll(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("longpoll", 3, 0)

	produced := make(chan struct{})
	go func() {
		defer close(produced)
		time.Sleep(200 * time.Millisecond)
		env.produce("longpoll", "k", `{"msg": "late"}`)
	}()

	resp := env.get("/v1/topics/longpoll/consume?wait=2s")
	expectOK(t, resp)
	select {
	case <-produced:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed producer")
	}
}

func TestConsumeWithExplicitOffset(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("replay", 3, 0)

	env.produce("replay", "k1", `{"n": 1}`)
	env.produce("replay", "k2", `{"n": 2}`)

	// Replay from offset 0.
	resp := env.get("/v1/topics/replay/consume?partition=0&offset=0")
	expectOK(t, resp)

	msg := readJSON[topic.Message](t, resp)
	if msg.Offset != 0 {
		t.Fatalf("replay offset: got %d, want 0", msg.Offset)
	}
}

func TestConsumeEmptyTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("empty", 3, 0)

	// Immediate consume on empty topic with no wait returns 204 (no message yet).
	resp := env.get("/v1/topics/empty/consume")
	expectStatus(t, resp, http.StatusNoContent)
}

func TestAck(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("ack-topic", 3, 0)
	env.produce("ack-topic", "k", `{"x": 1}`)

	msg := env.consume("/v1/topics/ack-topic/consume")
	if msg.ReceiptHandle == "" {
		t.Fatal("expected receipt_handle in consume response")
	}
	env.ack("ack-topic", msg.ReceiptHandle)

	// Next consume should skip the acked message and return 204 (no wait).
	resp := env.get("/v1/topics/ack-topic/consume")
	expectStatus(t, resp, http.StatusNoContent)
}

func TestConsumeMultipleThenAck(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("multi-ack", 3, 0)
	env.produce("multi-ack", "k", `{"seq": 0}`)
	env.produce("multi-ack", "k", `{"seq": 1}`)

	msg1 := env.consume("/v1/topics/multi-ack/consume?partition=0")
	if msg1.Offset != 0 {
		t.Fatalf("first offset: got %d, want 0", msg1.Offset)
	}
	env.ack("multi-ack", msg1.ReceiptHandle)

	msg2 := env.consume("/v1/topics/multi-ack/consume?partition=0")
	if msg2.Offset != 1 {
		t.Fatalf("second offset: got %d, want 1", msg2.Offset)
	}
}

func TestProduceNonExistentTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := env.rawPost("/v1/topics/no-such/produce", `{}`)
	expectNotFound(t, resp)
}

func TestProduceAcceptsInvalidJSONWithoutSchema(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("produce-bad", 3, 0)

	resp := env.rawPost("/v1/topics/produce-bad/produce?key=k", `{not-json`)
	expectStatus(t, resp, http.StatusAccepted)
	_ = resp.Body.Close()
}
