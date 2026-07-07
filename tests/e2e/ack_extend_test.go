package e2e

// End-to-end coverage for lease operations on the ack endpoint:
// extend=true renews the visibility window (slow-consumer heartbeat),
// extend=0 releases the reservation for immediate redelivery (nack).

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

// createShortVisibilityTopic creates a topic whose reservations expire
// after visibilityMs, so lease tests don't wait out the 30s default.
func createShortVisibilityTopic(t *testing.T, e *env, name string, visibilityMs int64) {
	t.Helper()
	resp := jsonReq(t, http.MethodPost, e.url("/v1/topics"), map[string]any{
		"name":                  name,
		"partitions":            3,
		"visibility_timeout_ms": visibilityMs,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create %s: got %d body=%s", name, resp.StatusCode, readBody(resp))
	}
	if !e.awaitPartitionAssignments(name, 3) {
		t.Fatalf("timed out waiting for %s assignments", name)
	}
}

func ackWith(t *testing.T, e *env, topicName, handle, extend string) *http.Response {
	t.Helper()
	u := e.url("/v1/topics/" + topicName + "/ack?receipt_handle=" + url.QueryEscape(handle))
	if extend != "" {
		u += "&extend=" + extend
	}
	return jsonReq(t, http.MethodPost, u, nil)
}

// A consumer that outlives the visibility window survives by extending:
// no redelivery happens past the original deadline, and the original
// handle still acks.
func TestAckExtendKeepsLeaseAlive(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	createShortVisibilityTopic(t, e, "lease-extend", 3000)

	mustProduce(t, e, "lease-extend", "k", map[string]any{"v": 1})
	msg, found := mustConsume(t, e, "lease-extend", consumeQuery{Wait: "2s"})
	if !found {
		t.Fatal("no message consumed")
	}

	// Renew the lease halfway through the window.
	time.Sleep(1500 * time.Millisecond)
	resp := ackWith(t, e, "lease-extend", msg.ReceiptHandle, "true")
	expectStatus(t, resp, http.StatusNoContent)

	// Past the ORIGINAL deadline, still within the extended one: the
	// message must not have been redelivered.
	time.Sleep(2100 * time.Millisecond)
	if redelivered, found := mustConsume(t, e, "lease-extend", consumeQuery{}); found {
		t.Fatalf("message redelivered despite extension: %+v", redelivered)
	}

	// The same handle acks after the extension.
	resp = ackWith(t, e, "lease-extend", msg.ReceiptHandle, "")
	expectStatus(t, resp, http.StatusNoContent)
}

// Extending after the window lapsed is rejected with 410 — the lease
// is gone and the message may already belong to someone else.
func TestAckExtendAfterExpiryIsGone(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	createShortVisibilityTopic(t, e, "lease-late", 1000)

	mustProduce(t, e, "lease-late", "k", map[string]any{"v": 1})
	msg, found := mustConsume(t, e, "lease-late", consumeQuery{Wait: "2s"})
	if !found {
		t.Fatal("no message consumed")
	}

	time.Sleep(1200 * time.Millisecond)
	resp := ackWith(t, e, "lease-late", msg.ReceiptHandle, "true")
	expectStatus(t, resp, http.StatusGone)
}

// extend=0 nacks: the message is redeliverable immediately under a new
// handle, and the old handle is dead.
func TestAckNackRedeliversImmediately(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	createShortVisibilityTopic(t, e, "lease-nack", 30_000)

	mustProduce(t, e, "lease-nack", "k", map[string]any{"v": 1})
	first, found := mustConsume(t, e, "lease-nack", consumeQuery{Wait: "2s"})
	if !found {
		t.Fatal("no message consumed")
	}

	resp := ackWith(t, e, "lease-nack", first.ReceiptHandle, "0")
	expectStatus(t, resp, http.StatusNoContent)

	// Immediately redeliverable — no visibility timeout wait.
	second, found := mustConsume(t, e, "lease-nack", consumeQuery{Wait: "2s"})
	if !found {
		t.Fatal("nacked message was not redelivered")
	}
	if second.Partition != first.Partition || second.Offset != first.Offset {
		t.Fatalf("redelivered a different message: first=%d/%d second=%d/%d",
			first.Partition, first.Offset, second.Partition, second.Offset)
	}
	if second.ReceiptHandle == first.ReceiptHandle {
		t.Fatal("redelivery reused the nacked receipt handle")
	}

	// The nacked handle is dead; the fresh one acks.
	resp = ackWith(t, e, "lease-nack", first.ReceiptHandle, "")
	expectStatus(t, resp, http.StatusGone)
	resp = ackWith(t, e, "lease-nack", second.ReceiptHandle, "")
	expectStatus(t, resp, http.StatusNoContent)
}

func TestAckRejectsUnknownExtendValue(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	createShortVisibilityTopic(t, e, "lease-bad", 30_000)

	mustProduce(t, e, "lease-bad", "k", map[string]any{"v": 1})
	msg, found := mustConsume(t, e, "lease-bad", consumeQuery{Wait: "2s"})
	if !found {
		t.Fatal("no message consumed")
	}
	resp := ackWith(t, e, "lease-bad", msg.ReceiptHandle, "later")
	expectBadRequest(t, resp)
}
