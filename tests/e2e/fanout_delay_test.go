package e2e

// End-to-end coverage for DELAY children: records fan out only after
// parentCommitTime + delay, the parent's retention must buffer the
// delay, direct produce to a delayed child is forbidden, and detach
// clears the delay.

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func attachChildWithDelay(t *testing.T, e *env, parent, child string, delayMs int64) *http.Response {
	t.Helper()
	return e.post("/v1/topics/"+parent+"/children", map[string]any{"child": child, "delay_ms": delayMs})
}

func TestFanoutDelayChildEndToEnd(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("dly-parent", 3, 0) // env default retention: 7d
	e.createTopic("dly-child", 3, 0)

	const delayMs = 1500
	resp := attachChildWithDelay(t, e, "dly-parent", "dly-child", delayMs)
	expectOK(t, resp)

	// Describe and the children listing both report the delay.
	resp = e.get("/v1/topics/dly-child")
	expectOK(t, resp)
	child := readJSON[struct {
		FanoutDelayMs int64 `json:"fanout_delay_ms"`
	}](t, resp)
	if child.FanoutDelayMs != delayMs {
		t.Fatalf("child fanout_delay_ms = %d, want %d", child.FanoutDelayMs, delayMs)
	}
	awaitCursorsAnchored(t, e, "dly-parent", "dly-child")
	resp = e.get("/v1/topics/dly-parent/children")
	expectOK(t, resp)
	listing := readJSON[struct {
		Children []struct {
			Name    string `json:"name"`
			DelayMs int64  `json:"delay_ms"`
		} `json:"children"`
	}](t, resp)
	if len(listing.Children) != 1 || listing.Children[0].DelayMs != delayMs {
		t.Fatalf("children listing = %+v, want delay_ms %d", listing.Children, delayMs)
	}

	// Direct produce to a delayed child is forbidden.
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/dly-child/produce"), []byte(`{"v":1}`))
	expectStatus(t, resp, http.StatusConflict)

	// Produce to the parent: nothing reaches the child before the
	// delay, everything (keys intact) after.
	produced := time.Now()
	for i := range 3 {
		mustProduce(t, e, "dly-parent", fmt.Sprintf("k%d", i), map[string]any{"seq": i})
	}
	time.Sleep(400 * time.Millisecond)
	if got := totalCommitted(t, e, "dly-child"); got != 0 {
		t.Fatalf("child received %d records before the delay elapsed", got)
	}
	awaitFanout(t, e, "dly-child", 3)
	if elapsed := time.Since(produced); elapsed < delayMs*time.Millisecond {
		t.Fatalf("delivered after %v, before the %dms delay", elapsed, delayMs)
	}
	for key, payloads := range childPayloads(t, e, "dly-child") {
		if key == "" {
			t.Fatalf("delayed record lost its key: %v", payloads)
		}
	}

	// Detach clears the delay: direct produce works again.
	resp = e.del("/v1/topics/dly-parent/children/dly-child")
	expectStatus(t, resp, http.StatusNoContent)
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/dly-child/produce"), []byte(`{"v":2}`))
	expectStatus(t, resp, http.StatusAccepted)
}

func TestFanoutDelayRetentionInvariant(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("dret-parent", 3, 7_200_000) // 2h: buffers delays up to 1h
	e.createTopic("dret-child", 3, 0)

	// A delay the retention cannot buffer is rejected at attach.
	resp := attachChildWithDelay(t, e, "dret-parent", "dret-child", 3_600_001)
	expectConflict(t, resp)
	// A negative delay is a bad request.
	resp = attachChildWithDelay(t, e, "dret-parent", "dret-child", -5)
	expectBadRequest(t, resp)

	// A fitting delay attaches...
	resp = attachChildWithDelay(t, e, "dret-parent", "dret-child", 1500)
	expectOK(t, resp)

	// ...and now the parent's retention cannot shrink below what the
	// child's delay requires (1500ms + the 1h floor > 1h).
	resp = e.patch("/v1/topics/dret-parent", map[string]any{"retention_ms": int64(3_600_000)})
	expectConflict(t, resp)
	// Shrinking to something that still buffers the delay is fine.
	resp = e.patch("/v1/topics/dret-parent", map[string]any{"retention_ms": int64(3_700_000)})
	expectOK(t, resp)

	// After detach the parent's retention is free again.
	resp = e.del("/v1/topics/dret-parent/children/dret-child")
	expectStatus(t, resp, http.StatusNoContent)
	resp = e.patch("/v1/topics/dret-parent", map[string]any{"retention_ms": int64(3_600_000)})
	expectOK(t, resp)
}
