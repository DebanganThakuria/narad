package e2e

// Remote children end to end over HTTP: a source Narad replicates a
// topic to a target Narad through the target's public batch produce,
// every data-plane route refuses the stub, a stalled link says why in
// the listing, and a delete refuses to abandon unshipped records unless
// forced.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

var (
	oliviaPass = remoteRandomString(18)
	ritaPass   = remoteRandomString(18)
	bobPass    = remoteRandomString(18)
)

func (p *remotePair) attach(t *testing.T, extra map[string]any) map[string]any {
	t.Helper()
	body := map[string]any{"child": "orders-to-b", "remote": "b", "remote_topic": "orders"}
	for k, v := range extra {
		body[k] = v
	}
	status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children", body)
	if status != http.StatusCreated {
		t.Fatalf("attach: %d %s", status, out)
	}
	var stub map[string]any
	_ = json.Unmarshal(out, &stub)
	return stub
}

// listing reads the source's children listing as the admin.
func (p *remotePair) listing(t *testing.T, query string) map[string]any {
	t.Helper()
	status, out := p.src.admin(http.MethodGet, "/v1/topics/orders/children"+query, nil)
	if status != http.StatusOK {
		t.Fatalf("listing: %d %s", status, out)
	}
	var l struct {
		Children []map[string]any `json:"children"`
	}
	_ = json.Unmarshal(out, &l)
	for _, c := range l.Children {
		if c["name"] == "orders-to-b" {
			return c
		}
	}
	t.Fatalf("orders-to-b missing from the listing: %s", out)
	return nil
}

func (p *remotePair) waitState(t *testing.T, state string, d time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(d)
	var c map[string]any
	for time.Now().Before(deadline) {
		c = p.listing(t, "")
		if c["state"] == state {
			return c
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("state never became %s; last %v", state, c)
	return nil
}

// waitComplete waits until every parent partition's cursor reports.
func (p *remotePair) waitComplete(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p.listing(t, "")["lag_complete"] == true {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the lag never became complete")
}

// produce sends n keyed and keyless messages to the source over HTTP.
func (p *remotePair) produce(t *testing.T, n, base int) []string {
	t.Helper()
	var sent []string
	msgs := make([]map[string]any, 0, n)
	for i := range n {
		payload := fmt.Sprintf(`{"seq":%d}`, base+i)
		m := map[string]any{"payload": json.RawMessage(payload)}
		if i%3 != 0 {
			m["key"] = fmt.Sprintf("k-%d", i%5)
		}
		msgs = append(msgs, m)
		sent = append(sent, payload)
	}
	status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/produce/batch", map[string]any{"messages": msgs})
	if status != http.StatusAccepted {
		t.Fatalf("produce: %d %s", status, out)
	}
	return sent
}

// drainTarget consumes and acks every message on the target as bob.
func (p *remotePair) drainTarget(t *testing.T, want []string, d time.Duration) map[string]int {
	t.Helper()
	got := map[string]int{}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		status, out, _ := p.dst.call(http.MethodGet, "/v1/topics/orders/consume?wait=200ms&max=100", "bob", bobPass, nil)
		if status == http.StatusOK {
			var batch struct {
				Messages []topic.Message `json:"messages"`
			}
			_ = json.Unmarshal(out, &batch)
			for _, m := range batch.Messages {
				got[string(m.Payload)]++
				ack, _, _ := p.dst.call(http.MethodPost, "/v1/topics/orders/ack?receipt_handle="+url.QueryEscape(m.ReceiptHandle), "bob", bobPass, nil)
				if ack != http.StatusNoContent && ack != http.StatusOK {
					t.Fatalf("ack on the target: %d", ack)
				}
			}
		}
		complete := true
		for _, w := range want {
			if got[w] == 0 {
				complete = false
				break
			}
		}
		if complete {
			return got
		}
	}
	t.Fatalf("the target delivered %d distinct of %d messages", len(got), len(want))
	return nil
}

func TestRemoteChildEndToEnd(t *testing.T) {
	p := newRemotePair(t)
	stub := p.attach(t, nil)
	if stub["partitions"] != float64(0) || stub["role"] != "child" || stub["parent"] != "orders" {
		t.Fatalf("attach answer = %v, want the zero-partition stub", stub)
	}
	link := stub["remote"].(map[string]any)
	tt, _ := p.dst.store.GetTopic(t.Context(), "orders")
	if link["name"] != "b" || link["topic"] != "orders" || link["target_id"] != tt.ID {
		t.Fatalf("link = %v", link)
	}
	if _, owned := stub["owner"]; owned {
		t.Fatal("the stub has an owner")
	}

	want := p.produce(t, 150, 0)
	got := p.drainTarget(t, want, 30*time.Second)
	if len(got) != len(want) {
		t.Fatalf("the target holds %d distinct messages, want %d", len(got), len(want))
	}

	c := p.waitState(t, topic.RemoteStateRunning, 10*time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for c["lag_messages"] != float64(0) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		c = p.listing(t, "")
	}
	if c["lag_messages"] != float64(0) || c["lag_complete"] != true || c["target_verified_at"] == nil {
		t.Fatalf("listing after the drain = %v", c)
	}
	c = p.listing(t, "?partitions=true")
	if rows, _ := c["partitions"].([]any); len(rows) != 3 {
		t.Fatalf("partition rows = %v", c["partitions"])
	}
	// The target's listing of its own topic says nothing about ours,
	// and carries its ID for our checks.
	status, out := p.dst.admin(http.MethodGet, "/v1/topics/orders/children", nil)
	if status != 200 || !strings.Contains(string(out), `"parent_id":"`+tt.ID+`"`) {
		t.Fatalf("target listing: %d %s", status, out)
	}
}

// Every data-plane route refuses the stub (409), in every mode, and a
// PATCH refuses every field.
func TestRemoteChildStubRouteSweep(t *testing.T) {
	p := newRemotePair(t)
	p.attach(t, nil)
	handle := url.QueryEscape("0:0:1") // well formed: the stub guard answers, not the decoder
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/v1/topics/orders-to-b/produce?key=k", "x"},
		{http.MethodPost, "/v1/topics/orders-to-b/produce", `{"a":1}`},
		{http.MethodPost, "/v1/topics/orders-to-b/produce/batch", map[string]any{"messages": []map[string]any{{"payload": 1}}}},
		{http.MethodGet, "/v1/topics/orders-to-b/consume", nil},
		{http.MethodGet, "/v1/topics/orders-to-b/consume?wait=100ms", nil},
		{http.MethodGet, "/v1/topics/orders-to-b/consume?max=10", nil},
		{http.MethodGet, "/v1/topics/orders-to-b/consume?partition=0", nil},
		{http.MethodGet, "/v1/topics/orders-to-b/consume?partition=0&offset=0", nil},
		{http.MethodPost, "/v1/topics/orders-to-b/ack?receipt_handle=" + handle, nil},
		{http.MethodPost, "/v1/topics/orders-to-b/ack?extend=true&receipt_handle=" + handle, nil},
		{http.MethodPost, "/v1/topics/orders-to-b/ack?extend=0&receipt_handle=" + handle, nil},
		{http.MethodPatch, "/v1/topics/orders-to-b", map[string]any{"partitions": 3}},
		{http.MethodPatch, "/v1/topics/orders-to-b", map[string]any{"retention_ms": 7_200_000}},
		{http.MethodPatch, "/v1/topics/orders-to-b", map[string]any{"schema": map[string]any{"type": "object"}}},
	} {
		status, out := p.src.admin(tc.method, tc.path, tc.body)
		if status != http.StatusConflict {
			t.Fatalf("%s %s on a stub: %d %s, want 409", tc.method, tc.path, status, out)
		}
	}
	// Reads still work: describe and the parent's listing.
	if status, out := p.src.admin(http.MethodGet, "/v1/topics/orders-to-b", nil); status != 200 || !strings.Contains(string(out), `"remote"`) {
		t.Fatalf("describe the stub: %d %s", status, out)
	}
	status, out := p.src.admin(http.MethodPost, "/v1/topics/orders-to-b/produce", `{"a":1}`)
	if status != 409 || !strings.Contains(string(out), "lives on remote b; consume it there") {
		t.Fatalf("stub produce: %d %s", status, out)
	}
}

// Who may do what on remote children.
func TestRemoteChildAuthorizationMatrix(t *testing.T) {
	p := newRemotePair(t)
	// A parent owner who is not an admin cannot attach a remote child.
	status, out, _ := p.src.call(http.MethodPost, "/v1/topics/orders/children", "olivia", oliviaPass,
		map[string]any{"child": "orders-to-b", "remote": "b"})
	if status != http.StatusForbidden {
		t.Fatalf("owner remote attach: %d %s", status, out)
	}
	p.attach(t, nil)
	// A reader sees the link but not the admins' names.
	status, out, _ = p.src.call(http.MethodGet, "/v1/topics/orders/children", "rita", ritaPass, nil)
	if status != 200 || strings.Contains(string(out), "created_by") || !strings.Contains(string(out), `"remote":{`) {
		t.Fatalf("reader listing: %d %s", status, out)
	}
	status, out, _ = p.src.call(http.MethodGet, "/v1/topics/orders-to-b", "rita", ritaPass, nil)
	if status != 200 || strings.Contains(string(out), "created_by") {
		t.Fatalf("reader describe: %d %s", status, out)
	}
	if _, out := p.src.admin(http.MethodGet, "/v1/topics/orders-to-b", nil); !strings.Contains(string(out), `"created_by":"admin"`) {
		t.Fatalf("admin describe lacks created_by: %s", out)
	}
	// Pause is admin only; the owner may not.
	if status, _, _ := p.src.call(http.MethodPost, "/v1/topics/orders/children/orders-to-b/pause", "olivia", oliviaPass, map[string]any{}); status != 403 {
		t.Fatalf("owner pause: %d", status)
	}
	// The parent's owner may delete it once every cursor reports
	// (nothing is unshipped).
	p.waitComplete(t)
	status, out, h := p.src.call(http.MethodDelete, "/v1/topics/orders/children/orders-to-b", "olivia", oliviaPass, nil)
	if status != http.StatusNoContent {
		t.Fatalf("owner delete: %d %s", status, out)
	}
	if h.Get("Cache-Control") != "no-store" {
		t.Fatal("a remote child delete answer must not be cacheable")
	}
}

// A stalled link says why in the listing; a delete refuses while
// records are unshipped, and --force abandons them.
func TestRemoteChildStallsAndDelete(t *testing.T) {
	p := newRemotePair(t)
	p.attach(t, nil)
	first := p.produce(t, 20, 0)
	p.drainTarget(t, first, 20*time.Second)

	p.dst.down.Store(true)
	p.produce(t, 30, 100)
	c := p.waitState(t, topic.RemoteStateUnavailable, 20*time.Second)
	if c["lag_messages"].(float64) == 0 {
		t.Fatalf("lag 0 while the target is down: %v", c)
	}
	// A 202 means the record is in the source's ingress WAL; the lag
	// counts it once the dispatcher committed it to the parent. Until
	// then the delete's 409 reports it in dispatch_backlog instead.
	deadline := time.Now().Add(10 * time.Second)
	for c["lag_messages"].(float64) < 30 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		c = p.listing(t, "")
	}
	if c["lag_messages"].(float64) != 30 {
		t.Fatalf("lag never reached the 30 unshipped records: %v", c)
	}
	status, out := p.src.admin(http.MethodDelete, "/v1/topics/orders/children/orders-to-b", nil)
	if status != http.StatusConflict || !strings.Contains(string(out), "unshipped") || !strings.Contains(string(out), `"lag_messages":30`) {
		t.Fatalf("delete with unshipped records: %d %s", status, out)
	}
	status, out = p.src.admin(http.MethodDelete, "/v1/topics/orders", nil)
	if status != http.StatusConflict && status != http.StatusTooManyRequests {
		t.Fatalf("parent delete with unshipped records: %d %s", status, out)
	}
	status, out = p.src.admin(http.MethodDelete, "/v1/topics/orders/children/orders-to-b?force=true", nil)
	if status != http.StatusNoContent {
		t.Fatalf("forced delete: %d %s", status, out)
	}
	if status, _ := p.src.admin(http.MethodGet, "/v1/topics/orders-to-b", nil); status != http.StatusNotFound {
		t.Fatalf("the stub survived: %d", status)
	}
}

// A wrong password stalls the link in auth_failed; the listing says so,
// and the corrected credential drains it.
func TestRemoteChildAuthFailedInTheListing(t *testing.T) {
	p := newRemotePair(t)
	p.attach(t, nil)
	p.src.registerRemoteEntry("b", p.dst, remoteRandomString(18), 2)
	sent := p.produce(t, 10, 0)
	p.waitState(t, topic.RemoteStateAuthFailed, 20*time.Second)
	p.src.registerRemoteEntry("b", p.dst, remoteReplPass, 3)
	p.drainTarget(t, sent, 20*time.Second)
	p.waitState(t, topic.RemoteStateRunning, 10*time.Second)
}

func TestRemoteChildPauseResumeOverHTTP(t *testing.T) {
	p := newRemotePair(t)
	p.attach(t, nil)
	if status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children/orders-to-b/pause", map[string]any{"reason": "B maintenance"}); status != 200 {
		t.Fatalf("pause: %d %s", status, out)
	}
	c := p.waitState(t, topic.RemoteStatePaused, 10*time.Second)
	link := c["remote"].(map[string]any)
	if c["paused"] != true || link["pause_reason"] != "B maintenance" || link["paused_by"] != "admin" {
		t.Fatalf("paused listing = %v", c)
	}
	sent := p.produce(t, 10, 0)
	time.Sleep(700 * time.Millisecond)
	if status, _, _ := p.dst.call(http.MethodGet, "/v1/topics/orders/consume", "bob", bobPass, nil); status != http.StatusNoContent {
		t.Fatalf("the target has messages while the link is paused: %d", status)
	}
	if status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children/orders-to-b/resume", nil); status != 200 {
		t.Fatalf("resume: %d %s", status, out)
	}
	p.drainTarget(t, sent, 20*time.Second)
}

func TestRemoteChildDryRunAndRefusals(t *testing.T) {
	p := newRemotePair(t)
	status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children",
		map[string]any{"child": "orders-to-b", "remote": "b", "dry_run": true, "from": "unconsumed", "lanes": 2})
	if status != 200 || !strings.Contains(string(out), `"dry_run":true`) || !strings.Contains(string(out), `"attach_offsets":[0,0,0]`) {
		t.Fatalf("dry run: %d %s", status, out)
	}
	if status, _ := p.src.admin(http.MethodGet, "/v1/topics/orders-to-b", nil); status != http.StatusNotFound {
		t.Fatal("a dry run created the stub")
	}
	for _, tc := range []struct {
		body   map[string]any
		status int
	}{
		{map[string]any{"child": "x", "remote": "nope"}, 400},
		{map[string]any{"child": "x", "remote": "b", "remote_topic": "missing"}, 409},
		{map[string]any{"child": "x", "remote": "b", "remote_topic": "../users"}, 400},
	} {
		if status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children", tc.body); status != tc.status {
			t.Fatalf("%v: %d %s, want %d", tc.body, status, out, tc.status)
		}
	}
	p.attach(t, nil)
	// A second link to the same remote topic is refused.
	status, out = p.src.admin(http.MethodPost, "/v1/topics", map[string]any{"name": "orders2", "partitions": 3, "retention_ms": topic.MinRemoteSourceRetentionMs})
	if status != 201 {
		t.Fatalf("orders2: %d %s", status, out)
	}
	awaitAssigned(t, p.src.store, "orders2")
	status, out = p.src.admin(http.MethodPost, "/v1/topics/orders2/children", map[string]any{"child": "orders2-to-b", "remote": "b", "remote_topic": "orders"})
	if status != http.StatusConflict {
		t.Fatalf("second link to b/orders: %d %s", status, out)
	}
	// The parent's retention cannot shrink under the 24 h floor.
	status, out = p.src.admin(http.MethodPatch, "/v1/topics/orders", map[string]any{"retention_ms": 3_600_000})
	if status != http.StatusConflict {
		t.Fatalf("shrink under the floor: %d %s", status, out)
	}
	names := []string{}
	status, out = p.src.admin(http.MethodGet, "/v1/topics", nil)
	var page struct {
		Topics []topic.Topic `json:"topics"`
	}
	_ = json.Unmarshal(out, &page)
	for _, tp := range page.Topics {
		names = append(names, tp.Name)
	}
	if status != 200 || !slices.Contains(names, "orders-to-b") {
		t.Fatalf("topic list: %d %v", status, names)
	}
}

// The target topic is deleted and recreated while the link holds
// unshipped records: the target_missing retry re-runs the target check
// first, so the link stops in target_replaced and sends nothing to the
// new topic until an admin resumes with accept_target.
func TestRemoteChildRecreatedTargetNeedsAcceptTarget(t *testing.T) {
	p := newRemotePair(t)
	p.attach(t, nil)
	first := p.produce(t, 5, 0)
	p.drainTarget(t, first, 20*time.Second)

	if status, out := p.dst.admin(http.MethodDelete, "/v1/topics/orders", nil); status != http.StatusNoContent {
		t.Fatalf("delete target: %d %s", status, out)
	}
	held := p.produce(t, 5, 100)
	p.waitState(t, "target_missing", 20*time.Second)
	if status, out := p.dst.admin(http.MethodPost, "/v1/topics", map[string]any{"name": "orders", "partitions": 3}); status != http.StatusCreated {
		t.Fatalf("recreate target: %d %s", status, out)
	}
	awaitAssigned(t, p.dst.store, "orders")

	deadline := time.Now().Add(45 * time.Second)
	for {
		c := p.listing(t, "")
		if c["state"] == "target_replaced" {
			break
		}
		if c["state"] == "running" && c["lag_messages"].(float64) == 0 {
			t.Fatalf("the link shipped into the recreated target without accept_target: %v", c)
		}
		if time.Now().After(deadline) {
			t.Fatalf("never stopped in target_replaced: %v", c)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if status, out := p.src.admin(http.MethodPost, "/v1/topics/orders/children/orders-to-b/resume", map[string]any{"accept_target": true}); status != http.StatusOK {
		t.Fatalf("resume with accept_target: %d %s", status, out)
	}
	p.drainTarget(t, held, 45*time.Second)
}
