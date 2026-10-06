package cluster

// The leader side of remote children: create, pause, resume, skip and
// the remote-aware deletes with their unshipped check, driven through
// the remote plane as the HTTP handlers drive it.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines(substr string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, l := range strings.Split(b.buf.String(), "\n") {
		if strings.Contains(l, substr) {
			out = append(out, l)
		}
	}
	return out
}

func (s *rigSource) auditLog() *syncBuffer {
	buf := &syncBuffer{}
	s.links.d.Log = slog.New(slog.NewJSONHandler(buf, nil))
	return buf
}

func (s *rigSource) write(t *testing.T, subOp string, body map[string]any) nodewire.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.plane.RemoteWrite(context.Background(), nodewire.RemoteWriteRequest{
		SubOp: subOp, Actor: "alice", RequestID: "req-" + subOp, Body: raw,
	})
	if err != nil {
		t.Fatalf("RemoteWrite(%s): %v", subOp, err)
	}
	return res
}

func bodyOf(t *testing.T, res nodewire.Response) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(res.Body, &out); err != nil {
		t.Fatalf("answer %d %q: %v", res.Status, res.Body, err)
	}
	return out
}

// linksRig is a source with remote "b" registered, no target needed:
// the checks are faked.
func linksRig(t *testing.T) *rigSource {
	t.Helper()
	s := newRigSource(t, rigSourceOpts{partitions: 2})
	registerRemote(t, s.store, domremote.Record{Name: "b", ID: "rid-b", URL: "https://b.example", Username: "repl"}, rigRandomString(18))
	s.checks.targetID = "tid-1"
	return s
}

func TestRemoteLinksAttachCreatesTheStub(t *testing.T) {
	s := linksRig(t)
	audit := s.auditLog()
	s.produce(t, 0, 7, 2, 0)
	s.produce(t, 1, 4, 2, 100)
	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"})
	if res.Status != http.StatusCreated {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	stub, err := s.store.GetTopic(context.Background(), "orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	r := stub.Remote
	if r.Name != "b" || r.Topic != "orders" || r.TargetID != "tid-1" || r.From != topic.RemoteFromAttach || r.Lanes != 1 || r.CreatedBy != "alice" {
		t.Fatalf("link = %+v", r)
	}
	if !slices.Equal(stub.AttachOffsets, []int64{7, 4}) {
		t.Fatalf("attach offsets = %v, want the high watermarks [7 4]", stub.AttachOffsets)
	}
	if lines := audit.lines(`"event":"remote_child.create"`); len(lines) != 1 || !strings.Contains(lines[0], `"outcome":"committed"`) ||
		!strings.Contains(lines[0], `"actor":"alice"`) || !strings.Contains(lines[0], `"request_id":"req-child.attach"`) {
		t.Fatalf("leader audit lines = %v, want exactly one committed line with actor and request ID", lines)
	}
	var answer map[string]any
	_ = json.Unmarshal(res.Body, &answer)
	if answer["name"] != "orders-to-b" || answer["partitions"] != float64(0) {
		t.Fatalf("answer = %v, want the stub record", answer)
	}
}

func TestRemoteLinksAttachStartModes(t *testing.T) {
	s := linksRig(t)
	s.produce(t, 0, 10, 2, 0)
	ctx := context.Background()
	earliest, err := s.runner.AttachOffsetsMode(ctx, "orders", topic.RemoteFromEarliest)
	if err != nil || !slices.Equal(earliest, []int64{0, 0}) {
		t.Fatalf("earliest = %v, %v", earliest, err)
	}
	// Nothing consumed yet: everything retained is unconsumed.
	unconsumed, err := s.runner.AttachOffsetsMode(ctx, "orders", topic.RemoteFromUnconsumed)
	if err != nil || !slices.Equal(unconsumed, []int64{0, 0}) {
		t.Fatalf("unconsumed before any ack = %v, %v", unconsumed, err)
	}
	// Consume and ack three records of partition 0: the frontier moves.
	for range 3 {
		p := 0
		msg, found, err := s.broker.Consume(ctx, "orders", brokerConsumeOpts(&p))
		if err != nil || !found {
			t.Fatalf("consume: %v %v", found, err)
		}
		if err := s.broker.Ack(ctx, "orders", handleOf(t, msg.ReceiptHandle)); err != nil {
			t.Fatal(err)
		}
	}
	unconsumed, err = s.runner.AttachOffsetsMode(ctx, "orders", topic.RemoteFromUnconsumed)
	if err != nil || !slices.Equal(unconsumed, []int64{3, 0}) {
		t.Fatalf("unconsumed after 3 acks = %v, %v, want [3 0]", unconsumed, err)
	}
	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b", "from": "unconsumed", "lanes": 3})
	if res.Status != http.StatusCreated {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	stub, _ := s.store.GetTopic(ctx, "orders-to-b")
	if !slices.Equal(stub.AttachOffsets, []int64{3, 0}) || stub.Remote.From != "unconsumed" || stub.Remote.Lanes != 3 {
		t.Fatalf("stub = %+v %+v", stub.AttachOffsets, stub.Remote)
	}
}

func TestRemoteLinksDryRunWritesNothingAndWarns(t *testing.T) {
	s := linksRig(t)
	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b", "dry_run": true})
	if res.Status != http.StatusOK {
		t.Fatalf("dry run: %d %s", res.Status, res.Body)
	}
	out := bodyOf(t, res)
	if out["dry_run"] != true || out["attach_offsets"] == nil || out["checks"] == nil {
		t.Fatalf("dry run answer = %v", out)
	}
	warnings, _ := out["warnings"].([]any)
	if len(warnings) == 0 || !strings.Contains(warnings[0].(string), "below 72h") {
		t.Fatalf("warnings = %v, want the 72h retention warning (retention is 24h)", warnings)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err == nil {
		t.Fatal("a dry run created the stub")
	}
}

func TestRemoteLinksAttachRefusals(t *testing.T) {
	s := linksRig(t)
	cases := []struct {
		name   string
		setup  func()
		body   map[string]any
		status int
		want   string
	}{
		{"unknown remote", nil, map[string]any{"parent": "orders", "child": "x", "remote": "nope"}, 400, "unknown remote"},
		{"bad remote name", nil, map[string]any{"parent": "orders", "child": "x", "remote": "B!"}, 400, "remote"},
		{"bad remote topic", nil, map[string]any{"parent": "orders", "child": "x", "remote": "b", "remote_topic": "../users"}, 400, "remote_topic"},
		{"lanes", nil, map[string]any{"parent": "orders", "child": "x", "remote": "b", "lanes": 9}, 400, "lanes"},
		{"from", nil, map[string]any{"parent": "orders", "child": "x", "remote": "b", "from": "latest"}, 400, "from"},
		{"unknown field", nil, map[string]any{"parent": "orders", "child": "x", "remote": "b", "url": "https://evil"}, 400, "invalid"},
		{"parent missing", nil, map[string]any{"parent": "nope", "child": "x", "remote": "b"}, 404, "parent"},
		{"posture", func() { s.checks.posture = errs.ErrRemotePosture }, map[string]any{"parent": "orders", "child": "x", "remote": "b"}, 412, "posture"},
		{"check fails", func() { s.checks.posture, s.checks.fail = nil, topic.RemoteStateAuthFailed }, map[string]any{"parent": "orders", "child": "x", "remote": "b"}, 409, "auth_failed"},
		{"old member", func() { s.checks.fail = "old_release" }, map[string]any{"parent": "orders", "child": "x", "remote": "b"}, 412, "old_release"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup()
			}
			res := s.write(t, nodewire.RemoteSubAttach, tc.body)
			if res.Status != tc.status || !strings.Contains(string(res.Body), tc.want) {
				t.Fatalf("status %d body %s, want %d mentioning %q", res.Status, res.Body, tc.status, tc.want)
			}
		})
	}
	if _, err := s.store.GetTopic(context.Background(), "x"); err == nil {
		t.Fatal("a refused attach created its stub")
	}
}

// A second remote that reaches the same scheme, host and port cannot
// open a second link to the same remote topic.
func TestRemoteLinksRefuseASecondLinkToTheSameEndpoint(t *testing.T) {
	s := linksRig(t)
	registerRemote(t, s.store, domremote.Record{Name: "b2", ID: "rid-b2", URL: "https://B.example:443/", Username: "repl"}, rigRandomString(18))
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != 201 {
		t.Fatalf("first attach: %d %s", res.Status, res.Body)
	}
	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b2", "remote": "b2", "remote_topic": "orders"})
	if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "orders/orders-to-b") {
		t.Fatalf("second link: %d %s, want 409 naming the first", res.Status, res.Body)
	}
	// The same remote and topic from another parent: the FSM refuses.
	if _, err := s.broker.CreateTopic(context.Background(), brokerCreateOpts("other", 3)); err != nil {
		t.Fatal(err)
	}
	res = s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "other", "child": "other-to-b", "remote": "b", "remote_topic": "orders"})
	if res.Status != http.StatusConflict {
		t.Fatalf("same remote topic from another parent: %d %s", res.Status, res.Body)
	}
}

func TestRemoteLinksPauseResumeSkip(t *testing.T) {
	s := linksRig(t)
	audit := s.auditLog()
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != 201 {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b", "reason": "B maintenance"})
	if res.Status != 200 {
		t.Fatalf("pause: %d %s", res.Status, res.Body)
	}
	stub, _ := s.store.GetTopic(context.Background(), "orders-to-b")
	if !stub.Remote.Paused || stub.Remote.PauseReason != "B maintenance" || stub.Remote.PausedBy != "alice" || stub.Remote.PausedAtMs == 0 {
		t.Fatalf("after pause: %+v", stub.Remote)
	}
	if res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b", "reason": "a\x07bell"}); res.Status != 400 {
		t.Fatalf("control character in reason: %d", res.Status)
	}
	if res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b", "reason": strings.Repeat("r", 257)}); res.Status != 400 {
		t.Fatalf("257-byte reason: %d", res.Status)
	}

	// The target was recreated: a plain resume refuses, accept_target
	// records the new ID.
	s.checks.targetID = "tid-2"
	if res := s.write(t, nodewire.RemoteSubResume, map[string]any{"parent": "orders", "child": "orders-to-b"}); res.Status != http.StatusConflict {
		t.Fatalf("resume onto a replaced target: %d %s", res.Status, res.Body)
	}
	if res := s.write(t, nodewire.RemoteSubResume, map[string]any{"parent": "orders", "child": "orders-to-b", "accept_target": true}); res.Status != 200 {
		t.Fatalf("resume with accept_target: %d %s", res.Status, res.Body)
	}
	stub, _ = s.store.GetTopic(context.Background(), "orders-to-b")
	if stub.Remote.Paused || stub.Remote.TargetID != "tid-2" {
		t.Fatalf("after resume: %+v", stub.Remote)
	}

	if res := s.write(t, nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": 1, "offset": 42}); res.Status != 200 {
		t.Fatalf("skip: %d %s", res.Status, res.Body)
	}
	if res := s.write(t, nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": 2, "offset": 42}); res.Status != 400 {
		t.Fatalf("skip on a partition the parent lacks: %d", res.Status)
	}
	stub, _ = s.store.GetTopic(context.Background(), "orders-to-b")
	if !stub.Remote.Skipped(1, 42) {
		t.Fatalf("skip = %v", stub.Remote.Skip)
	}
	for _, ev := range []string{"remote_child.pause", "remote_child.accept_target", "remote_child.skip"} {
		if lines := audit.lines(`"event":"` + ev + `"`); len(lines) == 0 {
			t.Fatalf("no leader audit line for %s", ev)
		}
	}
	if lines := audit.lines(`"reason":"B maintenance"`); len(lines) != 1 {
		t.Fatalf("pause audit lines with the reason = %v", lines)
	}
	if res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "nope"}); res.Status != 404 {
		t.Fatalf("pause of an unknown child: %d", res.Status)
	}
}

// A member on an older release applies the state op as an unknown op
// and stores nothing, so pause and skip refuse (412) while one answers
// as an older release, and propose nothing.
func TestRemoteLinksPauseAndSkipRefuseAnOlderMember(t *testing.T) {
	s := linksRig(t)
	audit := s.auditLog()
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != 201 {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	s.checks.mu.Lock()
	s.checks.releases = &PostureError{Err: errs.ErrRemoteFeatureGate, Members: []string{"node-old"}}
	s.checks.mu.Unlock()
	version := s.store.TopicVersion("orders-to-b")
	for _, c := range []struct {
		subOp string
		body  map[string]any
	}{
		{nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b", "reason": "B maintenance"}},
		{nodewire.RemoteSubSkip, map[string]any{"parent": "orders", "child": "orders-to-b", "partition": 1, "offset": 42}},
	} {
		res := s.write(t, c.subOp, c.body)
		if res.Status != http.StatusPreconditionFailed || !strings.Contains(string(res.Body), "node-old") {
			t.Fatalf("%s with an older member: %d %s, want 412 naming it", c.subOp, res.Status, res.Body)
		}
	}
	if v := s.store.TopicVersion("orders-to-b"); v != version {
		t.Fatalf("stub version moved %d -> %d: a refused pause or skip proposed a state op", version, v)
	}
	stub, _ := s.store.GetTopic(context.Background(), "orders-to-b")
	if stub.Remote.Paused || len(stub.Remote.Skip) != 0 {
		t.Fatalf("after refused pause and skip: %+v", stub.Remote)
	}
	for _, ev := range []string{"remote_child.pause", "remote_child.skip"} {
		if lines := audit.lines(`"event":"` + ev + `"`); len(lines) != 1 || !strings.Contains(lines[0], `"outcome":"refused"`) {
			t.Fatalf("%s audit lines = %v, want one refusal", ev, lines)
		}
	}
}

// deleteRig is a source whose remote child points at a target that is
// down, so its lag stays above zero until the test lets it ship.
func deleteRig(t *testing.T) *remoteRig {
	t.Helper()
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 1}})
	rg.src.links.checkEvery = 0
	return rg
}

func TestRemoteLinksDeleteRefusedWhileUnshippedUnlessForced(t *testing.T) {
	rg := deleteRig(t)
	s := rg.src
	audit := s.auditLog()
	rg.target.faults.set("down")
	s.start()
	defer s.stop()
	s.produce(t, 0, 25, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)

	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true})
	if res.Status != http.StatusConflict {
		t.Fatalf("delete with lag: %d %s", res.Status, res.Body)
	}
	out := bodyOf(t, res)
	if out["lag_messages"] != float64(25) || out["lag_complete"] != true || !strings.Contains(out["error"].(string), "unshipped") {
		t.Fatalf("409 body = %v", out)
	}
	if lines := audit.lines(`"event":"remote_child.delete"`); len(lines) != 1 || !strings.Contains(lines[0], `"outcome":"refused"`) {
		t.Fatalf("audit = %v", lines)
	}
	// An ingress stale view (it authorized as a local child) is refused.
	if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b"}); res.Status != http.StatusConflict ||
		!strings.Contains(string(res.Body), "retry") {
		t.Fatalf("delete authorized as a local child: %d %s", res.Status, res.Body)
	}

	res = s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true})
	if res.Status != http.StatusNoContent {
		t.Fatalf("forced delete: %d %s", res.Status, res.Body)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err == nil {
		t.Fatal("the stub survived a forced delete")
	}
	lines := audit.lines(`"outcome":"committed"`)
	if len(lines) != 1 || !strings.Contains(lines[0], `"forced":true`) || !strings.Contains(lines[0], `"abandoned_lag_messages":25`) {
		t.Fatalf("forced delete audit = %v, want the abandoned counts", lines)
	}
}

// A record accepted into the ingress WAL but not yet committed is
// invisible to cursor lag; the delete refuses on the dispatch backlog.
func TestRemoteLinksDeleteRefusedOnTheDispatchBacklog(t *testing.T) {
	rg := deleteRig(t)
	s := rg.src
	s.start()
	defer s.stop()
	shipped := s.produce(t, 0, 5, 2, 0)
	rg.waitDelivered(t, shipped, 15*time.Second)
	rigWait(t, "lag 0", 10*time.Second, func() bool { return s.cursorOffset(t, 0) == 5 })
	// Accepted, answered 202, and stuck: no dispatcher runs on the rig.
	if _, err := s.broker.AcceptProduce(context.Background(), "orders", "k", []byte(`{"late":1}`)); err != nil {
		t.Fatal(err)
	}
	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true})
	if res.Status != http.StatusConflict {
		t.Fatalf("delete with a backlog: %d %s", res.Status, res.Body)
	}
	out := bodyOf(t, res)
	backlog, _ := out["dispatch_backlog"].(map[string]any)
	if out["lag_messages"] != float64(0) || backlog["node-self"] != float64(1) {
		t.Fatalf("409 body = %v, want lag 0 and a backlog of 1 on node-self", out)
	}
	// The parent delete refuses the same way.
	res = s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "orders", "expect_remote": true})
	if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "remote children with unshipped records") {
		t.Fatalf("parent delete with a backlog: %d %s", res.Status, res.Body)
	}
	// Once the backlog is committed and shipped, the delete goes ahead.
	disp := NewProduceDispatcher(s.ingress, s.store, "node-self", s.broker, nil, rigLogger(), ProduceDispatcherConfig{PollInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go disp.Run(ctx)
	rigWait(t, "backlog drained and shipped", 20*time.Second, func() bool {
		n, _, _ := s.ingress.PendingForTopic("src-orders-id", "orders", 100)
		return n == 0 && s.cursorOffset(t, 0) == 6
	})
	res = s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "orders", "expect_remote": true})
	if res.Status != http.StatusNoContent {
		t.Fatalf("parent delete after the drain: %d %s", res.Status, res.Body)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err == nil {
		t.Fatal("the stub survived its parent")
	}
}

// Legacy records (no TopicID) and ID-less parents match by name.
func TestPendingForTopicMatchesByIDThenName(t *testing.T) {
	rg := deleteRig(t)
	ing := rg.src.ingress
	ctx := context.Background()
	if _, err := ing.AcceptProduceWithTopicID(ctx, "orders", "src-orders-id", "", 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := ing.AcceptProduceWithTopicID(ctx, "orders", "", "", 0, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if _, err := ing.AcceptProduceWithTopicID(ctx, "orders", "an-older-incarnation", "", 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := ing.AcceptProduceWithTopicID(ctx, "other", "x", "", 0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if n, complete, err := ing.PendingForTopic("src-orders-id", "orders", 100); err != nil || !complete || n != 2 {
		t.Fatalf("by ID: %d %v %v, want the matching ID and the legacy record", n, complete, err)
	}
	if n, _, _ := ing.PendingForTopic("", "orders", 100); n != 3 {
		t.Fatalf("ID-less parent: %d, want every record named orders", n)
	}
	if n, complete, _ := ing.PendingForTopic("", "orders", 2); n != 2 || complete {
		t.Fatalf("at the scan limit: %d complete=%v, want an incomplete scan", n, complete)
	}
}

// Two concurrent deletes each get a check that started after they
// arrived (one shares the other's, or waits and runs its own, exempt
// from the throttle); a third inside the window is 429 with a retry
// hint; a member scans afresh for every
// query, so no answer outlives the check it was made for.
func TestRemoteLinksUnshippedCheckIsBounded(t *testing.T) {
	rg := deleteRig(t)
	s := rg.src
	s.links.checkEvery = time.Hour
	rg.target.faults.set("down")
	s.start()
	defer s.stop()
	s.produce(t, 0, 5, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	for i := range 2 {
		wg.Go(func() {
			statuses[i] = s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true}).Status
		})
	}
	wg.Wait()
	if statuses[0] != http.StatusConflict || statuses[1] != http.StatusConflict {
		// One of them may have started after the first finished; then it
		// is throttled.
		if !(slices.Contains(statuses, http.StatusConflict) && slices.Contains(statuses, http.StatusTooManyRequests)) {
			t.Fatalf("concurrent deletes: %v", statuses)
		}
	}
	res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true})
	if res.Status != http.StatusTooManyRequests || bodyOf(t, res)["retry_after_seconds"] == nil {
		t.Fatalf("a third delete inside the window: %d %s", res.Status, res.Body)
	}
	// Forced, the delete skips the paced check.
	if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "orders-to-b", "expect_remote": true, "force": true}); res.Status != http.StatusNoContent {
		t.Fatalf("forced delete: %d %s", res.Status, res.Body)
	}

	// No member cache: a record accepted between two queries is in the
	// second answer.
	q, _ := json.Marshal(map[string]string{"topic": "orders", "topic_id": "src-orders-id"})
	count := func() float64 {
		res := s.links.ServeUnshipped(context.Background(), nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckUnshipped, Body: q})
		n, _ := bodyOf(t, res)["count"].(float64)
		return n
	}
	first := count()
	if _, err := s.broker.AcceptProduce(context.Background(), "orders", "k", []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if second := count(); second != first+1 {
		t.Fatalf("backlog %v then %v: a query answered from an earlier scan", first, second)
	}
}

func TestRemoteLinksPlainDeletesAreUnchanged(t *testing.T) {
	s := linksRig(t)
	ctx := context.Background()
	if _, err := s.broker.CreateTopic(ctx, brokerCreateOpts("local", 3)); err != nil {
		t.Fatal(err)
	}
	if err := s.broker.AttachChild(ctx, "orders", "local", 0); err != nil {
		t.Fatal(err)
	}
	if res := s.write(t, nodewire.RemoteSubDetach, map[string]any{"parent": "orders", "child": "local"}); res.Status != http.StatusNoContent {
		t.Fatalf("local detach through child.delete: %d %s", res.Status, res.Body)
	}
	if lc, err := s.store.GetTopic(ctx, "local"); err != nil || lc.IsChild() {
		t.Fatalf("local child after detach: %+v %v, want standalone", lc, err)
	}
	if res := s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "local"}); res.Status != http.StatusNoContent {
		t.Fatalf("plain topic delete: %d %s", res.Status, res.Body)
	}
	if res := s.write(t, nodewire.RemoteSubTopicDelete, map[string]any{"topic": "local"}); res.Status != http.StatusNotFound {
		t.Fatalf("delete of a missing topic: %d", res.Status)
	}
	if res := s.write(t, "child.nope", map[string]any{}); res.Status != http.StatusBadRequest {
		t.Fatalf("unknown sub-op: %d", res.Status)
	}
}

// No forwarding path skips the check: a leader answers a raw
// OpDeleteTopic or OpDetachChild for a stub, or a parent with remote
// children, with 409.
func TestRawDeletesOfRemoteLinkedTopicsAreRefused(t *testing.T) {
	s := linksRig(t)
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != 201 {
		t.Fatalf("attach: %d", res.Status)
	}
	srv := NewRPCServer(s.broker, s.store, rigLogger())
	for _, name := range []string{"orders", "orders-to-b"} {
		payload, _ := nodewire.EncodeTopicNameRequest(nodewire.OpDeleteTopic, nodewire.TopicNameRequest{Topic: name})
		if res := srv.handleDeleteTopic(payload); res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "remote-aware delete") {
			t.Fatalf("raw delete of %s: %d %s", name, res.Status, res.Body)
		}
	}
	payload, _ := nodewire.EncodeChildLinkRequest(nodewire.OpDetachChild, nodewire.ChildLinkRequest{Parent: "orders", Child: "orders-to-b"})
	if res := srv.handleDetachChild(payload); res.Status != http.StatusConflict {
		t.Fatalf("raw detach of the stub: %d %s", res.Status, res.Body)
	}
	if _, err := s.store.GetTopic(context.Background(), "orders-to-b"); err != nil {
		t.Fatal("a refused raw delete removed the stub")
	}
}
