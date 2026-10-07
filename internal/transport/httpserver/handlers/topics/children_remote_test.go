package topics

// The ingress side of remote children: who may do what, what goes to
// the leader, and the listing's remote fields.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// recordingWriter stands in for the remote plane: it records every
// remote write and answers with res (or err).
type recordingWriter struct {
	mu   sync.Mutex
	reqs []nodewire.RemoteWriteRequest
	res  nodewire.Response
	err  error
}

func (w *recordingWriter) RemoteWrite(_ context.Context, req nodewire.RemoteWriteRequest) (nodewire.Response, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reqs = append(w.reqs, req)
	if w.err != nil {
		return w.res, w.err
	}
	return w.res, nil
}

func (w *recordingWriter) last(t *testing.T) (nodewire.RemoteWriteRequest, map[string]any) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.reqs) == 0 {
		t.Fatal("nothing was sent to the leader")
	}
	req := w.reqs[len(w.reqs)-1]
	var body map[string]any
	if err := json.Unmarshal(req.Body, &body); err != nil {
		t.Fatal(err)
	}
	return req, body
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.reqs)
}

var (
	adminUser = user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionAdmin}}}
	ownerUser = user.User{Username: "olivia", Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"orders*"}}}}
	readUser  = user.User{Username: "rita", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"orders*"}}}}
)

// remoteBroker is a broker with a parent "orders" (owned by olivia)
// and a remote child stub "orders-to-b".
func remoteBroker() *fakeBroker {
	stub := topic.Topic{
		Name: "orders-to-b", Role: topic.RoleChild, Parent: "orders", AttachOffsets: []int64{10, 0},
		Remote: &topic.RemoteLink{
			Name: "b", Topic: "orders", TargetID: "tid", From: "attach", Lanes: 1,
			Paused: true, PauseReason: "maint", PausedBy: "alice", CreatedBy: "alice",
		},
	}
	parent := topic.Topic{
		Name: "orders", ID: "pid-1", Partitions: 2, RetentionMs: 7_200_000, Owner: "olivia",
		Role: topic.RoleParent, Children: []string{"orders-to-b"},
	}
	return &fakeBroker{getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
		switch name {
		case "orders":
			return parent, nil
		case "orders-to-b":
			return stub, nil
		}
		return topic.Topic{}, errs.ErrTopicNotFound
	}}
}

func remoteSet(b *fakeBroker, w handlers.RemoteWriter) *handlers.Set {
	s := newTestSet(b)
	s.Deps.Remote.Writer = w
	return s
}

func post(t *testing.T, h http.HandlerFunc, path, body string, id *user.User, pathValues ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(pathValues); i += 2 {
		r.SetPathValue(pathValues[i], pathValues[i+1])
	}
	if id != nil {
		r = withIdentity(r, *id)
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

func del(t *testing.T, h http.HandlerFunc, path string, id *user.User, pathValues ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodDelete, path, nil)
	for i := 0; i+1 < len(pathValues); i += 2 {
		r.SetPathValue(pathValues[i], pathValues[i+1])
	}
	if id != nil {
		r = withIdentity(r, *id)
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

// A remote attach is admin only, with security on, checked before any
// lookup: the parent's owner is refused, and so is a node with no
// identity at all.
func TestRemoteAttachIsAdminOnly(t *testing.T) {
	b := remoteBroker()
	lookups := 0
	get := b.getTopicFn
	b.getTopicFn = func(ctx context.Context, name string) (topic.Topic, error) {
		lookups++
		return get(ctx, name)
	}
	w := &recordingWriter{res: nodewire.Response{Status: 201, Body: []byte(`{"name":"x"}`)}}
	h := AttachChild(remoteSet(b, w))
	body := `{"child":"orders-to-c","remote":"c","remote_topic":"orders"}`
	for _, id := range []*user.User{&ownerUser, nil} {
		lookups = 0
		res := post(t, h, "/v1/topics/orders/children", body, id, "parent", "orders")
		if res.Code != http.StatusForbidden || lookups != 0 {
			t.Fatalf("caller %v: status %d after %d lookups, want 403 before any lookup", id, res.Code, lookups)
		}
		if res.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("a refused remote attach must not be cacheable")
		}
	}
	if w.count() != 0 {
		t.Fatal("a refused attach reached the leader")
	}
	res := post(t, h, "/v1/topics/orders/children", body, &adminUser, "parent", "orders")
	if res.Code != http.StatusCreated {
		t.Fatalf("admin attach: %d %s", res.Code, res.Body)
	}
	req, sent := w.last(t)
	if req.SubOp != nodewire.RemoteSubAttach || req.Actor != "alice" || len(req.RequestID) != 16 {
		t.Fatalf("forwarded %+v", req)
	}
	if sent["parent"] != "orders" || sent["child"] != "orders-to-c" || sent["remote"] != "c" || sent["remote_topic"] != "orders" {
		t.Fatalf("forwarded body %v", sent)
	}
}

func TestRemoteAttachValidation(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: 201}}
	h := AttachChild(remoteSet(remoteBroker(), w))
	for _, tc := range []struct{ body, want string }{
		{`{"child":"x","remote":"c","remote_topic":"../users"}`, "remote_topic"},
		{`{"child":"x","remote":"c","remote_topic":"a/b"}`, "remote_topic"},
		{`{"child":"x","remote":"c","remote_topic":"%2e%2e"}`, "remote_topic"},
		{`{"child":"..","remote":"c"}`, "child"},
		{`{"child":"x","remote":"C!"}`, "remote"},
		{`{"child":"x","remote":"c","lanes":9}`, "lanes"},
		{`{"child":"x","remote":"c","from":"latest"}`, "from"},
		{`{"child":"x","remote":"c","delay_ms":-1}`, "delay_ms"},
		{`{"child":"x","remote":"c","url":"https://evil"}`, "unknown field"},
		{`{"child":"x","remote_topic":"t"}`, "remote child only"},
		{`{"child":"x","lanes":2}`, "remote child only"},
	} {
		res := post(t, h, "/v1/topics/orders/children", tc.body, &adminUser, "parent", "orders")
		if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), tc.want) {
			t.Fatalf("%s: %d %s, want 400 mentioning %q", tc.body, res.Code, res.Body, tc.want)
		}
	}
	if w.count() != 0 {
		t.Fatal("an invalid attach reached the leader")
	}
	// The remote topic defaults to the parent's name.
	if res := post(t, h, "/v1/topics/orders/children", `{"child":"x","remote":"c"}`, &adminUser, "parent", "orders"); res.Code != 201 {
		t.Fatalf("attach: %d", res.Code)
	}
	if _, sent := w.last(t); sent["remote_topic"] != "orders" {
		t.Fatalf("remote_topic = %v, want the parent's name", sent["remote_topic"])
	}
}

func TestRemoteWritesWithoutThePlaneAnswer501(t *testing.T) {
	s := remoteSet(remoteBroker(), nil)
	for _, h := range []http.HandlerFunc{PauseChild(s), ResumeChild(s), SkipChild(s)} {
		res := post(t, h, "/", `{"partition":0,"offset":1}`, &adminUser, "parent", "orders", "child", "orders-to-b")
		if res.Code != http.StatusNotImplemented {
			t.Fatalf("status %d, want 501", res.Code)
		}
	}
	res := post(t, AttachChild(s), "/", `{"child":"x","remote":"c"}`, &adminUser, "parent", "orders")
	if res.Code != http.StatusNotImplemented {
		t.Fatalf("attach without the plane: %d", res.Code)
	}
}

func TestPauseResumeSkipForwardAndAreAdminOnly(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: 200, Body: []byte(`{}`)}}
	s := remoteSet(remoteBroker(), w)
	pv := []string{"parent", "orders", "child", "orders-to-b"}
	if res := post(t, PauseChild(s), "/", `{"reason":"B maintenance"}`, &ownerUser, pv...); res.Code != http.StatusForbidden {
		t.Fatalf("owner pause: %d", res.Code)
	}
	if res := post(t, PauseChild(s), "/", `{"reason":"B maintenance"}`, &adminUser, pv...); res.Code != 200 {
		t.Fatalf("pause: %d %s", res.Code, res.Body)
	}
	if req, body := w.last(t); req.SubOp != nodewire.RemoteSubPause || body["reason"] != "B maintenance" || body["child"] != "orders-to-b" {
		t.Fatalf("pause forwarded %+v %v", req, body)
	}
	if res := post(t, PauseChild(s), "/", `{"reason":"line\nbreak"}`, &adminUser, pv...); res.Code != 400 {
		t.Fatalf("control character: %d", res.Code)
	}
	if res := post(t, ResumeChild(s), "/", `{"accept_target":true}`, &adminUser, pv...); res.Code != 200 {
		t.Fatalf("resume: %d", res.Code)
	}
	if req, body := w.last(t); req.SubOp != nodewire.RemoteSubResume || body["accept_target"] != true {
		t.Fatalf("resume forwarded %+v %v", req, body)
	}
	// An empty resume body is fine.
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.SetPathValue("parent", "orders")
	r.SetPathValue("child", "orders-to-b")
	r = withIdentity(r, adminUser)
	rec := httptest.NewRecorder()
	ResumeChild(s)(rec, r)
	if rec.Code != 200 {
		t.Fatalf("resume without a body: %d %s", rec.Code, rec.Body)
	}
	if res := post(t, SkipChild(s), "/", `{"partition":3,"offset":98331}`, &adminUser, pv...); res.Code != 200 {
		t.Fatalf("skip: %d", res.Code)
	}
	if req, body := w.last(t); req.SubOp != nodewire.RemoteSubSkip || body["partition"] != float64(3) || body["offset"] != float64(98331) {
		t.Fatalf("skip forwarded %+v %v", req, body)
	}
	if res := post(t, SkipChild(s), "/", `{"partition":3}`, &adminUser, pv...); res.Code != 400 {
		t.Fatalf("skip without an offset: %d", res.Code)
	}
}

func TestRemoteWriteErrorsMapToStatuses(t *testing.T) {
	pv := []string{"parent", "orders", "child", "orders-to-b"}
	tooOld := &recordingWriter{err: errors.New("cluster: the leader does not serve remote writes")}
	if res := post(t, PauseChild(remoteSet(remoteBroker(), tooOld)), "/", `{}`, &adminUser, pv...); res.Code != http.StatusPreconditionFailed {
		t.Fatalf("an older leader: %d, want 412", res.Code)
	}
	down := &recordingWriter{err: errors.New("dial: connection refused")}
	if res := post(t, PauseChild(remoteSet(remoteBroker(), down)), "/", `{}`, &adminUser, pv...); res.Code != http.StatusServiceUnavailable {
		t.Fatalf("an unreachable leader: %d, want 503", res.Code)
	}
	throttled := &recordingWriter{res: nodewire.Response{Status: 429, ContentType: nodewire.ContentTypeJSON, Body: []byte(`{"error":"slow","retry_after_seconds":7}`)}}
	res := del(t, DetachChild(remoteSet(remoteBroker(), throttled)), "/", &adminUser, pv...)
	if res.Code != http.StatusTooManyRequests || res.Header().Get("Retry-After") != "7" {
		t.Fatalf("a throttled delete: %d Retry-After %q", res.Code, res.Header().Get("Retry-After"))
	}
}

// Deleting a stub: an admin or the parent's owner, security on; the
// stub never grants anything, and an admin demoted since keeps nothing.
func TestStubDeleteAuthorization(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: http.StatusNoContent}}
	s := remoteSet(remoteBroker(), w)
	pv := []string{"parent", "orders", "child", "orders-to-b"}
	demoted := user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"*"}}}}
	for _, tc := range []struct {
		name string
		id   *user.User
		want int
	}{
		{"no identity", nil, http.StatusForbidden},
		{"a reader", &readUser, http.StatusForbidden},
		{"the demoted admin who created it", &demoted, http.StatusForbidden},
		{"the parent's owner", &ownerUser, http.StatusNoContent},
		{"an admin", &adminUser, http.StatusNoContent},
	} {
		res := del(t, DetachChild(s), "/?force=true", tc.id, pv...)
		if res.Code != tc.want {
			t.Fatalf("%s: %d %s, want %d", tc.name, res.Code, res.Body, tc.want)
		}
	}
	req, body := w.last(t)
	if req.SubOp != nodewire.RemoteSubDetach || body["force"] != true || body["expect_remote"] != true {
		t.Fatalf("forwarded %+v %v", req, body)
	}
	if res := del(t, DetachChild(s), "/?force=maybe", &adminUser, pv...); res.Code != 400 {
		t.Fatalf("a bad force value: %d", res.Code)
	}
	// The topic route: the stub's delete follows the same rule.
	res := del(t, Delete(s), "/", &ownerUser, "topic", "orders-to-b")
	if res.Code != http.StatusNoContent {
		t.Fatalf("stub topic delete by the parent owner: %d %s", res.Code, res.Body)
	}
	req, body = w.last(t)
	if req.SubOp != nodewire.RemoteSubTopicDelete || body["topic"] != "orders-to-b" || body["expect_remote"] != true || body["force"] != false {
		t.Fatalf("forwarded %+v %v", req, body)
	}
	if res := del(t, Delete(s), "/", &readUser, "topic", "orders-to-b"); res.Code != http.StatusForbidden {
		t.Fatalf("stub topic delete by a reader: %d", res.Code)
	}
	// A parent with remote children says so to the leader.
	if res := del(t, Delete(s), "/?force=true", &ownerUser, "topic", "orders"); res.Code != http.StatusNoContent {
		t.Fatalf("parent delete: %d", res.Code)
	}
	if _, body := w.last(t); body["expect_remote"] != true || body["force"] != true {
		t.Fatalf("parent delete forwarded %v", body)
	}
}

// Deleting a parent with remote children needs security on, as deleting
// its stubs does: a node with no identity refuses it and forwards
// nothing, force or not.
func TestParentWithRemoteChildrenDeleteNeedsSecurity(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: http.StatusNoContent}}
	s := remoteSet(remoteBroker(), w)
	for _, path := range []string{"/", "/?force=true"} {
		if res := del(t, Delete(s), path, nil, "topic", "orders"); res.Code != http.StatusForbidden || !strings.Contains(res.Body.String(), "security") {
			t.Fatalf("delete %s with no identity: %d %s, want 403", path, res.Code, res.Body)
		}
	}
	if n := w.count(); n != 0 {
		t.Fatalf("%d remote writes forwarded for a caller with no identity", n)
	}
}

// A plain topic delete and a local detach take the topic path, as they
// do without remote children: no remote write. A missing topic is 404.
// A stub's delete goes to the leader as a remote write, and a leader
// that serves no remote children answers it 501.
func TestPlainDeletesTakeTheTopicPath(t *testing.T) {
	notBuilt := nodewire.Response{Status: http.StatusNotImplemented, Body: []byte(`{"error":"remote children are not available on this node"}`)}
	var deleted, detached string
	b := &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			if name == "missing" {
				return topic.Topic{}, errs.ErrTopicNotFound
			}
			return topic.Topic{Name: name}, nil
		},
		deleteTopicFn: func(_ context.Context, name string) error {
			if name == "missing" {
				return errs.ErrTopicNotFound
			}
			deleted = name
			return nil
		},
		detachChildFn: func(_ context.Context, parent, child string) error { detached = parent + "/" + child; return nil },
	}
	w := &recordingWriter{res: notBuilt}
	if res := del(t, Delete(remoteSet(b, w)), "/", nil, "topic", "plain"); res.Code != http.StatusNoContent || deleted != "plain" {
		t.Fatalf("plain delete: %d %s deleted=%q", res.Code, res.Body, deleted)
	}
	if res := del(t, Delete(remoteSet(b, w)), "/", nil, "topic", "missing"); res.Code != http.StatusNotFound {
		t.Fatalf("delete of a missing topic: %d %s, want 404", res.Code, res.Body)
	}
	if res := del(t, DetachChild(remoteSet(b, w)), "/", nil, "parent", "p1", "child", "c1"); res.Code != http.StatusNoContent || detached != "p1/c1" {
		t.Fatalf("local detach: %d %s detached=%q", res.Code, res.Body, detached)
	}
	if w.count() != 0 {
		t.Fatalf("%d remote writes for plain deletes, want none", w.count())
	}
	sb := remoteBroker()
	if res := del(t, DetachChild(remoteSet(sb, w)), "/", &adminUser, "parent", "orders", "child", "orders-to-b"); res.Code != http.StatusNotImplemented {
		t.Fatalf("stub detach against a leader without remote children: %d, want its 501", res.Code)
	}
	if w.count() != 1 {
		t.Fatalf("%d remote writes for the stub's detach, want 1", w.count())
	}
}

func TestRemoteChildListingFields(t *testing.T) {
	b := remoteBroker()
	frontier := int64(12)
	now := time.Now()
	b.fanoutCursorStatsFn = func(context.Context, string) ([]topic.FanoutCursorStat, error) {
		return []topic.FanoutCursorStat{
			{
				Child: "orders-to-b", Partition: 0, NextOffset: 10, HighWatermark: 30, Node: "n0", State: topic.RemoteStateRejectedRecord,
				BlockedAt:  &topic.RemoteBlock{Partition: 0, Offset: 10, State: topic.RemoteStateRejectedRecord},
				LagSeconds: 42, AckFrontier: &frontier, TargetVerifiedAtMs: now.Add(-time.Minute).UnixMilli(), LastSuccessMs: now.UnixMilli(),
			},
			{
				Child: "orders-to-b", Partition: 1, NextOffset: 5, HighWatermark: 5, Node: "n0", State: topic.RemoteStateRunning,
				AckFrontier: &frontier, TargetVerifiedAtMs: now.UnixMilli(),
			},
		}, nil
	}
	s := newTestSet(b)
	get := func(id *user.User, query string) map[string]any {
		r := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/children"+query, nil)
		r.SetPathValue("parent", "orders")
		if id != nil {
			r = withIdentity(r, *id)
		}
		rec := httptest.NewRecorder()
		ListChildren(s)(rec, r)
		if rec.Code != 200 {
			t.Fatalf("listing: %d %s", rec.Code, rec.Body)
		}
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	out := get(&adminUser, "?partitions=true")
	if out["parent_id"] != "pid-1" {
		t.Fatalf("parent_id = %v", out["parent_id"])
	}
	child := out["children"].([]any)[0].(map[string]any)
	link := child["remote"].(map[string]any)
	if child["state"] != topic.RemoteStateRejectedRecord || child["lag_seconds"] != float64(42) || child["paused"] != true ||
		link["name"] != "b" || link["created_by"] != "alice" || link["paused_by"] != "alice" {
		t.Fatalf("remote child = %v", child)
	}
	if blocked := child["blocked_at"].(map[string]any); blocked["offset"] != float64(10) {
		t.Fatalf("blocked_at = %v", child["blocked_at"])
	}
	if child["source_drained"] != true {
		t.Fatalf("source_drained = %v; the frontier (12) passed both starts (10 and 0)", child["source_drained"])
	}
	if h := child["retention_headroom_seconds"].(float64); h != 7200-42 {
		t.Fatalf("headroom = %v, want retention minus lag", h)
	}
	if rows := child["partitions"].([]any); len(rows) != 2 || rows[0].(map[string]any)["start_offset"] != float64(10) {
		t.Fatalf("partition rows = %v", child["partitions"])
	}
	if strings.Contains(string(mustJSON(t, out)), "https://") {
		t.Fatal("the listing named a URL")
	}
	// A reader sees the link but not the admins' names.
	out = get(&readUser, "")
	link = out["children"].([]any)[0].(map[string]any)["remote"].(map[string]any)
	if _, ok := link["created_by"]; ok {
		t.Fatalf("a reader saw created_by: %v", link)
	}
	if _, ok := link["paused_by"]; ok {
		t.Fatalf("a reader saw paused_by: %v", link)
	}
	if _, ok := out["children"].([]any)[0].(map[string]any)["partitions"]; ok {
		t.Fatal("partition rows without ?partitions=true")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRemoteStatusFolding(t *testing.T) {
	parent := topic.Topic{Name: "orders", Partitions: 2, RetentionMs: 0}
	stub := topic.Topic{Name: "s", Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}, AttachOffsets: []int64{5, 5}}
	f := func(p int, state string, frontier int64) topic.FanoutCursorStat {
		return topic.FanoutCursorStat{Child: "s", Partition: p, State: state, AckFrontier: &frontier}
	}
	now := time.Now()
	st := remoteStatus(parent, stub, []topic.FanoutCursorStat{f(0, topic.RemoteStateRunning, 5), f(1, topic.RemoteStateAuthFailed, 9)}, true, true, false, now)
	if st.State != topic.RemoteStateAuthFailed || !st.SourceDrained || st.RetentionHeadroomSeconds != nil {
		t.Fatalf("status = %+v, want the worst state, drained, no headroom for keep-forever", st)
	}
	st = remoteStatus(parent, stub, []topic.FanoutCursorStat{f(0, topic.RemoteStateRunning, 4)}, true, true, false, now)
	if st.State != topic.RemoteStateUnknown || st.SourceDrained {
		t.Fatalf("a partition without a cursor: %+v, want unknown and not drained", st)
	}
	st = remoteStatus(parent, stub, []topic.FanoutCursorStat{f(0, topic.RemoteStateRunning, 5), f(1, topic.RemoteStateRunning, 5)}, false, true, false, now)
	if st.State != topic.RemoteStateUnknown {
		t.Fatalf("an unreachable owner: %+v, want unknown", st)
	}
	st = remoteStatus(parent, stub, []topic.FanoutCursorStat{f(0, topic.RemoteStateRunning, 5), f(1, topic.RemoteStateRunning, 5)}, true, true, false, now)
	if st.State != topic.RemoteStateRunning || !st.Unverified {
		t.Fatalf("running with no target check: %+v, want unverified", st)
	}
}

// Topic describe and list strip the admin names for a non-admin.
func TestStubDescribeRedactsAdminNames(t *testing.T) {
	b := remoteBroker()
	b.getTopicDetailsFn = func(ctx context.Context, name string) (topic.Details, error) {
		t, err := b.getTopicFn(ctx, name)
		return topic.Details{Topic: t}, err
	}
	b.listTopicsFn = func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
		stub, _ := b.getTopicFn(context.Background(), "orders-to-b")
		return []topic.Topic{stub}, "", nil
	}
	s := newTestSet(b)
	for _, h := range []struct {
		name string
		fn   http.HandlerFunc
		path string
	}{{"get", Get(s), "/v1/topics/orders-to-b"}, {"list", List(s), "/v1/topics"}} {
		for _, id := range []user.User{readUser, adminUser} {
			r := httptest.NewRequest(http.MethodGet, h.path, nil)
			r.SetPathValue("topic", "orders-to-b")
			r = withIdentity(r, id)
			rec := httptest.NewRecorder()
			h.fn(rec, r)
			body, _ := io.ReadAll(rec.Body)
			hasNames := bytes.Contains(body, []byte(`"created_by"`)) || bytes.Contains(body, []byte(`"paused_by"`))
			if hasNames != id.IsAdmin() {
				t.Fatalf("%s as %s: admin names shown = %v (%s)", h.name, id.Username, hasNames, body)
			}
		}
	}
}

func TestRemoteChildWritesAreRateLimitedPerNode(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: 200, Body: []byte(`{}`)}}
	s := remoteSet(remoteBroker(), w)
	pv := []string{"parent", "orders", "child", "orders-to-b"}
	for i := range remoteChildWritesPerMinute {
		if res := post(t, PauseChild(s), "/", `{}`, &adminUser, pv...); res.Code != 200 {
			t.Fatalf("write %d: %d", i, res.Code)
		}
	}
	res := post(t, PauseChild(s), "/", `{}`, &adminUser, pv...)
	if res.Code != http.StatusTooManyRequests || res.Header().Get("Retry-After") == "" {
		t.Fatalf("write past the budget: %d Retry-After %q", res.Code, res.Header().Get("Retry-After"))
	}
	// Another node (another handler set) has its own budget.
	if res := post(t, PauseChild(remoteSet(remoteBroker(), w)), "/", `{}`, &adminUser, pv...); res.Code != 200 {
		t.Fatalf("another node's first write: %d", res.Code)
	}
}

// The node the client called and the leader each audit a remote child's
// detach or delete; the caller's line carries the request_id it sent to
// the leader, so the two lines can be joined (an abandonment with
// --force included).
func TestRemoteDetachAndDeleteAuditLinesCarryTheRequestID(t *testing.T) {
	w := &recordingWriter{res: nodewire.Response{Status: http.StatusNoContent}}
	s := remoteSet(remoteBroker(), w)
	var logs bytes.Buffer
	s.Deps.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	s = handlers.New(s.Deps)
	s.Deps.Remote.Writer = w

	del(t, DetachChild(s), "/?force=true", &adminUser, "parent", "orders", "child", "orders-to-b")
	req, _ := w.last(t)
	lines := auditLines(t, &logs)
	if len(lines) != 1 || lines[0]["event"] != "remote_child.delete" || lines[0]["request_id"] != req.RequestID || req.RequestID == "" || lines[0]["force"] != true {
		t.Fatalf("detach audit = %v, want remote_child.delete with request_id %q and force", lines, req.RequestID)
	}

	logs.Reset()
	del(t, Delete(s), "/", &adminUser, "topic", "orders-to-b")
	req, _ = w.last(t)
	lines = auditLines(t, &logs)
	if len(lines) != 1 || lines[0]["event"] != "topic.delete" || lines[0]["request_id"] != req.RequestID || req.RequestID == "" {
		t.Fatalf("delete audit = %v, want topic.delete with request_id %q", lines, req.RequestID)
	}
}

// A partition whose owner has never had a successful target check is
// the stalest of all: once its owner has checked for longer than the
// window, the link is unverified and has no target_verified_at, however
// fresh the other owners' checks are.
func TestRemoteStatusFlagsAPartitionNeverVerified(t *testing.T) {
	parent := topic.Topic{Name: "orders", Partitions: 2}
	stub := topic.Topic{Name: "s", Remote: &topic.RemoteLink{Name: "b", Topic: "orders"}, AttachOffsets: []int64{0, 0}}
	now := time.Now()
	fresh := now.Add(-time.Minute).UnixMilli()
	stat := func(p int, verified, since int64) topic.FanoutCursorStat {
		return topic.FanoutCursorStat{Child: "s", Partition: p, State: topic.RemoteStateRunning, TargetVerifiedAtMs: verified, TargetCheckSinceMs: since}
	}
	st := remoteStatus(parent, stub, []topic.FanoutCursorStat{stat(0, fresh, fresh), stat(1, 0, now.Add(-11*time.Minute).UnixMilli())}, true, true, false, now)
	if !st.Unverified || st.TargetVerifiedAt != nil {
		t.Fatalf("one partition never verified in 11 minutes: unverified %v, target_verified_at %v; want unverified and none", st.Unverified, st.TargetVerifiedAt)
	}
	// Its first check is still young: not yet flagged.
	st = remoteStatus(parent, stub, []topic.FanoutCursorStat{stat(0, fresh, fresh), stat(1, 0, now.Add(-time.Minute).UnixMilli())}, true, true, false, now)
	if st.Unverified || st.TargetVerifiedAt != nil {
		t.Fatalf("one partition checking for a minute: unverified %v, target_verified_at %v; want neither", st.Unverified, st.TargetVerifiedAt)
	}
	st = remoteStatus(parent, stub, []topic.FanoutCursorStat{stat(0, fresh, fresh), stat(1, fresh, fresh)}, true, true, false, now)
	if st.Unverified || st.TargetVerifiedAt == nil {
		t.Fatalf("every partition verified: unverified %v, target_verified_at %v", st.Unverified, st.TargetVerifiedAt)
	}
}
