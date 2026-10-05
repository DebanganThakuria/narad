package topics

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// auditLines returns the component=audit lines a JSON logger wrote.
func auditLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal(raw, &line); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		if line["component"] == "audit" {
			lines = append(lines, line)
		}
	}
	return lines
}

// auditWant is the part of an audit line a case checks.
type auditWant struct {
	level, event, actor, target, outcome string
	status                               float64
	extra                                map[string]any
}

func checkAudit(t *testing.T, name string, got []map[string]any, want ...auditWant) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: audit lines = %v, want %d", name, got, len(want))
	}
	for i, w := range want {
		line := got[i]
		if line["level"] != w.level || line["event"] != w.event || line["actor"] != w.actor ||
			line["target"] != w.target || line["outcome"] != w.outcome || line["status"] != w.status {
			t.Fatalf("%s: audit line %d = %v, want level=%s event=%s actor=%s target=%s outcome=%s status=%v",
				name, i, line, w.level, w.event, w.actor, w.target, w.outcome, w.status)
		}
		for k, v := range w.extra {
			if line[k] != v {
				t.Fatalf("%s: audit line %d %s = %v, want %v (line %v)", name, i, k, line[k], v, line)
			}
		}
	}
}

// Every topic create, alter, schema change, delete, attach and detach
// writes one audit line on the node the client called, with the caller,
// the target, the outcome and the status, whether this node applied it
// or the leader did. Master logged no audit line for any of them.
func TestTopicMutationsAreAudited(t *testing.T) {
	alice := user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}}}
	bob := user.User{Username: "bob"}
	owned := func(_ context.Context, name string) (topic.Topic, error) {
		return topic.Topic{Name: name, ID: "00000000000000a1", Partitions: 3, Owner: "alice"}, nil
	}
	br := &fakeBroker{
		getTopicFn: owned,
		createTopicFn: func(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
			return topic.Topic{Name: opts.Name, Owner: opts.Owner}, nil
		},
		updateTopicRetentionFn: func(_ context.Context, name string, retention int64) (topic.Topic, error) {
			return topic.Topic{Name: name, RetentionMs: retention}, nil
		},
		updateTopicSchemaFn: func(_ context.Context, name string, _ []byte, _ int) (topic.Topic, error) {
			return topic.Topic{Name: name}, nil
		},
		deleteTopicFn: func(context.Context, string) error { return context.Canceled },
	}
	router := &fakeRouter{}
	var logs bytes.Buffer
	s := handlers.New(handlers.Deps{
		Broker:         br,
		Logger:         slog.New(slog.NewJSONHandler(&logs, nil)),
		MaxConsumeWait: time.Second,
		Router:         router,
	})
	serve := func(h http.HandlerFunc, method, path, body string, as user.User, pathValues ...string) int {
		logs.Reset()
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		for i := 0; i+1 < len(pathValues); i += 2 {
			req.SetPathValue(pathValues[i], pathValues[i+1])
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, withIdentity(req, as))
		return rec.Code
	}

	// Leader-direct create.
	if code := serve(Create(s), http.MethodPost, "/v1/topics", `{"name":"orders","partitions":3}`, alice); code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}
	checkAudit(t, "create", auditLines(t, &logs), auditWant{"INFO", "topic.create", "alice", "orders", "ok", 201, nil})

	// Alter with retention and a schema: one line per kind of change.
	if code := serve(Alter(s), http.MethodPatch, "/v1/topics/orders", `{"retention_ms":60000,"schema":{"type":"object"}}`, alice, "topic", "orders"); code != http.StatusOK {
		t.Fatalf("alter: %d", code)
	}
	checkAudit(t, "alter", auditLines(t, &logs),
		auditWant{"INFO", "topic.alter", "alice", "orders", "ok", 200, map[string]any{"fields": "retention_ms"}},
		auditWant{"INFO", "topic.schema", "alice", "orders", "ok", 200, nil})

	// A body the ingress refuses is not a mutation and is not audited.
	if code := serve(Alter(s), http.MethodPatch, "/v1/topics/orders", `{"partitions":-1}`, alice, "topic", "orders"); code != http.StatusBadRequest {
		t.Fatalf("invalid alter: %d", code)
	}
	checkAudit(t, "invalid alter", auditLines(t, &logs))

	// A denied delete is audited at warn.
	if code := serve(Delete(s), http.MethodDelete, "/v1/topics/orders", "", bob, "topic", "orders"); code != http.StatusForbidden {
		t.Fatalf("bob's delete: %d", code)
	}
	checkAudit(t, "denied delete", auditLines(t, &logs), auditWant{"WARN", "topic.delete", "bob", "orders", "denied", 403, nil})

	// A leader-direct delete whose client went away mid-commit: the
	// change may or may not have landed.
	if code := serve(Delete(s), http.MethodDelete, "/v1/topics/orders", "", alice, "topic", "orders"); code != handlers.StatusClientClosedRequest {
		t.Fatalf("cancelled delete: %d", code)
	}
	checkAudit(t, "cancelled delete", auditLines(t, &logs), auditWant{"INFO", "topic.delete", "alice", "orders", "unknown", 499, nil})

	// Forwarded: the leader's answer is the outcome.
	router.routeDeleteTopicFn = func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) bool {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if code := serve(Delete(s), http.MethodDelete, "/v1/topics/orders", "", alice, "topic", "orders"); code != http.StatusNoContent {
		t.Fatalf("forwarded delete: %d", code)
	}
	checkAudit(t, "forwarded delete", auditLines(t, &logs), auditWant{"INFO", "topic.delete", "alice", "orders", "ok", 204, nil})

	router.routeAlterTopicFn = func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ []byte) bool {
		http.Error(w, "metastore unavailable", http.StatusServiceUnavailable) // decided by the leader
		return true
	}
	if code := serve(Alter(s), http.MethodPatch, "/v1/topics/orders", `{"partitions":6}`, alice, "topic", "orders"); code != http.StatusServiceUnavailable {
		t.Fatalf("forwarded alter: %d", code)
	}
	checkAudit(t, "leader 503", auditLines(t, &logs), auditWant{"INFO", "topic.alter", "alice", "orders", "failed", 503, map[string]any{"fields": "partitions"}})

	router.routeAttachChildFn = func(_ context.Context, w http.ResponseWriter, _ *http.Request, _, _ string, _ int64) bool {
		w.WriteHeader(http.StatusOK)
		return true
	}
	if code := serve(AttachChild(s), http.MethodPost, "/v1/topics/orders/children", `{"child":"audit"}`, alice, "parent", "orders"); code != http.StatusOK {
		t.Fatalf("attach: %d", code)
	}
	checkAudit(t, "attach", auditLines(t, &logs), auditWant{"INFO", "topic.attach", "alice", "orders", "ok", 200, map[string]any{"child": "audit"}})

	// Leader-direct detach.
	if code := serve(DetachChild(s), http.MethodDelete, "/v1/topics/orders/children/audit", "", alice, "parent", "orders", "child", "audit"); code != http.StatusNoContent {
		t.Fatalf("detach: %d", code)
	}
	checkAudit(t, "detach", auditLines(t, &logs), auditWant{"INFO", "topic.detach", "alice", "orders", "ok", 204, map[string]any{"child": "audit"}})

	// A forward that ended without the leader's answer is unknown: the
	// leader may have applied it.
	router.routeDeleteTopicFn = func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) bool {
		if m, ok := w.(interface{ MarkUndecided() }); ok {
			m.MarkUndecided()
		}
		http.Error(w, "leader unreachable", http.StatusServiceUnavailable)
		return true
	}
	if code := serve(Delete(s), http.MethodDelete, "/v1/topics/orders", "", alice, "topic", "orders"); code != http.StatusServiceUnavailable {
		t.Fatalf("undecided delete: %d", code)
	}
	checkAudit(t, "undecided delete", auditLines(t, &logs), auditWant{"INFO", "topic.delete", "alice", "orders", "unknown", 503, nil})
}
