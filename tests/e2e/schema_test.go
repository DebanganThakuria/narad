package e2e

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

const (
	schemaIDOnly    = `{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`
	schemaIDAndName = `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}`
	schemaIDNameQty = `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"qty":{"type":"integer"}},"required":["id"]}`
)

func (e *env) topicDetails(name string) topic.Details {
	e.t.Helper()
	resp := e.get("/v1/topics/" + name)
	expectOK(e.t, resp)
	return readJSON[topic.Details](e.t, resp)
}

func (e *env) schemaHistory(name string) topic.SchemaHistory {
	e.t.Helper()
	resp := e.get("/v1/topics/" + name + "/schema")
	expectOK(e.t, resp)
	return readJSON[topic.SchemaHistory](e.t, resp)
}

func (e *env) setSchema(name, sch string, base int) *http.Response {
	e.t.Helper()
	body := map[string]any{"schema": json.RawMessage(sch)}
	if base > 0 {
		body["schema_base_version"] = base
	}
	return e.patch("/v1/topics/"+name, body)
}

func expectSchemaVersion(t *testing.T, e *env, name string, version int, sch string) {
	t.Helper()
	d := e.topicDetails(name)
	if d.SchemaVersion != version {
		t.Fatalf("%s: schema_version = %d, want %d", name, d.SchemaVersion, version)
	}
	if version == 0 {
		if d.Schema != nil {
			t.Fatalf("%s: schema = %s, want none", name, d.Schema)
		}
		return
	}
	if !schema.Equal(d.Schema, []byte(sch)) {
		t.Fatalf("%s: schema = %s, want %s", name, d.Schema, sch)
	}
	h := e.schemaHistory(name)
	if h.Version != version || len(h.Versions) != version || !schema.Equal(h.Versions[version-1].Schema, []byte(sch)) {
		t.Fatalf("%s: history = %+v, want %d versions ending in %s", name, h, version, sch)
	}
}

// TestSchemaLifecycle walks create, describe, evolve, precondition,
// idempotent re-registration, partition growth, and the produce and
// consume contract of a schema topic over HTTP.
func TestSchemaLifecycle(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)

	resp := e.post("/v1/topics", map[string]any{"name": "sl", "partitions": 3, "schema": json.RawMessage(schemaIDOnly)})
	expectStatus(t, resp, http.StatusCreated)
	if !e.awaitPartitionAssignments("sl", 3) {
		t.Fatal("assignments")
	}
	expectSchemaVersion(t, e, "sl", 1, schemaIDOnly)

	// A topic created without a schema reports none.
	e.createTopic("sl-plain", 3, 0)
	expectSchemaVersion(t, e, "sl-plain", 0, "")
	if h := e.schemaHistory("sl-plain"); h.Version != 0 || len(h.Versions) != 0 {
		t.Fatalf("schema-less history = %+v", h)
	}

	// Evolve, then re-register the same value (whitespace differs).
	expectOK(t, e.setSchema("sl", schemaIDAndName, 0))
	expectSchemaVersion(t, e, "sl", 2, schemaIDAndName)
	expectOK(t, e.setSchema("sl", strings.ReplaceAll(schemaIDAndName, ",", ", "), 0))
	expectSchemaVersion(t, e, "sl", 2, schemaIDAndName)

	// Precondition: stale base is a conflict, current base lands v3.
	expectConflict(t, e.setSchema("sl", schemaIDNameQty, 1))
	expectSchemaVersion(t, e, "sl", 2, schemaIDAndName)
	expectOK(t, e.setSchema("sl", schemaIDNameQty, 2))
	expectSchemaVersion(t, e, "sl", 3, schemaIDNameQty)

	// Incompatible is 400 and leaves the history alone.
	expectBadRequest(t, e.setSchema("sl", schemaIDOnly, 0))
	expectSchemaVersion(t, e, "sl", 3, schemaIDNameQty)
	// A schema cannot be removed.
	expectBadRequest(t, e.patch("/v1/topics/sl", map[string]any{"schema": nil}))
	expectBadRequest(t, e.setSchema("sl", `{}`, 0))
	expectSchemaVersion(t, e, "sl", 3, schemaIDNameQty)

	// Partition growth keeps the schema.
	expectOK(t, e.patch("/v1/topics/sl", map[string]any{"partitions": 5}))
	if !e.awaitPartitionAssignments("sl", 5) {
		t.Fatal("assignments after growth")
	}
	expectSchemaVersion(t, e, "sl", 3, schemaIDNameQty)

	// Produce: only JSON valid under v3 is accepted; every other body
	// kind is a 400, and the accepted JSON comes back verbatim.
	for _, bad := range []string{
		`{"name":"no id"}`, `{"id":"1"}`, `{"id":1,"qty":"3"}`,
		`plain text`, "\x00\x01\xff\xfe", `{"id":1} trailing`, `{"id":1,"name":"\xff"}`,
	} {
		resp := rawReq(t, http.MethodPost, e.url("/v1/topics/sl/produce"), []byte(bad))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("produce %q: status = %d, want 400", bad, resp.StatusCode)
		}
		if msg := readError(t, resp); !strings.Contains(msg, "schema") {
			t.Fatalf("produce %q: error %q does not mention the schema", bad, msg)
		}
	}
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/sl/produce"), nil)
	expectBadRequest(t, resp)

	payload := `{"id": 7, "name": "seven", "qty": 2, "extra": {"nested": [1, 2, {"deep": true}]}}`
	res := produceAndAwaitVisibility(t, e, "sl", "k", []byte(payload))
	msg, ok := mustConsume(t, e, "sl", consumeQuery{Partition: &res.Partition, Wait: "2s"})
	if !ok {
		t.Fatal("consume returned nothing")
	}
	if string(msg.Payload) != payload {
		t.Fatalf("consumed payload = %s, want it verbatim: %s", msg.Payload, payload)
	}
	// Replay reads the same bytes.
	replay := e.consume("/v1/topics/sl/consume?partition=" + strconv.Itoa(res.Partition) + "&offset=" + strconv.FormatInt(res.Offset, 10))
	if string(replay.Payload) != payload {
		t.Fatalf("replayed payload = %s", replay.Payload)
	}
}

func intString(i int) string { return strconv.Itoa(i) }

// Registration limits and their status codes.
func TestSchemaRegistrationLimits(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("lim", 3, 0)

	// Over the HTTP JSON body limit: 413, before any schema logic.
	huge := `{"description":"` + strings.Repeat("x", 1<<20) + `"}`
	expectStatus(t, e.setSchema("lim", huge, 0), http.StatusRequestEntityTooLarge)
	// Under the body limit but over the schema limit: 400.
	big := `{"description":"` + strings.Repeat("x", schema.MaxSchemaBytes) + `"}`
	resp := e.setSchema("lim", big, 0)
	expectBadRequest(t, resp)
	if msg := readError(t, resp); !strings.Contains(msg, "maximum") {
		t.Fatalf("oversized schema error = %q", msg)
	}
	// Too deep: 400 naming the limit.
	deep := strings.Repeat(`{"properties":{"a":`, 200) + `{}` + strings.Repeat(`}}`, 200)
	resp = e.setSchema("lim", deep, 0)
	expectBadRequest(t, resp)
	if msg := readError(t, resp); !strings.Contains(msg, "levels deep") {
		t.Fatalf("deep schema error = %q", msg)
	}
	// false, non-objects, external refs, lookahead patterns.
	for _, bad := range []string{`false`, `"x"`, `[]`, `1`, `{"$ref":"file:///etc/passwd"}`, `{"type":"string","pattern":"^(?=a)a$"}`, `{"type":123}`} {
		resp := e.setSchema("lim", bad, 0)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("schema %s: status = %d, want 400", bad, resp.StatusCode)
		}
		if msg := readError(t, resp); strings.Contains(msg, "/etc/passwd") || strings.Contains(msg, "file://") {
			t.Fatalf("schema %s: error echoes the reference: %q", bad, msg)
		}
		resp.Body.Close()
	}
	expectSchemaVersion(t, e, "lim", 0, "")

	// The same rules apply at create time.
	resp = e.post("/v1/topics", map[string]any{"name": "lim-create", "partitions": 3, "schema": json.RawMessage(deep)})
	expectBadRequest(t, resp)
	resp = e.get("/v1/topics/lim-create")
	expectNotFound(t, resp)

	// true is a valid schema meaning "any JSON".
	expectOK(t, e.setSchema("lim", `true`, 0))
	expectSchemaVersion(t, e, "lim", 1, `true`)
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/lim/produce"), []byte(`not json`))
	expectBadRequest(t, resp)
	produceAndAwaitVisibility(t, e, "lim", "", []byte(`[1,2,3]`))

	// A rejection body is bounded even for a schema with a huge enum.
	e.createTopic("lim-enum", 3, 0)
	var sb strings.Builder
	sb.WriteString(`{"enum":[`)
	for i := range 8000 {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"value-number-` + intString(i) + `"`)
	}
	sb.WriteString(`]}`)
	expectOK(t, e.setSchema("lim-enum", sb.String(), 0))
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/lim-enum/produce"), []byte(`"nope"`))
	expectBadRequest(t, resp)
	if body := readBody(resp); len(body) > 4096 {
		t.Fatalf("rejection body is %d bytes, want it bounded", len(body))
	}
}

// Delete drops the schema with the topic; a same-named recreation
// starts fresh, with or without a new schema.
func TestSchemaDeleteAndRecreate(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	create := func(sch string) {
		t.Helper()
		body := map[string]any{"name": "dr", "partitions": 3}
		if sch != "" {
			body["schema"] = json.RawMessage(sch)
		}
		expectStatus(t, e.post("/v1/topics", body), http.StatusCreated)
		if !e.awaitPartitionAssignments("dr", 3) {
			t.Fatal("assignments")
		}
	}
	create(schemaIDOnly)
	expectOK(t, e.setSchema("dr", schemaIDAndName, 0))
	expectSchemaVersion(t, e, "dr", 2, schemaIDAndName)
	resp := rawReq(t, http.MethodPost, e.url("/v1/topics/dr/produce"), []byte(`text`))
	expectBadRequest(t, resp)

	expectStatus(t, e.del("/v1/topics/dr"), http.StatusNoContent)
	create("")
	expectSchemaVersion(t, e, "dr", 0, "")
	produceAndAwaitVisibility(t, e, "dr", "", []byte(`text is fine now`))

	expectStatus(t, e.del("/v1/topics/dr"), http.StatusNoContent)
	create(`{"type":"array"}`)
	expectSchemaVersion(t, e, "dr", 1, `{"type":"array"}`)
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/dr/produce"), []byte(`{"id":1}`))
	expectBadRequest(t, resp)
	produceAndAwaitVisibility(t, e, "dr", "", []byte(`[]`))
}

// Fan-out: a child created under a schema'd parent adopts the whole
// history, is parent-managed while attached, keeps the history on
// detach, and can only re-attach where the histories match.
func TestSchemaFanoutChildLifecycle(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	resp := e.post("/v1/topics", map[string]any{"name": "fp", "partitions": 3, "schema": json.RawMessage(schemaIDOnly)})
	expectStatus(t, resp, http.StatusCreated)
	expectOK(t, e.setSchema("fp", schemaIDAndName, 0))
	if !e.awaitPartitionAssignments("fp", 3) {
		t.Fatal("assignments")
	}

	resp = e.post("/v1/topics", map[string]any{"name": "fc", "parent": "fp"})
	expectStatus(t, resp, http.StatusCreated)
	if !e.awaitPartitionAssignments("fc", 3) {
		t.Fatal("child assignments")
	}
	expectSchemaVersion(t, e, "fc", 2, schemaIDAndName)
	if h := e.schemaHistory("fc"); len(h.Versions) != 2 || !schema.Equal(h.Versions[0].Schema, []byte(schemaIDOnly)) {
		t.Fatalf("child history = %+v, want the parent's two versions", h)
	}

	// Parent-managed while attached, even with a matching base version.
	expectConflict(t, e.setSchema("fc", schemaIDNameQty, 2))
	// The parent's evolution propagates.
	expectOK(t, e.setSchema("fp", schemaIDNameQty, 2))
	expectSchemaVersion(t, e, "fc", 3, schemaIDNameQty)

	// Detach: the child keeps v1..v3 and manages them.
	expectStatus(t, e.del("/v1/topics/fp/children/fc"), http.StatusNoContent)
	expectSchemaVersion(t, e, "fc", 3, schemaIDNameQty)
	childV4 := `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"qty":{"type":"integer"},"note":{"type":"string"}},"required":["id"]}`
	expectOK(t, e.setSchema("fc", childV4, 3))
	expectSchemaVersion(t, e, "fc", 4, childV4)
	// Diverged histories cannot be re-linked.
	expectConflict(t, e.attachChild("fp", "fc"))
	// Catching the parent up to the same history makes the link legal.
	expectOK(t, e.setSchema("fp", childV4, 3))
	expectOK(t, e.attachChild("fp", "fc"))
	expectSchemaVersion(t, e, "fc", 4, childV4)

	// A schema'd child cannot go under a schema-less parent, and a
	// schema-less child adopts on plain attach too.
	e.createTopic("fp-plain", 3, 0)
	expectStatus(t, e.del("/v1/topics/fp/children/fc"), http.StatusNoContent)
	expectConflict(t, e.attachChild("fp-plain", "fc"))
	e.createTopic("fc-plain", 3, 0)
	expectOK(t, e.attachChild("fp", "fc-plain"))
	expectSchemaVersion(t, e, "fc-plain", 4, childV4)
}

// Authorization over real users: the owner and an admin may set the
// schema; a produce grant may read it but not change it; no grant
// means no read.
func TestSchemaSecurity(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t, withSecurity())
	au, ap := e.adminUser, e.adminPass
	for _, u := range []map[string]any{
		{"username": "alice", "password": "alicepw", "grants": []map[string]any{{"action": "create", "patterns": []string{"ss-*"}}}},
		{"username": "bob", "password": "bobpw", "grants": []map[string]any{{"action": "produce", "patterns": []string{"ss-*"}}}},
		{"username": "carol", "password": "carolpw"},
	} {
		resp := e.authReq(t, http.MethodPost, "/v1/users", u, au, ap)
		expectStatus(t, resp, http.StatusCreated)
		resp.Body.Close()
	}
	resp := e.authReq(t, http.MethodPost, "/v1/topics", map[string]any{"name": "ss-topic", "partitions": 3, "schema": json.RawMessage(schemaIDOnly)}, "alice", "alicepw")
	expectStatus(t, resp, http.StatusCreated)
	resp.Body.Close()

	patch := func(user, pass, sch string) int {
		resp := e.authReq(t, http.MethodPatch, "/v1/topics/ss-topic", map[string]any{"schema": json.RawMessage(sch)}, user, pass)
		resp.Body.Close()
		return resp.StatusCode
	}
	get := func(user, pass, path string) int {
		resp := e.authReq(t, http.MethodGet, path, nil, user, pass)
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := patch("bob", "bobpw", schemaIDAndName); got != http.StatusForbidden {
		t.Fatalf("produce grant PATCH schema: %d, want 403", got)
	}
	if got := patch("carol", "carolpw", schemaIDAndName); got != http.StatusForbidden {
		t.Fatalf("no grant PATCH schema: %d, want 403", got)
	}
	if got := patch("alice", "alicepw", schemaIDAndName); got != http.StatusOK {
		t.Fatalf("owner PATCH schema: %d, want 200", got)
	}
	if got := patch(au, ap, schemaIDNameQty); got != http.StatusOK {
		t.Fatalf("admin PATCH schema: %d, want 200", got)
	}
	for _, path := range []string{"/v1/topics/ss-topic", "/v1/topics/ss-topic/schema"} {
		if got := get("bob", "bobpw", path); got != http.StatusOK {
			t.Fatalf("produce grant GET %s: %d, want 200", path, got)
		}
		if got := get("alice", "alicepw", path); got != http.StatusOK {
			t.Fatalf("owner GET %s: %d, want 200", path, got)
		}
		if got := get("carol", "carolpw", path); got != http.StatusForbidden {
			t.Fatalf("no grant GET %s: %d, want 403", path, got)
		}
	}
	// The produce grant is still enforced by the schema.
	resp = rawAuthReq(t, e, "/v1/topics/ss-topic/produce", []byte(`{"id":"x"}`), "bob", "bobpw")
	expectBadRequest(t, resp)
	resp = rawAuthReq(t, e, "/v1/topics/ss-topic/produce", []byte(`{"id":1}`), "bob", "bobpw")
	expectStatus(t, resp, http.StatusAccepted)
}

func rawAuthReq(t *testing.T, e *env, path string, body []byte, user, pass string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.url(path), strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// The version cap is reachable and answers 409 without touching the
// history; re-registering the latest still succeeds.
func TestSchemaHistoryCapOverHTTP(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("cap", 3, 0)
	sch := func(v int) string {
		return `{"type":"object","title":"v` + intString(v) + `"}`
	}
	for v := 1; v <= metastore.MaxSchemaVersions; v++ {
		if resp := e.setSchema("cap", sch(v), 0); resp.StatusCode != http.StatusOK {
			t.Fatalf("v%d: status %d body %s", v, resp.StatusCode, readBody(resp))
		} else {
			resp.Body.Close()
		}
	}
	expectSchemaVersion(t, e, "cap", metastore.MaxSchemaVersions, sch(metastore.MaxSchemaVersions))
	expectConflict(t, e.setSchema("cap", sch(metastore.MaxSchemaVersions+1), 0))
	expectOK(t, e.setSchema("cap", sch(metastore.MaxSchemaVersions), 0))
	expectSchemaVersion(t, e, "cap", metastore.MaxSchemaVersions, sch(metastore.MaxSchemaVersions))
}
