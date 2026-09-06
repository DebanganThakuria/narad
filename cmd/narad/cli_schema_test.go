package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestCobraTopicSchemaRoundTrip drives the schema surface of the CLI
// against a real broker: create with --schema, read it back through
// `topic info` and `topic schema`, evolve with --schema from a file
// and from stdin, use --schema-base-version as a precondition, and see
// an incompatible change refused.
func TestCobraTopicSchemaRoundTrip(t *testing.T) {
	env := newCLITestEnv(t)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", env.server.URL)

	v1 := `{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}`
	v2 := `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}`
	v3 := `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"qty":{"type":"integer"}},"required":["id"]}`

	run := func(stdin string, args ...string) (string, error) {
		t.Helper()
		out, _, err := captureCLIOutput(t, func() error { return route(args) }, stdin)
		return out, err
	}

	if _, err := run("", "topic", "add", "orders", "--partitions", "3", "--schema", v1); err != nil {
		t.Fatalf("topic add --schema: %v", err)
	}
	if err := waitForAssignments(env.store, "orders"); err != nil {
		t.Fatal(err)
	}

	// --schema must be JSON; the server decides whether it is a schema.
	if _, err := run("", "topic", "add", "bad", "--schema", `{not json`); err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Fatalf("topic add with malformed --schema error = %v", err)
	}

	out, err := run("", "topic", "info", "orders")
	if err != nil {
		t.Fatalf("topic info: %v", err)
	}
	details := decodeCLIJSON[topic.Details](t, out)
	if details.SchemaVersion != 1 || !jsonEqualStrings(t, string(details.Schema), v1) {
		t.Fatalf("topic info schema = v%d %s, want v1 %s", details.SchemaVersion, details.Schema, v1)
	}

	out, err = run("", "topic", "schema", "orders", "--current")
	if err != nil {
		t.Fatalf("topic schema --current: %v", err)
	}
	if !jsonEqualStrings(t, out, v1) {
		t.Fatalf("topic schema --current = %s, want %s", out, v1)
	}

	// Evolve from a file with a matching base version.
	schemaFile := filepath.Join(t.TempDir(), "v2.json")
	if err := os.WriteFile(schemaFile, []byte(v2), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run("", "topic", "edit", "orders", "--schema", "@"+schemaFile, "--schema-base-version", "1"); err != nil {
		t.Fatalf("topic edit --schema @file: %v", err)
	}
	// A stale base version is refused; the history is untouched.
	out, err = run("", "topic", "edit", "orders", "--schema", v3, "--schema-base-version", "1")
	if err == nil && !strings.Contains(out, "conflict") {
		t.Fatalf("stale --schema-base-version accepted: %s", out)
	}
	// From stdin, unconditional.
	if _, err := run(v3, "topic", "edit", "orders", "--schema", "-"); err != nil {
		t.Fatalf("topic edit --schema -: %v", err)
	}
	// Incompatible (drops a property) is refused.
	out, err = run("", "topic", "edit", "orders", "--schema", v1)
	if err == nil && !strings.Contains(out, "incompatible") {
		t.Fatalf("incompatible schema accepted: %s", out)
	}
	if _, err := run("", "topic", "edit", "orders", "--schema-base-version", "3"); err == nil {
		t.Fatal("--schema-base-version without --schema accepted")
	}

	out, err = run("", "topic", "schema", "orders")
	if err != nil {
		t.Fatalf("topic schema: %v", err)
	}
	history := decodeCLIJSON[topic.SchemaHistory](t, out)
	if history.Version != 3 || len(history.Versions) != 3 {
		t.Fatalf("history = %+v, want 3 versions", history)
	}
	for i, want := range []string{v1, v2, v3} {
		if history.Versions[i].Version != i+1 || !jsonEqualStrings(t, string(history.Versions[i].Schema), want) {
			t.Fatalf("history[%d] = v%d %s, want %s", i, history.Versions[i].Version, history.Versions[i].Schema, want)
		}
	}

	// A topic without a schema says so.
	if _, err := run("", "topic", "add", "plain", "--partitions", "3"); err != nil {
		t.Fatal(err)
	}
	out, err = run("", "topic", "schema", "plain", "--current")
	if err != nil || strings.TrimSpace(out) != "no schema" {
		t.Fatalf("topic schema --current on a schema-less topic = %q, %v", out, err)
	}

	// The registered schema is enforced on pub.
	if _, err := run("", "pub", "orders", `{"id":"not-an-integer"}`); err == nil {
		t.Fatal("pub of an invalid payload succeeded")
	}
	if _, err := run("", "pub", "orders", `{"id":1,"name":"ok","qty":2}`); err != nil {
		t.Fatalf("pub of a valid payload: %v", err)
	}
}

func jsonEqualStrings(t *testing.T, a, b string) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal([]byte(a), &va); err != nil {
		t.Fatalf("not JSON: %q", a)
	}
	if err := json.Unmarshal([]byte(b), &vb); err != nil {
		t.Fatalf("not JSON: %q", b)
	}
	ja, _ := json.Marshal(va)
	jb, _ := json.Marshal(vb)
	return string(ja) == string(jb)
}
