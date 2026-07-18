package main

// Tests for the cobra CLI surface: context store semantics, connection
// resolution precedence, and the modern commands end to end against a
// real in-process broker (the harness in cli_env_test.go).

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func withTempConfigDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("NARAD_CONFIG_DIR", dir)
	return dir
}

func clearConnEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NARAD_ADDR", "")
	t.Setenv("NARAD_USER", "")
	t.Setenv("NARAD_PASS", "")
	flagServer, flagUser, flagPassword = "", "", ""
}

func TestContextStoreRoundTripAndSelect(t *testing.T) {
	withTempConfigDir(t)
	clearConnEnv(t)

	if err := route([]string{"ctx", "add", "stage", "--server", "https://stage.example", "--user", "admin", "--password", "s3cret"}); err != nil {
		t.Fatalf("ctx add: %v", err)
	}
	if err := route([]string{"ctx", "add", "local", "--server", "http://127.0.0.1:7942"}); err != nil {
		t.Fatalf("ctx add local: %v", err)
	}
	// First added context became current; resolution must reflect it.
	got := resolveContext("", "", "")
	if got.Server != "https://stage.example" || got.User != "admin" || got.Password != "s3cret" {
		t.Fatalf("resolveContext = %+v, want stage context", got)
	}

	if err := route([]string{"ctx", "select", "local"}); err != nil {
		t.Fatalf("ctx select: %v", err)
	}
	if got := resolveContext("", "", ""); got.Server != "http://127.0.0.1:7942" || got.User != "" {
		t.Fatalf("after select, resolveContext = %+v, want local", got)
	}

	// The store file must be private: it can hold credentials.
	s, err := loadContextStore()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(s.Contexts) != 2 || s.Current != "local" {
		t.Fatalf("store = %+v, want 2 contexts, current local", s)
	}
	path, _ := contextFilePath()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat store: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("context file mode = %o, want 0600", fi.Mode().Perm())
	}

	if err := route([]string{"ctx", "rm", "stage"}); err != nil {
		t.Fatalf("ctx rm: %v", err)
	}
	if err := route([]string{"ctx", "select", "stage"}); err == nil {
		t.Fatal("selecting a removed context must fail")
	}
}

func TestResolveContextPrecedence(t *testing.T) {
	withTempConfigDir(t)
	clearConnEnv(t)
	if err := route([]string{"ctx", "add", "base", "--server", "http://ctx.example", "--user", "ctxuser"}); err != nil {
		t.Fatalf("ctx add: %v", err)
	}

	// Env beats context.
	t.Setenv("NARAD_ADDR", "http://env.example")
	if got := resolveContext("", "", ""); got.Server != "http://env.example" || got.User != "ctxuser" {
		t.Fatalf("env precedence: %+v", got)
	}
	// Flag beats env.
	if got := resolveContext("http://flag.example", "flaguser", ""); got.Server != "http://flag.example" || got.User != "flaguser" {
		t.Fatalf("flag precedence: %+v", got)
	}
}

func TestPubBodySources(t *testing.T) {
	if b, err := pubBody([]string{"t", "hello"}, ""); err != nil || string(b) != "hello" {
		t.Fatalf("arg body = %q, %v", b, err)
	}
	f := t.TempDir() + "/msg"
	if err := os.WriteFile(f, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if b, err := pubBody([]string{"t"}, f); err != nil || string(b) != "from-file" {
		t.Fatalf("file body = %q, %v", b, err)
	}
}

// The modern commands against a real broker: topic add (with human
// durations), ls, pub, and a peek that observes without consuming.
func TestCobraTopicPubPeekEndToEnd(t *testing.T) {
	env := newCLITestEnv(t)
	withTempConfigDir(t)
	clearConnEnv(t)
	t.Setenv("NARAD_ADDR", env.server.URL)

	if err := route([]string{"topic", "add", "orders", "--partitions", "3", "--retention", "2h"}); err != nil {
		t.Fatalf("topic add: %v", err)
	}
	if err := route([]string{"pub", "orders", `{"n":1}`, "--key", "k1"}); err != nil {
		t.Fatalf("pub: %v", err)
	}

	// Produce is WAL-first: wait for the record to become consumable.
	c := cliClient()
	deadline := time.Now().Add(5 * time.Second)
	var cursors map[int]int64
	for {
		var err error
		cursors, err = peekStartCursors(c, "orders", -1, -1)
		if err != nil {
			t.Fatalf("peekStartCursors: %v", err)
		}
		var total int64
		for _, v := range cursors {
			total += v
		}
		if total >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("produced record never became visible")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Peek from offset 0 on whichever partition holds the record; the
	// read must return the payload and must NOT reserve it.
	var part int
	found := false
	for p, hwm := range cursors {
		if hwm > 0 {
			part, found = p, true
		}
	}
	if !found {
		t.Fatal("no partition holds the record")
	}
	msg, ok, err := fetchOne(context.Background(), c, fmt.Sprintf("/v1/topics/orders/consume?partition=%d&offset=0", part))
	if err != nil || !ok {
		t.Fatalf("replay fetch = ok %v, err %v", ok, err)
	}
	if msg.ReceiptHandle != "" {
		t.Fatal("peek read returned a receipt handle — it reserved the message")
	}
	if string(msg.Payload) != `{"n":1}` || msg.Key != "k1" {
		t.Fatalf("peeked payload/key = %s / %s", msg.Payload, msg.Key)
	}

	// The message is still available to a real consumer afterwards.
	qmsg, ok, err := fetchOne(context.Background(), c, "/v1/topics/orders/consume?wait=2s")
	if err != nil || !ok {
		t.Fatalf("queue fetch after peek = ok %v, err %v", ok, err)
	}
	if qmsg.ReceiptHandle == "" {
		t.Fatal("queue fetch missing receipt handle")
	}
	ackWithRetry(context.Background(), c, "orders", qmsg.ReceiptHandle)
}

func TestParseGrants(t *testing.T) {
	g, err := parseGrants([]string{"produce:orders-*,invoices", "admin"})
	if err != nil {
		t.Fatalf("parseGrants: %v", err)
	}
	if len(g) != 2 || g[0]["action"] != "produce" || g[1]["action"] != "admin" {
		t.Fatalf("grants = %+v", g)
	}
	pats, _ := g[0]["patterns"].([]string)
	if len(pats) != 2 || pats[0] != "orders-*" || pats[1] != "invoices" {
		t.Fatalf("patterns = %+v", g[0]["patterns"])
	}
	if _, ok := g[1]["patterns"]; ok {
		t.Fatal("bare admin grant must have no patterns key")
	}
	if _, err := parseGrants([]string{"fly:me"}); err == nil {
		t.Fatal("unknown action must fail")
	}
}
