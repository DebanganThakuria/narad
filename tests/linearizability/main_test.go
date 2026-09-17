package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// historyFile writes a small clean history: one message produced,
// delivered and acked. That is enough for a verdict of OK, which matters
// here because run() calls os.Exit(1) on a failed verdict and a test
// process cannot survive that.
func historyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "h.jsonl")
	content := `{"op":"produce","msg":"m1","path":"orders","call":1,"ret":2,"status":202,"outcome":"ok"}
{"op":"deliver","msg":"m1","path":"orders","call":3,"ret":4,"status":200,"outcome":"ok"}
{"op":"ack","msg":"m1","path":"orders","call":5,"ret":6,"status":204,"outcome":"ok"}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// Both cases here return their error before run() ever builds a result,
// which is what keeps them away from the os.Exit(1) a failed verdict
// would trigger.
func TestRunFlagValidationErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing -history", []string{}, "-history is required"},
		{"negative -samples", []string{"-history", historyFile(t), "-samples", "-1"}, "-samples must be >= 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := run(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) = %v, want an error containing %q", tc.args, err, tc.want)
			}
		})
	}
}

// The end-to-end path over a clean history: both output files get
// written, and a -faults path that does not exist is a legitimate run
// with no faults injected rather than an I/O error.
func TestRunEndToEndWritesJSONAndMarkdown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "verdict.json")
	mdPath := filepath.Join(dir, "verdict.md")

	err := run([]string{
		"-history", historyFile(t),
		"-faults", filepath.Join(dir, "no-such-faults.jsonl"),
		"-json", jsonPath,
		"-markdown", mdPath,
	})
	if err != nil {
		t.Fatalf("run() = %v, want nil for a clean history", err)
	}

	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read %s: %v", jsonPath, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json output did not parse: %v", err)
	}
	if decoded["verdict"] != "OK" {
		t.Errorf("json verdict = %v, want OK", decoded["verdict"])
	}

	md, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read %s: %v", mdPath, err)
	}
	if !strings.Contains(string(md), "OK") {
		t.Errorf("markdown summary should carry the verdict:\n%s", md)
	}
}
