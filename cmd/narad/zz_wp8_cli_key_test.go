package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// wp8CaptureStdout returns what fn wrote to os.Stdout.
func wp8CaptureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = stdout }()
	fn()
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// TestWP8SubDecodesBinaryKeys: the broker base64-wraps a key that is
// not valid UTF-8 and flags it with key_encoding, as it does a binary
// payload. `narad sub` must decode it rather than print the base64 as
// if it were the key.
func TestWP8SubDecodesBinaryKeys(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{key: "\xff\x00\x10", want: "key=ff0010 (binary)"},
		{key: "user-42", want: "key=user-42"},
	} {
		body := topic.Message{Topic: "t", Partition: 1, Offset: 7, Key: tc.key, Payload: json.RawMessage(`{"a":1}`)}.AppendJSON(nil)
		var msg consumedMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			t.Fatal(err)
		}
		out := wp8CaptureStdout(t, func() { printMessage(msg, false) })
		if !strings.Contains(out, tc.want) {
			t.Fatalf("sub output for key %q = %q, want it to contain %q", tc.key, out, tc.want)
		}
	}
}
