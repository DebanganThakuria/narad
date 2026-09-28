package topic

// A produce key is whatever the caller URL-encoded, stored unvalidated,
// so the consume encoder must turn ANY key into valid JSON that decodes
// back to the produced bytes. A key the response cannot carry leaves the
// consumer unable to parse the receipt handle: the record can never be
// acked and, once max_acked_ahead later offsets are acked, it is the
// only record its partition hands out.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"unicode/utf8"
)

// wp1bDecodedMessage is the consumer's view of a response: the key and
// payload with their encoding markers.
type wp1bDecodedMessage struct {
	Topic           string          `json:"topic"`
	Key             *string         `json:"key"`
	KeyEncoding     string          `json:"key_encoding"`
	Payload         json.RawMessage `json:"payload"`
	PayloadEncoding string          `json:"payload_encoding"`
	ReceiptHandle   string          `json:"receipt_handle"`
}

// wp1bCheckEncoding asserts m encodes to valid JSON through both
// AppendJSON and json.Marshal, and that the key and payload decode back
// to exactly what was produced. It returns the decoded response.
func wp1bCheckEncoding(t *testing.T, m Message) wp1bDecodedMessage {
	t.Helper()
	got := m.AppendJSON(nil)
	if !json.Valid(got) {
		t.Fatalf("AppendJSON produced invalid JSON for key %q: %s", m.Key, got)
	}
	// json.Marshal delegates to AppendJSON, then compacts the result and
	// HTML-escapes it, as it does for every Marshaler.
	viaMarshal, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal(key %q): %v", m.Key, err)
	}
	var compact, escaped bytes.Buffer
	if err := json.Compact(&compact, got); err != nil {
		t.Fatalf("compact: %v", err)
	}
	json.HTMLEscape(&escaped, compact.Bytes())
	if !bytes.Equal(escaped.Bytes(), viaMarshal) {
		t.Fatalf("AppendJSON and json.Marshal disagree:\n  append:  %s\n  marshal: %s", got, viaMarshal)
	}

	var d wp1bDecodedMessage
	if err := json.Unmarshal(got, &d); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if d.ReceiptHandle != m.ReceiptHandle {
		t.Fatalf("receipt_handle = %q, want %q", d.ReceiptHandle, m.ReceiptHandle)
	}

	// Key: absent when empty, else recoverable byte for byte.
	switch {
	case m.Key == "" && d.Key != nil:
		t.Fatalf("empty key encoded as %q", *d.Key)
	case m.Key != "" && d.Key == nil:
		t.Fatalf("key %q missing from response %s", m.Key, got)
	case m.Key != "":
		gotKey := []byte(*d.Key)
		if d.KeyEncoding == "base64" {
			if gotKey, err = base64.StdEncoding.DecodeString(*d.Key); err != nil {
				t.Fatalf("base64 key: %v", err)
			}
		} else if d.KeyEncoding != "" {
			t.Fatalf("unknown key_encoding %q", d.KeyEncoding)
		}
		if !bytes.Equal(gotKey, []byte(m.Key)) {
			t.Fatalf("key round trip = %q, want %q (response %s)", gotKey, m.Key, got)
		}
	}
	if d.KeyEncoding != "" && m.Key == "" {
		t.Fatalf("key_encoding %q without a key", d.KeyEncoding)
	}

	// Payload: verbatim JSON, a JSON string, or base64.
	switch {
	case len(m.Payload) == 0:
		if string(d.Payload) != "null" {
			t.Fatalf("empty payload encoded as %s", d.Payload)
		}
	case d.PayloadEncoding == "base64":
		var s string
		if err := json.Unmarshal(d.Payload, &s); err != nil {
			t.Fatalf("base64 payload is not a string: %v", err)
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil || !bytes.Equal(raw, m.Payload) {
			t.Fatalf("base64 payload round trip = %q (%v), want %q", raw, err, m.Payload)
		}
	case json.Valid(m.Payload):
		if !bytes.Equal(d.Payload, bytes.Trim(m.Payload, " \t\r\n")) {
			t.Fatalf("JSON payload = %s, want %s", d.Payload, m.Payload)
		}
	default:
		var s string
		if err := json.Unmarshal(d.Payload, &s); err != nil || s != string(m.Payload) {
			t.Fatalf("text payload round trip = %q (%v), want %q", s, err, m.Payload)
		}
	}
	return d
}

func TestWP1BMessageKeyEncoding(t *testing.T) {
	digest := sha256.Sum256([]byte("customer-42"))
	tests := []struct {
		name         string
		key          string
		wantEncoding string
	}{
		{"plain ascii", "customer-42", ""},
		{"quote and backslash", `a"b\c`, ""},
		{"tab and newline", "tab\there\nnl\r", ""},
		{"control byte", "\x01", ""},
		{"nul inside", "a\x00b", ""},
		{"delete", "\x7f", ""},
		{"vertical tab and bell", "a\vb\a", ""},
		{"latin", "clé", ""},
		{"emoji", "\U0001F600", ""},
		{"non-printable astral tag rune", "\U000E0067", ""},
		{"line and paragraph separators", "a\u2028b\u2029c", ""},
		{"soft hyphen", "a\u00adb", ""},
		{"html characters", "<a&b>", ""},
		{"invalid utf8 byte", "\xff", "base64"},
		{"truncated multibyte", "k\xc3", "base64"},
		{"binary digest", string(digest[:]), "base64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Message{
				Topic: "orders", Partition: 1, Offset: 7, Key: tt.key,
				Payload: json.RawMessage(`{"a":1}`), Timestamp: 42, ReceiptHandle: "1:7:99",
			}
			d := wp1bCheckEncoding(t, m)
			if d.KeyEncoding != tt.wantEncoding {
				t.Fatalf("key_encoding = %q, want %q", d.KeyEncoding, tt.wantEncoding)
			}
		})
	}
}

// TestWP1BMessagePrintableKeyUnchanged pins the wire bytes for the
// common key shapes: printable keys (ASCII or not) are emitted as they
// always were, so existing consumers see no difference.
func TestWP1BMessagePrintableKeyUnchanged(t *testing.T) {
	for key, want := range map[string]string{
		"customer-42": `"customer-42"`,
		`a"b\c`:       `"a\"b\\c"`,
		"clé-客户":      `"clé-客户"`,
	} {
		m := Message{Topic: "t", Key: key, Payload: json.RawMessage(`1`)}
		got := string(m.AppendJSON(nil))
		wantAll := `{"topic":"t","partition":0,"offset":0,"key":` + want + `,"payload":1,"timestamp":0}`
		if got != wantAll {
			t.Fatalf("AppendJSON(key %q) = %s, want %s", key, got, wantAll)
		}
	}
}

// FuzzWP1BMessageAppendJSON asserts the consume encoding is valid JSON
// and lossless for arbitrary keys and payloads.
func FuzzWP1BMessageAppendJSON(f *testing.F) {
	f.Add("", []byte(`{"a":1}`))
	f.Add("k", []byte("plain text"))
	f.Add("\x01", []byte{0x00, 0xff})
	f.Add("\xff\xfe", []byte(" 42 "))
	f.Add("\U000E0067", []byte("\x7f"))
	f.Add("a\u2028b", []byte(`"s"`))
	f.Fuzz(func(t *testing.T, key string, payload []byte) {
		m := Message{
			Topic: "t", Partition: 2, Offset: 9, Key: key,
			Payload: payload, Timestamp: 1, ReceiptHandle: "2:9:1",
		}
		d := wp1bCheckEncoding(t, m)
		if key != "" && utf8.ValidString(key) && d.KeyEncoding != "" {
			t.Fatalf("valid UTF-8 key %q got key_encoding %q", key, d.KeyEncoding)
		}
	})
}
