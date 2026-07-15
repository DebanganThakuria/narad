package topic

// Produce is octet-stream permissive, so consume must be able to return
// ANY produced bytes inside its JSON envelope: JSON values verbatim,
// non-JSON text as a JSON string, and binary base64-wrapped with an
// explicit payload_encoding marker. Every branch must yield valid JSON
// from both AppendJSON and json.Marshal (which delegates to it) — a
// payload that encodes on the local consume path but fails the node-RPC
// marshal path becomes an unconsumable poison message.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestMessageEncodingByPayloadKind(t *testing.T) {
	base := Message{Topic: "orders", Partition: 1, Offset: 7, Timestamp: 42, ReceiptHandle: "1:7:99"}

	tests := []struct {
		name         string
		payload      []byte
		wantPayload  string // raw JSON expected in the "payload" field
		wantEncoding string // expected payload_encoding, "" = absent
	}{
		{"json object", []byte(`{"a":1}`), `{"a":1}`, ""},
		{"json string", []byte(`"already quoted"`), `"already quoted"`, ""},
		{"json number", []byte(`42`), `42`, ""},
		{"empty is null", nil, `null`, ""},
		{"plain text", []byte("hello world"), `"hello world"`, ""},
		{"text needing escapes", []byte("line1\nline2\t\"quoted\""), `"line1\nline2\t\"quoted\""`, ""},
		{"binary", []byte{0x00, 0xFF, 0x8A, 0x01}, `"` + base64.StdEncoding.EncodeToString([]byte{0x00, 0xFF, 0x8A, 0x01}) + `"`, "base64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := base
			m.Payload = tt.payload

			got := m.AppendJSON(nil)
			if !json.Valid(got) {
				t.Fatalf("AppendJSON produced invalid JSON: %s", got)
			}

			// MarshalJSON must delegate: identical bytes, never an error.
			viaMarshal, err := json.Marshal(m)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			if !bytes.Equal(got, viaMarshal) {
				t.Fatalf("AppendJSON and json.Marshal disagree:\n  append:  %s\n  marshal: %s", got, viaMarshal)
			}

			var decoded struct {
				Payload  json.RawMessage `json:"payload"`
				Encoding string          `json:"payload_encoding"`
			}
			if err := json.Unmarshal(got, &decoded); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if string(decoded.Payload) != tt.wantPayload {
				t.Fatalf("payload field = %s, want %s", decoded.Payload, tt.wantPayload)
			}
			if decoded.Encoding != tt.wantEncoding {
				t.Fatalf("payload_encoding = %q, want %q", decoded.Encoding, tt.wantEncoding)
			}
		})
	}
}

// Whatever bytes were produced must be recoverable from the response.
func TestMessagePayloadRoundTrips(t *testing.T) {
	payloads := map[string][]byte{
		"text":   []byte("hello world"),
		"binary": {0x1F, 0x8B, 0x00, 0xFF, 0xFE},
	}
	for name, produced := range payloads {
		t.Run(name, func(t *testing.T) {
			m := Message{Topic: "t", Payload: produced}
			var decoded struct {
				Payload  string `json:"payload"`
				Encoding string `json:"payload_encoding"`
			}
			if err := json.Unmarshal(m.AppendJSON(nil), &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := []byte(decoded.Payload)
			if decoded.Encoding == "base64" {
				var err error
				got, err = base64.StdEncoding.DecodeString(decoded.Payload)
				if err != nil {
					t.Fatalf("base64 decode: %v", err)
				}
			}
			if !bytes.Equal(got, produced) {
				t.Fatalf("round trip = %q, want %q", got, produced)
			}
		})
	}
}

// The verbatim splice must still match encoding/json's omitempty
// behavior for the surrounding fields.
func TestMessageOmitemptyFields(t *testing.T) {
	m := Message{Topic: "t", Partition: 0, Offset: 3, Payload: []byte(`1`), Timestamp: 9}
	got := string(m.AppendJSON(nil))
	want := `{"topic":"t","partition":0,"offset":3,"payload":1,"timestamp":9}`
	if got != want {
		t.Fatalf("AppendJSON = %s, want %s", got, want)
	}
}
