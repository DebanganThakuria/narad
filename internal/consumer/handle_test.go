package consumer

import (
	"errors"
	"strings"
	"testing"
)

func TestHandleRoundTrip(t *testing.T) {
	t.Parallel()
	secret := []byte("test-secret-at-least-16-bytes")

	cases := []struct {
		name      string
		topic     string
		partition int32
		offset    int64
		exp       int64
		nonce     int64
	}{
		{"basic", "orders", 0, 5, 1_700_000_000_000, 1},
		{"high values", "events.high", 999, 1 << 40, 1 << 50, 1 << 33},
		{"empty topic", "", 0, 0, 0, 0}, // edge case — len-prefixed handles zero length fine
		{"long topic", strings.Repeat("a", 256), 7, 42, 999, 17},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := EncodeHandle(secret, tc.topic, tc.partition, tc.offset, tc.exp, tc.nonce)
			got, err := DecodeHandle(secret, raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Topic != tc.topic || got.Partition != tc.partition ||
				got.Offset != tc.offset || got.ExpiresAtUnixMs != tc.exp ||
				got.Nonce != tc.nonce {
				t.Fatalf("round-trip mismatch: got %+v want topic=%q partition=%d offset=%d exp=%d nonce=%d",
					got, tc.topic, tc.partition, tc.offset, tc.exp, tc.nonce)
			}
		})
	}
}

func TestHandleHMACMismatch(t *testing.T) {
	t.Parallel()
	secretA := []byte("secret-a-at-least-16-bytes!!")
	secretB := []byte("secret-b-at-least-16-bytes!!")

	raw := EncodeHandle(secretA, "orders", 0, 1, 1, 1)
	_, err := DecodeHandle(secretB, raw)
	if !errors.Is(err, ErrHandleHMACMismatch) {
		t.Fatalf("expected ErrHandleHMACMismatch, got %v", err)
	}
}

func TestHandleTamperedPayload(t *testing.T) {
	t.Parallel()
	secret := []byte("secret-aaaaaaaaaaaaaaaa")
	raw := EncodeHandle(secret, "orders", 0, 5, 1, 1)

	// Flip a byte in the payload portion.
	dot := strings.IndexByte(raw, '.')
	if dot <= 0 {
		t.Fatal("expected '.' separator in handle")
	}
	tampered := []byte(raw)
	tampered[dot-1] ^= 1 // toggle last byte of payload b64
	if _, err := DecodeHandle(secret, string(tampered)); err == nil {
		t.Fatal("expected tampered payload to fail; got nil")
	}
}

func TestHandleMalformed(t *testing.T) {
	t.Parallel()
	secret := []byte("secret-aaaaaaaaaaaaaaaa")

	cases := []string{
		"",
		"no-dot-anywhere",
		".no-payload",
		"no-mac.",
		"!!!.@@@", // invalid b64
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeHandle(secret, raw)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", raw)
			}
		})
	}
}

func TestHandleMatchTopic(t *testing.T) {
	t.Parallel()
	h := Handle{Topic: "orders"}
	if err := h.MatchTopic("orders"); err != nil {
		t.Fatalf("matching topic should pass: %v", err)
	}
	if err := h.MatchTopic("other"); !errors.Is(err, ErrHandleTopicMismatch) {
		t.Fatalf("mismatching topic should return ErrHandleTopicMismatch, got %v", err)
	}
}
