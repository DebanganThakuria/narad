package consumer

import (
	"errors"
	"testing"
)

func TestDecodeHandleRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"%%%",
		"0:1",
		"0:1:2:3",
		":0:1",
		"abc:1:2",
		"+0:1:2",
		"0:+1:2",
		"0:1:+2",
		"999999999999999999999999:1:2",
		"0:999999999999999999999999:2",
		"0:1:999999999999999999999999",
		EncodeHandle(Handle{Partition: -1, Offset: 0, Nonce: 1}),
		EncodeHandle(Handle{Partition: 0, Offset: -1, Nonce: 1}),
		EncodeHandle(Handle{Partition: 0, Offset: 0, Nonce: 0}),
	} {
		if _, err := DecodeHandle(input); !errors.Is(err, ErrHandleMalformed) {
			t.Fatalf("DecodeHandle(%q) error = %v, want %v", input, err, ErrHandleMalformed)
		}
	}
}

func TestEncodeHandleRoundTrip(t *testing.T) {
	t.Parallel()

	input := Handle{Partition: 2, Offset: 42, Nonce: 7}
	want := Handle{Partition: 2, Offset: 42, Nonce: 7}
	got, err := DecodeHandle(EncodeHandle(input))
	if err != nil {
		t.Fatalf("DecodeHandle() error = %v", err)
	}
	if got != want {
		t.Fatalf("DecodeHandle() = %+v, want %+v", got, want)
	}
	if encoded := EncodeHandle(input); encoded != "2:42:7" {
		t.Fatalf("EncodeHandle() = %q, want 2:42:7", encoded)
	}
}

func TestEncodeHandleRoundTripMultiplePartitions(t *testing.T) {
	t.Parallel()

	for _, h := range []Handle{{Partition: 0, Offset: 1, Nonce: 1}, {Partition: 2, Offset: 9, Nonce: 3}} {
		got, err := DecodeHandle(EncodeHandle(h))
		if err != nil {
			t.Fatalf("DecodeHandle() error = %v", err)
		}
		if got != h {
			t.Fatalf("DecodeHandle() = %+v, want %+v", got, h)
		}
	}
}

func TestEncodeHandleProducesNonEmptyString(t *testing.T) {
	t.Parallel()

	if got := EncodeHandle(Handle{Partition: 0, Offset: 0, Nonce: 1}); got == "" {
		t.Fatal("EncodeHandle() = empty, want non-empty")
	}
}

func TestEncodeHandleDeterministicForSameInput(t *testing.T) {
	t.Parallel()

	h := Handle{Partition: 0, Offset: 1, Nonce: 1}
	if a, b := EncodeHandle(h), EncodeHandle(h); a != b {
		t.Fatalf("EncodeHandle() outputs differ: %q vs %q", a, b)
	}
}

func TestDecodeHandleWithoutTopic(t *testing.T) {
	t.Parallel()

	handle := EncodeHandle(Handle{Partition: 0, Offset: 1, Nonce: 1})
	if handle != "0:1:1" {
		t.Fatalf("EncodeHandle() = %q, want 0:1:1", handle)
	}
	got, err := DecodeHandle(handle)
	if err != nil {
		t.Fatalf("DecodeHandle() error = %v", err)
	}
	want := Handle{Partition: 0, Offset: 1, Nonce: 1}
	if got != want {
		t.Fatalf("DecodeHandle() = %+v, want %+v", got, want)
	}
}

func TestDecodeHandleRejectsWrongFieldCount(t *testing.T) {
	t.Parallel()

	if _, err := DecodeHandle("0:1:2:3"); !errors.Is(err, ErrHandleMalformed) {
		t.Fatal("DecodeHandle() error = nil, want error")
	}
}

func TestDecodeHandleRejectsEmptyString(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle("")
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsZeroNonce(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle(EncodeHandle(Handle{Partition: 0, Offset: 1, Nonce: 0}))
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsNegativeOffset(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle(EncodeHandle(Handle{Partition: 0, Offset: -1, Nonce: 1}))
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsNegativePartition(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle(EncodeHandle(Handle{Partition: -1, Offset: 1, Nonce: 1}))
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsNonNumericPayload(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle("bm90LWpzb24")
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsInvalidCharacters(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle("@")
	wantErr(t, err, ErrHandleMalformed)
}

func TestDecodeHandleRejectsWhitespace(t *testing.T) {
	t.Parallel()

	_, err := DecodeHandle("   ")
	wantErr(t, err, ErrHandleMalformed)
}
