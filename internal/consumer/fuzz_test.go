package consumer

import (
	"math"
	"testing"
)

// FuzzDecodeHandle throws arbitrary receipt handles at the parser. A
// handle that decodes must be valid, and re-encoding it must decode to
// the same value.
func FuzzDecodeHandle(f *testing.F) {
	f.Add("0:0:1")
	f.Add("3:17:99")
	f.Add(EncodeHandle(Handle{Partition: math.MaxInt32, Offset: math.MaxInt64, Nonce: math.MaxInt64}))
	f.Add("")
	f.Add("::")
	f.Add("1:2")
	f.Add("1:2:3:4")
	f.Add("-1:2:3")
	f.Add("1:2:0")
	f.Add("01:02:03")
	f.Add("99999999999999999999:1:1")
	f.Add("1:2:3 ")
	f.Add("١:2:3")

	f.Fuzz(func(t *testing.T, s string) {
		h, err := DecodeHandle(s)
		if err != nil {
			return
		}
		if verr := ValidateHandle(h); verr != nil {
			t.Fatalf("DecodeHandle(%q) returned an invalid handle %+v: %v", s, h, verr)
		}
		again, err := DecodeHandle(EncodeHandle(h))
		if err != nil {
			t.Fatalf("DecodeHandle(EncodeHandle(%+v)): %v", h, err)
		}
		if again != h {
			t.Fatalf("round trip: %+v -> %+v", h, again)
		}
	})
}

// FuzzHandleRoundTrip encodes fuzzer-chosen fields; every valid handle
// must survive, every invalid one must be refused on decode.
func FuzzHandleRoundTrip(f *testing.F) {
	f.Add(0, int64(0), int64(1))
	f.Add(7, int64(42), int64(1_000_000))
	f.Add(-1, int64(0), int64(1))
	f.Add(0, int64(-1), int64(1))
	f.Add(0, int64(0), int64(0))
	f.Add(math.MaxInt32, int64(math.MaxInt64), int64(math.MaxInt64))
	f.Add(math.MinInt64, int64(math.MinInt64), int64(math.MinInt64))

	f.Fuzz(func(t *testing.T, partition int, offset, nonce int64) {
		h := Handle{Partition: partition, Offset: offset, Nonce: nonce}
		got, err := DecodeHandle(EncodeHandle(h))
		if ValidateHandle(h) != nil {
			if err == nil {
				t.Fatalf("invalid handle %+v decoded as %+v", h, got)
			}
			return
		}
		if err != nil {
			t.Fatalf("DecodeHandle(EncodeHandle(%+v)): %v", h, err)
		}
		if got != h {
			t.Fatalf("round trip: %+v -> %+v", h, got)
		}
	})
}
