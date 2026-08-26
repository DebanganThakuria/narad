package codec

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// This file is a throwaway measurement harness: it compares the current
// default frame codec (zstd SpeedFastest) against S2 on realistic frame
// payloads before deciding whether a swap is worth it. It reports wall
// time (ns/op), process CPU time (cpu-ns/op), the ratio between them
// (>1 under parallelism means real concurrency; ~1 means serialised),
// throughput over the UNCOMPRESSED bytes, and the compression ratio.

// -- S2 codec under test --------------------------------------------------

// FlagS2 is the candidate on-disk codec ID. Declared here (not in
// codec.go) so the production flag space stays untouched until we decide.
const flagS2Candidate = uint8(2)

type s2Codec struct{ better bool }

func (s2Codec) Flag() uint8 { return flagS2Candidate }

// Encode honours the Codec contract (append to dst) even though
// encodeFrame always passes dst=nil, so the numbers reflect the real
// implementation we would ship rather than a fast-path shortcut.
func (c s2Codec) Encode(dst, src []byte) []byte {
	need := s2.MaxEncodedLen(len(src))
	if need < 0 {
		panic("codec: s2 input too large")
	}
	if cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need)
		copy(grown, dst)
		dst = grown
	}
	scratch := dst[len(dst) : len(dst) : len(dst)+need]
	var out []byte
	if c.better {
		out = s2.EncodeBetter(scratch, src)
	} else {
		out = s2.Encode(scratch, src)
	}
	return dst[:len(dst)+len(out)]
}

func (s2Codec) Decode(dst, src []byte, dstSizeHint int) ([]byte, error) {
	n, err := s2.DecodedLen(src)
	if err != nil {
		return nil, fmt.Errorf("codec: s2 decoded len: %w", err)
	}
	if cap(dst)-len(dst) < n {
		grown := make([]byte, len(dst), len(dst)+n)
		copy(grown, dst)
		dst = grown
	}
	scratch := dst[len(dst) : len(dst) : len(dst)+n]
	out, err := s2.Decode(scratch, src)
	if err != nil {
		return nil, fmt.Errorf("codec: s2 decode: %w", err)
	}
	return dst[:len(dst)+len(out)], nil
}

// -- corpora --------------------------------------------------------------

// buildFrameInner mirrors storage.encodeFrame's inner payload: a
// concatenation of 4-byte-length-prefixed keyed records, each record
// being the keyed envelope (version, uvarint keyLen, key, 8-byte ts)
// wrapping the message payload. Records are appended until the inner
// payload reaches targetBytes.
func buildFrameInner(targetBytes int, payload func(i int) []byte) []byte {
	inner := make([]byte, 0, targetBytes+4096)
	for i := 0; len(inner) < targetBytes; i++ {
		key := fmt.Sprintf("user-%d", i%10000)
		body := payload(i)

		rec := make([]byte, 0, 1+binary.MaxVarintLen32+len(key)+8+len(body))
		rec = append(rec, 0x02)
		rec = binary.AppendUvarint(rec, uint64(len(key)))
		rec = append(rec, key...)
		rec = binary.BigEndian.AppendUint64(rec, uint64(1750000000000+int64(i)))
		rec = append(rec, body...)

		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(rec)))
		inner = append(inner, lb[:]...)
		inner = append(inner, rec...)
	}
	return inner
}

// jsonEvent is the common case for a message bus: structured events with
// repeated field names and low-cardinality values. Highly compressible.
func jsonEvent(rng *rand.Rand) func(int) []byte {
	statuses := []string{"created", "authorized", "captured", "failed", "refunded"}
	methods := []string{"card", "upi", "netbanking", "wallet"}
	return func(i int) []byte {
		return fmt.Appendf(nil,
			`{"event_id":"evt_%012d","entity":"payment","status":"%s","method":"%s","amount":%d,"currency":"INR","merchant_id":"acc_%08d","created_at":%d,"attempts":%d,"notes":{"order_id":"ord_%010d","source":"checkout"}}`,
			i, statuses[rng.Intn(len(statuses))], methods[rng.Intn(len(methods))],
			rng.Intn(1000000), rng.Intn(100000), 1750000000+i, rng.Intn(3), i)
	}
}

// randomBytes is the worst case: payloads that are already compressed or
// encrypted, where every codec is pure overhead.
func randomBytes(rng *rand.Rand, size int) func(int) []byte {
	return func(int) []byte {
		b := make([]byte, size)
		rng.Read(b)
		return b
	}
}

type corpus struct {
	name  string
	inner []byte
}

func corpora(tb testing.TB) []corpus {
	tb.Helper()
	const (
		smallFrame = 4 << 10 // high fan-out: many partitions, tiny frames
		largeFrame = 1 << 20 // FlushBytes default: the fat-batch case
	)
	out := []corpus{}
	for _, size := range []struct {
		label string
		bytes int
	}{{"4KiB", smallFrame}, {"1MiB", largeFrame}} {
		rng := rand.New(rand.NewSource(1))
		out = append(out, corpus{
			name:  "json/" + size.label,
			inner: buildFrameInner(size.bytes, jsonEvent(rng)),
		})
		rng = rand.New(rand.NewSource(2))
		out = append(out, corpus{
			name:  "random/" + size.label,
			inner: buildFrameInner(size.bytes, randomBytes(rng, 256)),
		})
	}
	return out
}

func codecsUnderTest(tb testing.TB) []struct {
	name string
	c    Codec
} {
	tb.Helper()
	zstdFastest, err := NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		tb.Fatalf("zstd codec: %v", err)
	}
	return []struct {
		name string
		c    Codec
	}{
		{"zstd-fastest", zstdFastest},
		{"s2", s2Codec{}},
		{"s2-better", s2Codec{better: true}},
		{"noop", NewNoopCodec()},
	}
}

// -- CPU time -------------------------------------------------------------

func nowNanos() int64 { return time.Now().UnixNano() }

func cpuNanos() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	user := ru.Utime.Sec*1e9 + int64(ru.Utime.Usec)*1e3
	sys := ru.Stime.Sec*1e9 + int64(ru.Stime.Usec)*1e3
	return user + sys
}

// reportCPU records CPU-time-per-op alongside the wall time testing
// already reports, plus the cpu/wall ratio.
func reportCPU(b *testing.B, cpuStart int64, wallNsPerOp func() float64) {
	cpuPerOp := float64(cpuNanos()-cpuStart) / float64(b.N)
	b.ReportMetric(cpuPerOp, "cpu-ns/op")
	if w := wallNsPerOp(); w > 0 {
		b.ReportMetric(cpuPerOp/w, "cpu/wall")
	}
}

// -- ratios + round-trip --------------------------------------------------

// TestCodecCompressionRatios prints the size side of the trade the
// benchmarks measure the speed of, and proves the candidate S2 codec
// round-trips (including the append-to-dst contract) so the throughput
// numbers describe a codec we could actually ship.
func TestCodecCompressionRatios(t *testing.T) {
	for _, cp := range corpora(t) {
		t.Logf("corpus %s: %d uncompressed bytes", cp.name, len(cp.inner))
		for _, cd := range codecsUnderTest(t) {
			encoded := cd.c.Encode(nil, cp.inner)

			decoded, err := cd.c.Decode(nil, encoded, len(cp.inner))
			if err != nil {
				t.Fatalf("%s/%s: decode: %v", cd.name, cp.name, err)
			}
			if string(decoded) != string(cp.inner) {
				t.Fatalf("%s/%s: round-trip mismatch", cd.name, cp.name)
			}
			// Same again with a non-empty dst, exercising the append contract.
			prefix := []byte("prefix")
			withPrefix := cd.c.Encode(append([]byte(nil), prefix...), cp.inner)
			if string(withPrefix[:len(prefix)]) != string(prefix) {
				t.Fatalf("%s/%s: Encode clobbered dst", cd.name, cp.name)
			}
			roundTrip, err := cd.c.Decode(append([]byte(nil), prefix...), withPrefix[len(prefix):], len(cp.inner))
			if err != nil {
				t.Fatalf("%s/%s: decode with dst: %v", cd.name, cp.name, err)
			}
			if string(roundTrip) != string(prefix)+string(cp.inner) {
				t.Fatalf("%s/%s: round-trip with dst mismatch", cd.name, cp.name)
			}

			t.Logf("  %-14s %9d bytes  ratio %.2fx", cd.name, len(encoded),
				float64(len(cp.inner))/float64(len(encoded)))
		}
	}
}

// -- benchmarks -----------------------------------------------------------

func BenchmarkCodecEncode(b *testing.B) {
	for _, cd := range codecsUnderTest(b) {
		for _, cp := range corpora(b) {
			b.Run(cd.name+"/"+cp.name, func(b *testing.B) {
				encoded := cd.c.Encode(nil, cp.inner)
				b.SetBytes(int64(len(cp.inner)))
				b.ResetTimer()

				cpuStart := cpuNanos()
				start := nowNanos()
				var dst []byte
				for range b.N {
					dst = cd.c.Encode(dst[:0], cp.inner)
				}
				wall := float64(nowNanos()-start) / float64(b.N)
				b.StopTimer()
				// Reported after ResetTimer: ResetTimer clears b.extra, so a
				// metric recorded before it never reaches the output.
				b.ReportMetric(float64(len(cp.inner))/float64(len(encoded)), "ratio")
				reportCPU(b, cpuStart, func() float64 { return wall })
				_ = dst
			})
		}
	}
}

func BenchmarkCodecDecode(b *testing.B) {
	for _, cd := range codecsUnderTest(b) {
		for _, cp := range corpora(b) {
			b.Run(cd.name+"/"+cp.name, func(b *testing.B) {
				encoded := cd.c.Encode(nil, cp.inner)
				b.SetBytes(int64(len(cp.inner)))
				b.ResetTimer()

				cpuStart := cpuNanos()
				start := nowNanos()
				var dst []byte
				for range b.N {
					var err error
					dst, err = cd.c.Decode(dst[:0], encoded, len(cp.inner))
					if err != nil {
						b.Fatal(err)
					}
				}
				wall := float64(nowNanos()-start) / float64(b.N)
				b.StopTimer()
				reportCPU(b, cpuStart, func() float64 { return wall })
			})
		}
	}
}

// BenchmarkCodecEncodeParallel is the one that matters most for narad:
// many partitions commit concurrently through ONE shared codec. zstd
// pools a bounded set of encoder states (GOMAXPROCS, capped at 32) and
// blocks when they are all busy; S2 is stateless, so it has no pool to
// contend on. Watch cpu/wall: values well below GOMAXPROCS mean threads
// are parked waiting for an encoder rather than compressing.
func BenchmarkCodecEncodeParallel(b *testing.B) {
	for _, cd := range codecsUnderTest(b) {
		for _, cp := range corpora(b) {
			b.Run(cd.name+"/"+cp.name, func(b *testing.B) {
				b.SetBytes(int64(len(cp.inner)))
				b.ResetTimer()

				cpuStart := cpuNanos()
				start := nowNanos()
				b.RunParallel(func(pb *testing.PB) {
					var dst []byte
					for pb.Next() {
						dst = cd.c.Encode(dst[:0], cp.inner)
					}
					_ = dst
				})
				wall := float64(nowNanos()-start) / float64(b.N)
				b.StopTimer()
				reportCPU(b, cpuStart, func() float64 { return wall })
			})
		}
	}
}
