package wal

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Under load appends keep arriving while a batch's write and sync are in
// flight. Those appends must land in a recycled buffer sized to the
// previous batch; before the spare buffer they started a fresh one-frame
// allocation and doubled their way back up on every batch, and the
// written buffer was dropped because the slot was already taken. The
// records staged into recycled buffers must also replay intact: a
// recycled buffer aliasing a batch still being written would show up as
// a checksum failure or a wrong payload.
func TestZZWP5WriteBufferRecycledWhileSyncInFlight(t *testing.T) {
	dir := t.TempDir()
	// The hour-long backstop keeps the sync loop idle: the test drives
	// every flush itself.
	l, err := Open(dir, Options{SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	payloadFor := func(round, i int) []byte {
		return fmt.Appendf(nil, "r%d-i%03d-%s", round, i, bytes.Repeat([]byte("b"), 90))
	}
	stage := func(p []byte) {
		t.Helper()
		if _, _, err := l.stage(len(p), func(dst []byte) []byte { return append(dst, p...) }); err != nil {
			t.Fatal(err)
		}
	}

	const perBatch = 64
	frame := frameHeaderSize + len(payloadFor(0, 0))
	var round atomic.Int64
	var armed atomic.Bool
	var capsDuringSync []int
	restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
		if op != syncfile.OpSyncData || !armed.CompareAndSwap(true, false) {
			return nil
		}
		// A record arriving while this batch's sync is in flight (the
		// flusher holds fileOps, not mu).
		stage(payloadFor(int(round.Load()), perBatch))
		l.mu.Lock()
		capsDuringSync = append(capsDuringSync, cap(l.writeBuffer))
		l.mu.Unlock()
		return nil
	})
	defer restore()

	const rounds = 4
	var want []string
	for r := range rounds {
		round.Store(int64(r))
		// The previous round's in-flight arrival is already staged.
		for i := range perBatch {
			if r > 0 && i == 0 {
				continue
			}
			p := payloadFor(r, i)
			stage(p)
		}
		armed.Store(true)
		l.flushSync()
	}
	restore()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	if len(capsDuringSync) != rounds {
		t.Fatalf("hook ran %d times, want %d", len(capsDuringSync), rounds)
	}
	// Round 0 has no spare yet. From round 1 on, the in-flight arrival
	// must land in the recycled buffer of a whole earlier batch.
	for r, c := range capsDuringSync[1:] {
		if c < perBatch*frame {
			t.Fatalf("round %d: record staged during the sync went into a %d-byte buffer, want the recycled >= %d-byte one (every batch regrows from one frame)", r+1, c, perBatch*frame)
		}
	}

	for r := range rounds {
		first := 0
		if r > 0 {
			first = 1
			want = append(want, string(payloadFor(r-1, perBatch)))
		}
		for i := first; i < perBatch; i++ {
			want = append(want, string(payloadFor(r, i)))
		}
	}
	want = append(want, string(payloadFor(rounds-1, perBatch)))
	var got []Record
	if err := Replay(dir, 0, 0, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	assertPayloads(t, got, want...)
	for i, r := range got {
		if r.ID.Seq != uint64(i) {
			t.Fatalf("record %d has seq %d", i, r.ID.Seq)
		}
	}
}
