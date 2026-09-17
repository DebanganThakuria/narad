package history

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestReadParsesRecords(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"op":"meta","call":1,"ret":1,"run_id":"r1","topics":["orders"],"visibility_timeout_ms":3000}
{"op":"produce","msg":"m1","path":"orders","call":10,"ret":12,"status":202,"outcome":"ok"}
{"op":"deliver","msg":"m1","path":"orders","call":20,"ret":22,"status":200,"outcome":"ok"}
{"op":"ack","msg":"m1","path":"orders","call":30,"ret":32,"status":204,"outcome":"ok"}
{"op":"fault","kind":"kill","target":"narad-1","call":40,"ret":50}
`)

	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !log.HasMeta || log.Meta.RunID != "r1" || log.Meta.VisibilityTimeoutMs != 3000 {
		t.Fatalf("meta = %+v, want run r1 with a 3000ms visibility timeout", log.Meta)
	}
	if len(log.Ops) != 3 {
		t.Fatalf("ops = %d, want 3", len(log.Ops))
	}
	if len(log.Faults) != 1 || log.Faults[0].Target != "narad-1" {
		t.Fatalf("faults = %+v, want one against narad-1", log.Faults)
	}
	if log.Truncated != 0 || log.Skewed != 0 {
		t.Errorf("truncated=%d skewed=%d, want 0/0", log.Truncated, log.Skewed)
	}
}

// A run killed mid-write leaves a half-written final line. Refusing to
// read it would discard the evidence from exactly the runs most worth
// checking, so the prefix is kept and the truncation is counted.
func TestReadToleratesATruncatedFinalLine(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"op":"produce","msg":"m1","path":"orders","call":10,"ret":12,"outcome":"ok"}
{"op":"deliver","msg":"m1","path":"orde`)

	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(log.Ops) != 1 {
		t.Fatalf("ops = %d, want 1", len(log.Ops))
	}
	if log.Truncated != 1 {
		t.Errorf("truncated = %d, want 1", log.Truncated)
	}
}

// Corruption in the middle of a file is not truncation, and quietly
// skipping it would let a checker report a verdict over a history it did
// not fully read.
func TestReadRejectsAMalformedLineInTheMiddle(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"op":"produce","msg":"m1","path":"orders","call":10,"ret":12,"outcome":"ok"}
not json at all
{"op":"ack","msg":"m1","path":"orders","call":30,"ret":32,"outcome":"ok"}
`)

	if _, err := Read(path); err == nil {
		t.Fatal("Read should reject a malformed line that is not the last")
	}
}

func TestReadRejectsARecordWithNoOp(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"msg":"m1","call":10,"ret":12}
{"op":"ack","msg":"m1","call":30,"ret":32}
`)
	if _, err := Read(path); err == nil {
		t.Fatal("Read should reject a record with no op")
	}
}

// Porcupine requires a closed, non-inverted interval. A backwards clock
// is clamped rather than fatal, and counted so the verdict is not read
// as if the clock had behaved.
func TestReadClampsInvertedIntervals(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"op":"deliver","msg":"m1","path":"orders","call":100,"ret":50,"outcome":"ok"}
`)
	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if log.Skewed != 1 {
		t.Fatalf("skewed = %d, want 1", log.Skewed)
	}
	if got := log.Ops[0]; got.Ret != got.Call {
		t.Errorf("interval = [%d,%d], want the return clamped to the call", got.Call, got.Ret)
	}
}

// The driver and the fault injector are separate processes writing
// separate files; merging them is the normal case, and the merged
// result has to be in time order for the checker's real-time reasoning
// to hold.
func TestReadMergesFilesInTimeOrder(t *testing.T) {
	t.Parallel()

	ops := writeFile(t, "h.jsonl", `{"op":"deliver","msg":"m2","path":"orders","call":300,"ret":310,"outcome":"ok"}
{"op":"produce","msg":"m1","path":"orders","call":100,"ret":110,"outcome":"ok"}
`)
	faults := writeFile(t, "f.jsonl", `{"op":"fault","kind":"partition","target":"narad-3","call":500,"ret":600}
{"op":"fault","kind":"kill","target":"narad-1","call":200,"ret":250}
`)

	log, err := Read(ops, faults)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(log.Ops) != 2 || log.Ops[0].Msg != "m1" {
		t.Fatalf("ops not sorted by call time: %+v", log.Ops)
	}
	if len(log.Faults) != 2 || log.Faults[0].Kind != FaultKill {
		t.Fatalf("faults not sorted by call time: %+v", log.Faults)
	}
}

// An older checker must still be able to read a history written by a
// newer driver, so an op it does not recognize is skipped rather than
// fatal.
func TestReadIgnoresUnknownOps(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "h.jsonl", `{"op":"something-new","call":10,"ret":12}
{"op":"produce","msg":"m1","path":"orders","call":20,"ret":22,"outcome":"ok"}
`)
	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(log.Ops) != 1 {
		t.Errorf("ops = %d, want the unknown op skipped and the known one kept", len(log.Ops))
	}
}

func TestReadMissingFileIsAnError(t *testing.T) {
	t.Parallel()

	if _, err := Read(filepath.Join(t.TempDir(), "absent.jsonl")); err == nil {
		t.Fatal("Read should fail on a missing file")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error should wrap os.ErrNotExist, got %v", err)
	}
}

// Records are small and fixed-shape; a line past the cap is a corrupt
// file, not a large record, and this is what stands between that and an
// unbounded read.
func TestReadRejectsAnOversizedLine(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("a", maxLineBytes+1)
	path := writeFile(t, "h.jsonl", huge+"\n")

	_, err := Read(path)
	if err == nil {
		t.Fatal("Read should reject a line larger than the cap")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error should say the line exceeds the cap, got: %v", err)
	}
}

func TestRecorderRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "h.jsonl")
	recorder, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	if recorder.Path() != path {
		t.Errorf("Path() = %q, want %q", recorder.Path(), path)
	}
	recorder.Meta("run-1", []string{"orders"}, map[string][]string{"orders": {"orders"}}, 3*time.Second)
	recorder.Record(Record{Op: OpProduce, Msg: "m1", Path: "orders", Call: 10, Ret: 12, Status: 202, Outcome: OutcomeOK})
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !log.HasMeta || log.Meta.RunID != "run-1" || log.Meta.VisibilityTimeoutMs != 3000 {
		t.Fatalf("meta round trip failed: %+v", log.Meta)
	}
	if got := log.Meta.Paths["orders"]; len(got) != 1 || got[0] != "orders" {
		t.Fatalf("declared paths did not round trip: %+v", log.Meta.Paths)
	}
	if len(log.Ops) != 1 || log.Ops[0].Msg != "m1" || log.Ops[0].Outcome != OutcomeOK {
		t.Fatalf("op round trip failed: %+v", log.Ops)
	}
}

// The driver calls these unconditionally, so a nil recorder has to be
// safe rather than guarded at every call site.
func TestNilRecorderIsSafe(t *testing.T) {
	t.Parallel()

	recorder, err := NewRecorder("")
	if err != nil {
		t.Fatalf("NewRecorder(\"\"): %v", err)
	}
	if recorder != nil {
		t.Fatal("an empty path should give a nil recorder")
	}
	recorder.Record(Record{Op: OpProduce})
	recorder.Meta("run", nil, nil, time.Second)
	if got := recorder.Path(); got != "" {
		t.Errorf("Path() = %q, want empty", got)
	}
	if err := recorder.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestRecorderConcurrentWrites(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "h.jsonl")
	recorder, err := NewRecorder(path)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	const workers, each = 8, 100
	done := make(chan struct{})
	for w := range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range each {
				recorder.Record(Record{
					Op: OpDeliver, Msg: "m", Path: "orders",
					Client: w, Call: int64(i), Ret: int64(i) + 1, Outcome: OutcomeOK,
				})
			}
		}()
	}
	for range workers {
		<-done
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	log, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(log.Ops) != workers*each {
		t.Fatalf("ops = %d, want %d: concurrent writes must not interleave", len(log.Ops), workers*each)
	}
}

func TestOutcomeClassification(t *testing.T) {
	t.Parallel()

	boom := errors.New("connection reset")
	cases := []struct {
		name string
		got  Outcome
		want Outcome
	}{
		{"accepted produce", ProduceOutcome(202, nil), OutcomeOK},
		{"throttled produce", ProduceOutcome(429, nil), OutcomeRejected},
		{"unavailable produce", ProduceOutcome(503, nil), OutcomeRejected},
		{"errored produce is ambiguous", ProduceOutcome(0, boom), OutcomeAmbiguous},
		{"errored produce outranks its status", ProduceOutcome(202, boom), OutcomeAmbiguous},
		{"confirmed ack", AckOutcome(204, nil), OutcomeOK},
		{"gone ack", AckOutcome(410, nil), OutcomeRejected},
		{"throttled ack", AckOutcome(429, nil), OutcomeRejected},
		{"errored ack is ambiguous", AckOutcome(0, boom), OutcomeAmbiguous},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}
