package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/tests/linearizability/history"
)

// Histories are written in milliseconds from an arbitrary base, which
// keeps the tests readable and matches the resolution the findings are
// reported at.
const base int64 = 1_700_000_000_000_000_000

func at(msOffset int64) int64 { return base + msOffset*int64(time.Millisecond) }

type op struct {
	kind    history.Op
	msg     string
	path    string
	call    int64
	ret     int64
	outcome history.Outcome
}

func produce(msg, path string, call, ret int64) op {
	return op{history.OpProduce, msg, path, at(call), at(ret), history.OutcomeOK}
}

func deliver(msg, path string, call, ret int64) op {
	return op{history.OpDeliver, msg, path, at(call), at(ret), history.OutcomeOK}
}

func ack(msg, path string, call, ret int64) op {
	return op{history.OpAck, msg, path, at(call), at(ret), history.OutcomeOK}
}

func (o op) with(outcome history.Outcome) op {
	o.outcome = outcome
	return o
}

// buildLog assembles a history.Log the way the reader would, so the
// check tests exercise the same structure a real run produces. Each
// topic produced to is declared as its own only path, which is what a
// run without fan-out records.
func buildLog(ops []op, faults ...history.Record) *history.Log {
	paths := map[string][]string{}
	for _, o := range ops {
		if o.kind == history.OpProduce {
			paths[o.path] = []string{o.path}
		}
	}
	return buildLogWithPaths(paths, ops, faults...)
}

// buildLogWithPaths is buildLog with an explicit declared topology, for
// the fan-out and misroute cases.
func buildLogWithPaths(paths map[string][]string, ops []op, faults ...history.Record) *history.Log {
	log := &history.Log{
		Meta: history.Record{
			Op: history.OpMeta, VisibilityTimeoutMs: 3000, Paths: paths,
		},
		HasMeta: true,
		Faults:  faults,
	}
	for _, o := range ops {
		log.Ops = append(log.Ops, history.Record{
			Op: o.kind, Msg: o.msg, Path: o.path,
			Call: o.call, Ret: o.ret, Outcome: o.outcome,
		})
	}
	return log
}

func fault(kind, target string, call, ret int64) history.Record {
	return history.Record{Op: history.OpFault, Kind: kind, Target: target, Call: at(call), Ret: at(ret)}
}

func defaultOpts() checkOptions {
	return checkOptions{Grace: 5 * time.Second, Timeout: 30 * time.Second, Samples: 10, MinOperations: 1}
}

func TestCleanRunIsOK(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		produce("m2", "orders", 1, 6),
		deliver("m2", "orders", 20, 22),
		ack("m2", "orders", 25, 27),
	})
	res := check(log, defaultOpts())

	if res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK (%s)", res.Verdict, verdictSentence(res))
	}
	if res.PostAckTotal != 0 {
		t.Errorf("post-ack redeliveries = %d, want 0", res.PostAckTotal)
	}
	if res.Messages != 2 || res.AcksOK != 2 || res.Deliveries != 2 {
		t.Errorf("counts: messages=%d acks=%d deliveries=%d, want 2/2/2", res.Messages, res.AcksOK, res.Deliveries)
	}
	if res.failed() {
		t.Error("a clean run must exit zero")
	}
}

// The headline case: a message comes back after its ack while a broker
// was being killed. The delivery contract permits it, so the run passes,
// but the event is still counted and attributed.
func TestPostAckRedeliveryInsideFaultWindowIsExplained(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 120, 122),
		ack("m1", "orders", 125, 127),
	}, fault(history.FaultKill, "narad-2", 100, 110))

	res := check(log, defaultOpts())

	if res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK (%s)", res.Verdict, verdictSentence(res))
	}
	if res.PostAckExplained != 1 || res.PostAckUnexplained != 0 {
		t.Fatalf("explained=%d unexplained=%d, want 1/0", res.PostAckExplained, res.PostAckUnexplained)
	}
	if len(res.PostAckSamples) != 1 || !strings.Contains(res.PostAckSamples[0].Fault, "narad-2") {
		t.Errorf("sample should name the fault, got %+v", res.PostAckSamples)
	}
}

// The same event with no fault to account for it is the anomaly this
// whole tool exists to surface.
func TestPostAckRedeliveryWithNoFaultIsAnomaly(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 120, 122),
		ack("m1", "orders", 125, 127),
	})

	res := check(log, defaultOpts())

	if res.Verdict != verdictAnomaly {
		t.Fatalf("verdict = %s, want ANOMALY", res.Verdict)
	}
	if res.PostAckUnexplained != 1 {
		t.Fatalf("unexplained = %d, want 1", res.PostAckUnexplained)
	}
	if !res.failed() {
		t.Error("an unexplained anomaly must exit non-zero")
	}
}

// A redelivery just past the end of a fault is still the fault's doing:
// the lease it held only expires a visibility timeout later. The grace
// window is what encodes that, and its boundary is worth pinning.
func TestGraceWindowBoundary(t *testing.T) {
	t.Parallel()

	opts := defaultOpts()
	opts.Grace = 5 * time.Second

	// Fault ends at 110ms; grace reaches 5110ms.
	inside := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 5100, 5102),
	}, fault(history.FaultKill, "narad-1", 100, 110))
	if res := check(inside, opts); res.PostAckExplained != 1 || res.PostAckUnexplained != 0 {
		t.Errorf("inside grace: explained=%d unexplained=%d, want 1/0", res.PostAckExplained, res.PostAckUnexplained)
	}

	outside := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 5200, 5202),
	}, fault(history.FaultKill, "narad-1", 100, 110))
	if res := check(outside, opts); res.PostAckUnexplained != 1 {
		t.Errorf("outside grace: unexplained = %d, want 1", res.PostAckUnexplained)
	}
}

func TestStrictModeMakesPostAckRedeliveryAViolation(t *testing.T) {
	t.Parallel()

	opts := defaultOpts()
	opts.Strict = true
	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 120, 122),
	}, fault(history.FaultKill, "narad-2", 100, 110))

	res := check(log, opts)

	if res.Verdict != verdictViolation {
		t.Fatalf("verdict = %s, want VIOLATION", res.Verdict)
	}
	if res.IllegalPartition == "" {
		t.Error("a violation should name the partition that broke")
	}
}

// This is the honest limit of the analysis, and it is asserted so nobody
// later "fixes" it into a false positive. A redelivery whose request was
// already in flight when the ack returned has an ordering where the
// broker handed it out before the ack landed, so it proves nothing.
func TestRedeliveryOverlappingTheAckIsNotCounted(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		// The ack is in flight from 15 to 40; this poll started at 20,
		// before the ack returned.
		ack("m1", "orders", 15, 40),
		deliver("m1", "orders", 20, 45),
	})

	res := check(log, defaultOpts())

	if res.PostAckTotal != 0 {
		t.Fatalf("post-ack = %d, want 0: an overlapping redelivery is unprovable", res.PostAckTotal)
	}
	if res.Verdict == verdictAnomaly {
		t.Fatal("an overlapping redelivery must not be reported as an anomaly")
	}
}

func TestDeliveryOfAMessageNeverProducedIsAViolation(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		deliver("ghost", "orders", 10, 12),
	})
	if res := check(log, defaultOpts()); res.Verdict != verdictViolation {
		t.Fatalf("verdict = %s, want VIOLATION", res.Verdict)
	}
}

// A 429 or 503 produce means the broker answered and refused the
// message. Delivering it afterwards is the broker contradicting itself.
func TestDeliveryOfARefusedProduceIsAViolation(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5).with(history.OutcomeRejected),
		deliver("m1", "orders", 10, 12),
	})
	res := check(log, defaultOpts())
	if res.Verdict != verdictViolation {
		t.Fatalf("verdict = %s, want VIOLATION", res.Verdict)
	}
	if res.ProducedRejected != 1 {
		t.Errorf("rejected produces = %d, want 1", res.ProducedRejected)
	}
}

// A produce whose request errored may still have landed. Delivering it
// is correct behaviour, and must not be reported as delivering something
// that was never produced.
func TestAmbiguousProduceThenDeliveryIsClean(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5).with(history.OutcomeAmbiguous),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
	})
	res := check(log, defaultOpts())

	if res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK (%s)", res.Verdict, verdictSentence(res))
	}
	if res.ProducedAmbiguous != 1 {
		t.Errorf("ambiguous produces = %d, want 1", res.ProducedAmbiguous)
	}
	// An ambiguous produce that is never delivered is not loss either:
	// the broker may never have had it.
	never := buildLog([]op{produce("m2", "orders", 0, 5).with(history.OutcomeAmbiguous)})
	if res := check(never, defaultOpts()); res.Undelivered != 0 {
		t.Errorf("undelivered = %d, want 0 for an ambiguous produce", res.Undelivered)
	}
}

// An ack that errored, and a 410 ack, both leave the client unable to
// say whether the message was acked. Excluding them is what keeps a
// later redelivery from being reported as post-ack.
func TestUndecidableAcksAreExcluded(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		outcome history.Outcome
	}{
		{"errored ack", history.OutcomeAmbiguous},
		{"410 gone", history.OutcomeRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := buildLog([]op{
				produce("m1", "orders", 0, 5),
				deliver("m1", "orders", 10, 12),
				ack("m1", "orders", 15, 17).with(tc.outcome),
				deliver("m1", "orders", 120, 122),
				ack("m1", "orders", 125, 127),
			})
			res := check(log, defaultOpts())
			if res.PostAckTotal != 0 {
				t.Fatalf("post-ack = %d, want 0: the first ack proved nothing", res.PostAckTotal)
			}
			if res.Verdict != verdictOK {
				t.Fatalf("verdict = %s, want OK (%s)", res.Verdict, verdictSentence(res))
			}
			if res.AcksExcluded != 1 {
				t.Errorf("excluded acks = %d, want 1", res.AcksExcluded)
			}
		})
	}
}

// Loss is liveness, not safety: it is reported loudly and exits zero,
// because a backlog the consumers did not drain is not a broken promise.
func TestUndeliveredAndUnackedAreOverdueNotFailure(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5), // accepted, never delivered
		produce("m2", "orders", 1, 6),
		deliver("m2", "orders", 10, 12), // delivered, never acked
	})
	res := check(log, defaultOpts())

	if res.Verdict != verdictOverdue {
		t.Fatalf("verdict = %s, want OVERDUE", res.Verdict)
	}
	if res.Undelivered != 1 || res.Unacked != 1 {
		t.Fatalf("undelivered=%d unacked=%d, want 1/1", res.Undelivered, res.Unacked)
	}
	if res.failed() {
		t.Error("OVERDUE must exit zero: it is a liveness result")
	}
}

// Fan-out makes a message a separate obligation on every path. A replica
// child that never received a message is loss on that child even though
// the parent delivered cleanly, and keying by message alone would miss
// it entirely.
func TestFanOutChildIsCheckedAsItsOwnPath(t *testing.T) {
	t.Parallel()

	log := buildLogWithPaths(map[string][]string{"orders": {"orders", "orders-replica"}}, []op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		produce("m2", "orders", 1, 6),
		deliver("m2", "orders", 20, 22),
		ack("m2", "orders", 25, 27),
		deliver("m2", "orders-replica", 30, 32),
		ack("m2", "orders-replica", 35, 37),
	})
	res := check(log, defaultOpts())

	if res.Partitions != 4 {
		t.Fatalf("partitions = %d, want 4 (2 messages x 2 paths)", res.Partitions)
	}
	// m1 never arrived on the replica.
	if res.Undelivered != 1 {
		t.Fatalf("undelivered = %d, want 1", res.Undelivered)
	}
	if res.Verdict != verdictOverdue {
		t.Fatalf("verdict = %s, want OVERDUE", res.Verdict)
	}
	found := false
	for _, key := range res.UndeliveredSamples {
		if key.Msg == "m1" && key.Path == "orders-replica" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected m1 on orders-replica to be named, got %+v", res.UndeliveredSamples)
	}
}

// Two consumers holding the same message at once is a genuine
// double-lease, and this model cannot see it, because it has no clock.
// The limit is asserted so the report is never read as ruling it out.
func TestOverlappingDeliveriesAreAcceptedByTheUntimedModel(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 20),
		deliver("m1", "orders", 12, 22),
		ack("m1", "orders", 25, 27),
	})
	if res := check(log, defaultOpts()); res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK: the untimed model cannot see a double lease", res.Verdict)
	}
}

func TestSamplesAreCapped(t *testing.T) {
	t.Parallel()

	var ops []op
	for i := range 40 {
		msg := string(rune('a'+i%26)) + string(rune('0'+i/26))
		offset := int64(i) * 10
		ops = append(ops,
			produce(msg, "orders", offset, offset+1),
			deliver(msg, "orders", offset+2, offset+3),
			ack(msg, "orders", offset+4, offset+5),
			deliver(msg, "orders", 10_000+offset, 10_000+offset+1),
		)
	}
	opts := defaultOpts()
	opts.Samples = 5
	res := check(buildLog(ops), opts)

	if res.PostAckUnexplained != 40 {
		t.Fatalf("unexplained = %d, want 40", res.PostAckUnexplained)
	}
	if len(res.Unexplained) != 5 {
		t.Fatalf("samples = %d, want them capped at 5", len(res.Unexplained))
	}
}

func TestReportsMentionTheVerdictAndTheNumbers(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 120, 122),
	})
	res := check(log, defaultOpts())

	var text bytes.Buffer
	writeText(&text, res)
	if !strings.Contains(text.String(), "ANOMALY") {
		t.Errorf("text report should carry the verdict:\n%s", text.String())
	}
	if !strings.Contains(text.String(), "UNEXPLAINED") {
		t.Errorf("text report should list the unexplained events:\n%s", text.String())
	}

	var md bytes.Buffer
	writeMarkdown(&md, res)
	if !strings.Contains(md.String(), "ANOMALY") || !strings.Contains(md.String(), "unexplained") {
		t.Errorf("markdown summary should carry the verdict and the count:\n%s", md.String())
	}
}

// A history with nothing in it linearizes perfectly. Reporting that as
// OK would turn a driver that died on its first request, or a --history
// flag that silently did nothing, into a green nightly run.
func TestEmptyHistoryProvesNothing(t *testing.T) {
	t.Parallel()

	res := check(&history.Log{}, defaultOpts())
	if res.Verdict != verdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN for an empty history", res.Verdict)
	}
	if !res.failed() {
		t.Error("a run that proves nothing must not exit zero")
	}
	if !strings.Contains(verdictSentence(res), "too few") {
		t.Errorf("the sentence should say why, got: %s", verdictSentence(res))
	}
}

func TestBelowTheEvidenceFloorIsUnknown(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
	})
	opts := defaultOpts()
	opts.MinOperations = 1000
	if res := check(log, opts); res.Verdict != verdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN below the evidence floor", res.Verdict)
	}
	// The same history clears a floor it meets.
	opts.MinOperations = 3
	if res := check(log, opts); res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK at the floor", res.Verdict)
	}
}

func TestDefaultGraceFollowsTheVisibilityTimeout(t *testing.T) {
	t.Parallel()

	withMeta := &history.Log{HasMeta: true, Meta: history.Record{VisibilityTimeoutMs: 10_000}}
	if got, want := defaultGrace(withMeta), 10*time.Second+graceMargin; got != want {
		t.Errorf("grace = %s, want %s", got, want)
	}
	if got, want := defaultGrace(&history.Log{}), 30*time.Second+graceMargin; got != want {
		t.Errorf("grace without meta = %s, want %s", got, want)
	}
}

// The verdict sentence is the line a person reads first, so its
// agreement is asserted rather than assumed.
func TestVerdictSentenceAgreesWithItsCounts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		res  *result
		want string
	}{
		{
			"one explained redelivery",
			&result{Verdict: verdictOK, PostAckExplained: 1},
			"Every partition linearized. The one post-ack redelivery happened while a fault was in flight.",
		},
		{
			"several explained redeliveries",
			&result{Verdict: verdictOK, PostAckExplained: 4},
			"Every partition linearized. All 4 post-ack redeliveries happened while a fault was in flight.",
		},
		{
			"nothing to explain",
			&result{Verdict: verdictOK},
			"Every partition linearized and nothing was redelivered after its ack.",
		},
		{
			"one anomaly",
			&result{Verdict: verdictAnomaly, PostAckUnexplained: 1},
			"1 message was redelivered after a confirmed ack with no fault to account for it.",
		},
		{
			"several anomalies",
			&result{Verdict: verdictAnomaly, PostAckUnexplained: 3},
			"3 messages were redelivered after a confirmed ack with no fault to account for it.",
		},
		{
			"one of each overdue",
			&result{Verdict: verdictOverdue, Undelivered: 1, Unacked: 1},
			"No safety problem. 1 message was still undelivered and 1 was still unacked when the run ended.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := verdictSentence(tc.res); got != tc.want {
				t.Errorf("verdictSentence()\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// A message handed to a consumer polling a topic it was never produced
// to is a cross-topic leak. Before the topology was declared rather than
// inferred, the second path defined itself as legal simply by appearing,
// so this read as a clean run.
func TestDeliveryOnAnUndeclaredPathIsAMisroute(t *testing.T) {
	t.Parallel()

	log := buildLogWithPaths(map[string][]string{"orders": {"orders"}}, []op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		// The same message handed out on a topic it never belonged to.
		deliver("m1", "billing", 20, 22),
	})
	res := check(log, defaultOpts())

	if res.Verdict != verdictViolation {
		t.Fatalf("verdict = %s, want VIOLATION", res.Verdict)
	}
	if res.Misrouted != 1 {
		t.Fatalf("misrouted = %d, want 1", res.Misrouted)
	}
	if len(res.MisroutedSamples) != 1 || res.MisroutedSamples[0].Path != "billing" {
		t.Errorf("the leak should be named, got %+v", res.MisroutedSamples)
	}
	if !strings.Contains(verdictSentence(res), "never declared") {
		t.Errorf("the sentence should name the misroute, got: %s", verdictSentence(res))
	}
}

// Without a meta record there is no statement of what correct would have
// been, so paths are inferred and nothing is called misrouted. The
// fallback is asserted so it stays a deliberate, documented weakening
// rather than something that quietly becomes the normal path.
func TestWithoutADeclaredTopologyPathsAreInferred(t *testing.T) {
	t.Parallel()

	log := &history.Log{
		Ops: []history.Record{
			{Op: history.OpProduce, Msg: "m1", Path: "orders", Call: at(0), Ret: at(5), Outcome: history.OutcomeOK},
			{Op: history.OpDeliver, Msg: "m1", Path: "billing", Call: at(10), Ret: at(12), Outcome: history.OutcomeOK},
			{Op: history.OpAck, Msg: "m1", Path: "billing", Call: at(15), Ret: at(17), Outcome: history.OutcomeOK},
		},
	}
	res := check(log, defaultOpts())
	if res.Misrouted != 0 {
		t.Fatalf("misrouted = %d, want 0 without a declared topology", res.Misrouted)
	}
	// This is the weakness, stated exactly: inference takes the leaked
	// path for a fan-out child, so the leak surfaces as an ordinary
	// looking backlog on the real topic instead of a violation. A run
	// that declares its topology reports the same history as a
	// VIOLATION naming the leak.
	if res.Verdict != verdictOverdue {
		t.Fatalf("verdict = %s, want OVERDUE: inference downgrades a leak to a backlog", res.Verdict)
	}
}

// A consume is a long poll. A delivery that began just before a fault
// opened and returned inside it was handed out during that fault, and
// testing only its start instant reported it as an unexplained anomaly.
func TestARedeliveryOverlappingTheFaultWindowIsExplained(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		// Poll runs 900 to 1400; the fault opens at 1000.
		deliver("m1", "orders", 900, 1400),
	}, fault(history.FaultKill, "narad-1", 1000, 1200))

	res := check(log, defaultOpts())
	if res.PostAckExplained != 1 || res.PostAckUnexplained != 0 {
		t.Fatalf("explained=%d unexplained=%d, want 1/0", res.PostAckExplained, res.PostAckUnexplained)
	}
	if res.Verdict != verdictOK {
		t.Fatalf("verdict = %s, want OK (%s)", res.Verdict, verdictSentence(res))
	}
}

// Coverage is what stops "explained by a fault" from quietly meaning
// "happened during the fault phase". Overlapping windows must be merged,
// or the figure exceeds the run.
func TestFaultCoverageMergesOverlappingWindows(t *testing.T) {
	t.Parallel()

	opts := defaultOpts()
	opts.Grace = 0

	// Run spans 0 to 1000ms. Two windows overlapping across 100 to 500.
	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 990, 1000),
	},
		fault(history.FaultKill, "narad-1", 100, 400),
		fault(history.FaultKill, "narad-2", 300, 500),
	)

	res := check(log, opts)
	if got := res.FaultCoverage; got < 0.39 || got > 0.41 {
		t.Fatalf("coverage = %.3f, want ~0.40 (400ms of a 1000ms run, merged)", got)
	}

	var text bytes.Buffer
	writeText(&text, res)
	if !strings.Contains(text.String(), "fault coverage") {
		t.Errorf("the report should state coverage:\n%s", text.String())
	}
}

func TestFaultCoverageIsZeroWithoutFaults(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
	})
	if got := check(log, defaultOpts()).FaultCoverage; got != 0 {
		t.Fatalf("coverage = %.3f, want 0", got)
	}
}

// The injector writes a provisional window when a fault starts and the
// real one when it ends, so a fault cut short still leaves a mark. The
// pair must collapse, or every fault would be counted twice.
func TestProvisionalAndFinalFaultRecordsMerge(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 150, 152),
	},
		fault(history.FaultKill, "narad-1", 100, 100), // provisional
		fault(history.FaultKill, "narad-1", 100, 200), // final
	)

	res := check(log, defaultOpts())
	if res.Faults != 1 {
		t.Fatalf("faults = %d, want 1 after merging the pair", res.Faults)
	}
	if res.FaultsByKind[history.FaultKill] != 1 {
		t.Errorf("faults by kind = %+v, want one kill", res.FaultsByKind)
	}
	if res.PostAckExplained != 1 {
		t.Errorf("explained = %d, want 1", res.PostAckExplained)
	}
}

// A fault the injector never got to finish leaves only the provisional
// record. Its grace window still has to cover the aftermath, or the run
// it was already failing would gain a second, misleading failure.
func TestAProvisionalFaultAloneStillExplains(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		deliver("m1", "orders", 2000, 2002),
	}, fault(history.FaultKill, "narad-1", 100, 100))

	res := check(log, defaultOpts())
	if res.PostAckUnexplained != 0 {
		t.Fatalf("unexplained = %d, want 0: the provisional window plus grace covers it", res.PostAckUnexplained)
	}
	if res.Faults != 1 {
		t.Errorf("faults = %d, want 1", res.Faults)
	}
}

// TestVerdictSentenceAgreesWithItsCounts stops short of VIOLATION and
// UNKNOWN, and each has two distinct reasons the sentence has to tell
// apart, so they get their own table.
func TestVerdictSentenceCoversViolationAndUnknown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		res  *result
		want string
	}{
		{
			"violation from a misroute",
			&result{Verdict: verdictViolation, Misrouted: 1},
			"1 message was delivered on a path the run never declared for its topic.",
		},
		{
			"violation from several misroutes",
			&result{Verdict: verdictViolation, Misrouted: 2},
			"2 messages were delivered on a path the run never declared for its topic.",
		},
		{
			"violation with no misroute is a bare model rejection",
			&result{Verdict: verdictViolation},
			"A partition admits no valid ordering: the broker did something the delivery contract does not allow.",
		},
		{
			"unknown from too few operations",
			&result{Verdict: verdictUnknown, Porcupine: "Ok", Operations: 3},
			"Only 3 operations were recorded, too few for a clean result to mean anything. The run proves nothing either way.",
		},
		{
			"unknown from a porcupine timeout",
			&result{Verdict: verdictUnknown, Porcupine: "Unknown"},
			"The search did not finish within its timeout, so this run proves nothing either way.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := verdictSentence(tc.res); got != tc.want {
				t.Errorf("verdictSentence()\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// The Markdown report leans on badge() to put a glyph next to the
// verdict; every verdict must resolve to one rather than falling through
// to Go's zero value for a string.
func TestBadgeCoversEveryVerdict(t *testing.T) {
	t.Parallel()

	for v, want := range map[verdict]string{
		verdictOK:        "✅",
		verdictOverdue:   "⏳",
		verdictAnomaly:   "❌",
		verdictViolation: "❌",
		verdictUnknown:   "❌",
	} {
		if got := badge(v); got != want {
			t.Errorf("badge(%s) = %q, want %q", v, got, want)
		}
	}
}

// A broker that answers 410 to every ack leaves every message unacked.
// That used to read as OVERDUE and exit zero, which made the worst
// ack-path failure there is indistinguishable from a slow drain.
func TestAnAckPathThatNeverConfirmsIsStalledNotOverdue(t *testing.T) {
	t.Parallel()

	var ops []op
	for i := range 20 {
		msg := fmt.Sprintf("m%02d", i)
		base := int64(i * 10)
		ops = append(ops,
			produce(msg, "orders", base, base+1),
			deliver(msg, "orders", base+2, base+3),
			// The broker answered and said no, so the checker excludes
			// the ack and the message stays unacked.
			ack(msg, "orders", base+4, base+5).with(history.OutcomeRejected),
		)
	}
	opts := defaultOpts()
	opts.MaxOverdue = 0.05

	res := check(buildLog(ops), opts)

	if res.Verdict != verdictStalled {
		t.Fatalf("verdict = %s, want STALLED when nothing was ever acked", res.Verdict)
	}
	if !res.failed() {
		t.Error("a run where no ack was ever confirmed must exit non-zero")
	}
	if res.Unacked != 20 {
		t.Errorf("unacked = %d, want 20", res.Unacked)
	}
}

// The ceiling only trips past the bound. A small tail is still the
// liveness footnote OVERDUE was written for.
func TestASmallBacklogStaysOverdueUnderTheCeiling(t *testing.T) {
	t.Parallel()

	var ops []op
	for i := range 40 {
		msg := fmt.Sprintf("m%02d", i)
		base := int64(i * 10)
		ops = append(ops, produce(msg, "orders", base, base+1), deliver(msg, "orders", base+2, base+3))
		if i > 0 { // one message of forty left unacked: 2.5%
			ops = append(ops, ack(msg, "orders", base+4, base+5))
		}
	}
	opts := defaultOpts()
	opts.MaxOverdue = 0.05

	res := check(buildLog(ops), opts)

	if res.Verdict != verdictOverdue {
		t.Fatalf("verdict = %s, want OVERDUE for a 2.5%% tail under a 5%% ceiling", res.Verdict)
	}
	if res.failed() {
		t.Error("a backlog under the ceiling must not fail the run")
	}
}

// Zero means unbounded, so a caller that states no ceiling keeps the
// reading it had before the ceiling existed.
func TestWithoutACeilingAnyBacklogIsStillOverdue(t *testing.T) {
	t.Parallel()

	log := buildLog([]op{
		produce("m1", "orders", 0, 5),
		produce("m2", "orders", 1, 6),
		deliver("m2", "orders", 10, 12),
	})
	opts := defaultOpts() // MaxOverdue unset
	if res := check(log, opts); res.Verdict != verdictOverdue {
		t.Fatalf("verdict = %s, want OVERDUE with no ceiling set", res.Verdict)
	}
}

// Coverage was computed and reported as the number that bounds what a
// clean result is worth, and then never consulted. Saturate it and the
// answer has to be "this run decided nothing", not "OK".
func TestSaturatedFaultCoverageIsUndecidedNotClean(t *testing.T) {
	t.Parallel()

	ops := []op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
	}
	// One fault spanning the whole run, so every moment sits inside a
	// window and its grace.
	log := buildLog(ops, fault(history.FaultKill, "narad-1", 0, 100))
	opts := defaultOpts()
	opts.MaxFaultCoverage = 0.75

	res := check(log, opts)

	if res.Verdict != verdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN at saturated coverage", res.Verdict)
	}
	if res.UndecidedBecause != undecidedCoverage {
		t.Errorf("undecided because %q, want %q", res.UndecidedBecause, undecidedCoverage)
	}
	if !res.failed() {
		t.Error("an undecided run must exit non-zero")
	}
	if got := verdictSentence(res); !strings.Contains(got, "explained by construction") {
		t.Errorf("sentence = %q, want it to say why the run decided nothing", got)
	}
}

// An unexplained redelivery is a real finding whatever the coverage was,
// so the anomaly has to outrank the coverage ceiling.
func TestAnUnexplainedRedeliveryOutranksTheCoverageCeiling(t *testing.T) {
	t.Parallel()

	ops := []op{
		produce("m1", "orders", 0, 5),
		deliver("m1", "orders", 10, 12),
		ack("m1", "orders", 15, 17),
		// Past the fault's end at 100ms plus the 5s grace.
		deliver("m1", "orders", 8000, 8002),
	}
	log := buildLog(ops, fault(history.FaultKill, "narad-1", 0, 100))
	opts := defaultOpts()
	opts.MaxFaultCoverage = 0.1 // deliberately trippable

	res := check(log, opts)

	if res.Verdict != verdictAnomaly {
		t.Fatalf("verdict = %s, want ANOMALY: the redelivery is a finding regardless of coverage", res.Verdict)
	}
}

// The overdue fraction counts partitions, not message ids. Under fan-out
// one message is several partitions, so dividing by the message count
// could exceed 1 and made the ceiling fire at half its stated value.
func TestOverdueFractionIsPerPartitionUnderFanOut(t *testing.T) {
	t.Parallel()

	paths := map[string][]string{"orders": {"orders", "orders-replica"}}
	var ops []op
	for i := range 10 {
		msg := fmt.Sprintf("m%02d", i)
		base := int64(i * 10)
		ops = append(ops, produce(msg, "orders", base, base+1))
		for _, path := range []string{"orders", "orders-replica"} {
			ops = append(ops,
				deliver(msg, path, base+2, base+3),
				ack(msg, path, base+4, base+5),
			)
		}
	}
	res := check(buildLogWithPaths(paths, ops), defaultOpts())

	if res.Partitions != 20 {
		t.Fatalf("partitions = %d, want 20 (10 messages over 2 paths)", res.Partitions)
	}
	if res.Messages != 10 {
		t.Fatalf("messages = %d, want 10", res.Messages)
	}
	if got := overdueFraction(res); got != 0 {
		t.Fatalf("overdue fraction = %v, want 0 on a complete run", got)
	}

	// Now strand one whole message: two partitions of twenty, which is
	// 10% of partitions and would read as 20% against the message count.
	stranded := ops[:len(ops)-4]
	res = check(buildLogWithPaths(paths, stranded), defaultOpts())
	if got := overdueFraction(res); got < 0.09 || got > 0.11 {
		t.Fatalf("overdue fraction = %v, want ~0.10 of partitions", got)
	}
	if got := overdueFraction(res); got > 1 {
		t.Fatalf("overdue fraction = %v, must never exceed 1", got)
	}
}
