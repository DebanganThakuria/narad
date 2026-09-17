package main

import (
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/debanganthakuria/narad/tests/linearizability/history"
)

// verdict is the run's outcome. OK and OVERDUE exit 0; the rest exit 1.
type verdict string

const (
	// verdictOK means every partition linearized and nothing was
	// unexplained.
	verdictOK verdict = "OK"
	// verdictOverdue means no safety problem, but messages were still
	// undelivered or unacked when the run ended. That is a liveness
	// result: a backlog the consumers did not drain, not a broken
	// promise, so it does not fail the run on its own.
	verdictOverdue verdict = "OVERDUE"
	// verdictAnomaly means a message was provably redelivered after its
	// ack at a moment when no fault was in flight. The delivery contract
	// permits redelivery, so this is not a contract violation; it is an
	// event with no accounted-for cause, which is the thing worth
	// failing on.
	verdictAnomaly verdict = "ANOMALY"
	// verdictViolation means some partition admits no valid ordering at
	// all under the model.
	verdictViolation verdict = "VIOLATION"
	// verdictUnknown means the checker could not decide within its
	// timeout. Treated as a failure: an undecided gate that reports
	// success is worse than no gate.
	verdictUnknown verdict = "UNKNOWN"
)

// checkOptions are the knobs the CLI exposes.
type checkOptions struct {
	// Strict makes any post-ack redelivery a linearizability violation
	// rather than an anomaly to attribute.
	Strict bool
	// Grace extends every fault window forwards. A broker that restarts
	// redelivers what it still holds leased only when those leases
	// expire, so the effect of a fault outlasts the fault by up to a
	// visibility timeout.
	Grace time.Duration
	// Timeout bounds the porcupine search.
	Timeout time.Duration
	// Samples caps how many examples of each finding the result carries.
	Samples int
	// Visualize asks for the linearization detail a visualization needs.
	// It is off by default: verbose mode retains every partial
	// linearization and disables porcupine's early abort on the first
	// illegal partition, which costs both memory and time on a run that
	// is already failing.
	Visualize bool
	// MinOperations is the fewest operations a run must contain before a
	// clean result means anything. A history with nothing in it
	// linearizes perfectly, and reporting that as OK would turn a driver
	// that died on its first request into a green nightly.
	MinOperations int
}

// postAckRedelivery is a delivery that began strictly after an ack for
// the same message returned 204.
//
// "Strictly after" is what makes it provable rather than suspected: the
// ack returned before the delivery was even requested, so every possible
// ordering of the two puts the ack first. A delivery that merely
// overlaps the ack is not counted, because an ordering exists where the
// broker handed the message out before the ack landed. That gap is the
// honest limit of this analysis.
type postAckRedelivery struct {
	Key         partitionKey `json:"key"`
	AckReturned int64        `json:"ack_returned"`
	Delivered   int64        `json:"delivered"`
	// Gap is how long after the ack the redelivery began.
	Gap time.Duration `json:"gap"`
	// Fault names the fault window that explains it, empty if none does.
	Fault string `json:"fault,omitempty"`
}

// faultWindow is a fault plus the grace period during which its
// after-effects are still attributable to it.
type faultWindow struct {
	Kind   string
	Target string
	Start  int64
	// End is the fault's end plus the grace period.
	End int64
}

// result is everything the report needs.
type result struct {
	Verdict verdict `json:"verdict"`

	Partitions int `json:"partitions"`
	Messages   int `json:"messages"`
	Operations int `json:"operations"`

	ProducedOK        int `json:"produced_ok"`
	ProducedAmbiguous int `json:"produced_ambiguous"`
	ProducedRejected  int `json:"produced_rejected"`
	Deliveries        int `json:"deliveries"`
	AcksOK            int `json:"acks_ok"`
	AcksExcluded      int `json:"acks_excluded"`

	PostAckTotal       int                 `json:"post_ack_total"`
	PostAckExplained   int                 `json:"post_ack_explained"`
	PostAckUnexplained int                 `json:"post_ack_unexplained"`
	PostAckSamples     []postAckRedelivery `json:"post_ack_samples,omitempty"`
	Unexplained        []postAckRedelivery `json:"unexplained,omitempty"`

	Undelivered        int            `json:"undelivered"`
	UndeliveredSamples []partitionKey `json:"undelivered_samples,omitempty"`
	Unacked            int            `json:"unacked"`
	UnackedSamples     []partitionKey `json:"unacked_samples,omitempty"`

	Faults       int            `json:"faults"`
	FaultsByKind map[string]int `json:"faults_by_kind,omitempty"`
	// FaultCoverage is the fraction of the run's wall time covered by a
	// fault window including its grace period. It is reported because it
	// bounds what a clean result is worth: where coverage approaches
	// 100%, every redelivery is "explained" by construction, and the
	// discriminating power of the check lives in the uncovered time.
	FaultCoverage float64 `json:"fault_coverage"`

	// Misrouted counts deliveries on a path the run never declared for
	// that message's topic.
	Misrouted        int            `json:"misrouted"`
	MisroutedSamples []partitionKey `json:"misrouted_samples,omitempty"`

	Porcupine string `json:"porcupine"`
	// IllegalPartition names a partition with no valid ordering, when
	// porcupine found one.
	IllegalPartition string `json:"illegal_partition,omitempty"`

	Strict    bool          `json:"strict"`
	Grace     time.Duration `json:"grace"`
	RunStart  int64         `json:"run_start"`
	RunEnd    int64         `json:"run_end"`
	Truncated int           `json:"truncated_lines"`
	Skewed    int           `json:"skewed_records"`

	// info carries the porcupine linearization for visualization. Not
	// serialized.
	info    porcupine.LinearizationInfo `json:"-"`
	hasInfo bool                        `json:"-"`
	model   porcupine.Model             `json:"-"`
}

// failed reports whether the verdict should exit non-zero.
func (r *result) failed() bool {
	switch r.Verdict {
	case verdictOK, verdictOverdue:
		return false
	default:
		return true
	}
}

// check runs the whole analysis over a parsed history.
func check(log *history.Log, opts checkOptions) *result {
	res := &result{
		Strict:    opts.Strict,
		Grace:     opts.Grace,
		Truncated: log.Truncated,
		Skewed:    log.Skewed,
	}
	if len(log.Ops) > 0 {
		res.RunStart = log.Ops[0].Call
		res.RunEnd = log.Ops[0].Ret
		for _, rec := range log.Ops {
			if rec.Ret > res.RunEnd {
				res.RunEnd = rec.Ret
			}
		}
	}

	paths := discoverPaths(log)
	ops, stats := buildOperations(log.Ops, paths)
	res.ProducedOK = stats.producedOK
	res.ProducedAmbiguous = stats.producedAmbiguous
	res.ProducedRejected = stats.producedRejected
	res.Deliveries = stats.deliveries
	res.AcksOK = stats.acksOK
	res.AcksExcluded = stats.acksExcluded
	res.Messages = stats.messages
	res.Operations = len(ops)

	windows := buildFaultWindows(log.Faults, opts.Grace)
	res.Faults = len(windows)
	res.FaultsByKind = map[string]int{}
	for _, w := range windows {
		res.FaultsByKind[w.Kind]++
	}
	res.FaultCoverage = coverage(windows, res.RunStart, res.RunEnd)

	byKey := groupByKey(log.Ops, paths)
	res.Partitions = len(byKey)

	analysePostAck(byKey, windows, opts, res)
	analyseLoss(byKey, opts, res)
	analyseMisroutes(byKey, opts, res)

	res.model = newModel(opts.Strict)
	switch {
	case len(ops) == 0:
		res.Porcupine = "Ok"
	case opts.Visualize:
		checkResult, info := porcupine.CheckOperationsVerbose(res.model, ops, opts.Timeout)
		res.Porcupine = string(checkResult)
		res.info = info
		res.hasInfo = true
	default:
		res.Porcupine = string(porcupine.CheckOperationsTimeout(res.model, ops, opts.Timeout))
	}
	if res.Porcupine == string(porcupine.Illegal) {
		res.IllegalPartition = describeIllegal(byKey, opts)
	}

	res.Verdict = decide(res, opts)
	return res
}

// decide turns the findings into a verdict, worst first.
func decide(res *result, opts checkOptions) verdict {
	switch res.Porcupine {
	case string(porcupine.Illegal):
		return verdictViolation
	case string(porcupine.Unknown):
		return verdictUnknown
	}
	// Checked before the clean paths below, so "we proved nothing"
	// never renders as "nothing was wrong".
	if res.Operations < opts.MinOperations {
		return verdictUnknown
	}
	if res.PostAckUnexplained > 0 {
		return verdictAnomaly
	}
	if res.Undelivered > 0 || res.Unacked > 0 {
		return verdictOverdue
	}
	return verdictOK
}

// pathSet answers, for a message, which paths it must be delivered on.
//
// The declared topology from the run's meta record is authoritative when
// there is one. Inferring paths from observed deliveries instead is
// unsound in a way that is easy to miss: a delivery on a path the
// message was never produced to would define that path as legal simply
// by happening, so a broker leaking messages between topics would read
// as a clean run, and the post-ack analysis would be split across two
// partitions that never compare against each other.
//
// Without a meta record the checker falls back to inference and counts
// nothing as misrouted, because it has no statement of what correct
// would have been.
type pathSet struct {
	msgTopic  map[string]string
	topicPath map[string]map[string]bool
	// declared is true when the topology came from the run rather than
	// from what the run happened to do.
	declared bool
}

func discoverPaths(log *history.Log) *pathSet {
	ps := &pathSet{
		msgTopic:  make(map[string]string),
		topicPath: make(map[string]map[string]bool),
	}
	for _, rec := range log.Ops {
		if rec.Op == history.OpProduce {
			ps.msgTopic[rec.Msg] = rec.Path
		}
	}

	if log.HasMeta && len(log.Meta.Paths) > 0 {
		ps.declared = true
		for topic, paths := range log.Meta.Paths {
			ps.addPath(topic, topic)
			for _, path := range paths {
				ps.addPath(topic, path)
			}
		}
		return ps
	}

	for _, rec := range log.Ops {
		switch rec.Op {
		case history.OpProduce:
			ps.addPath(rec.Path, rec.Path)
		case history.OpDeliver:
			if topic, ok := ps.msgTopic[rec.Msg]; ok {
				ps.addPath(topic, rec.Path)
			}
		}
	}
	return ps
}

func (ps *pathSet) addPath(topic, path string) {
	set := ps.topicPath[topic]
	if set == nil {
		set = make(map[string]bool)
		ps.topicPath[topic] = set
	}
	set[path] = true
}

// pathsFor returns the sorted paths a message must be delivered on.
func (ps *pathSet) pathsFor(msg string) []string {
	topic, ok := ps.msgTopic[msg]
	if !ok {
		return nil
	}
	set := ps.topicPath[topic]
	paths := make([]string, 0, len(set))
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// expected reports whether a delivery on this path is one the run
// declared. False means the broker handed a message to a consumer
// polling a topic it was never produced to.
func (ps *pathSet) expected(msg, path string) bool {
	if !ps.declared {
		return true
	}
	topic, ok := ps.msgTopic[msg]
	if !ok {
		// No produce for it at all. The model rejects that on its own.
		return true
	}
	return ps.topicPath[topic][path]
}

type buildStats struct {
	producedOK        int
	producedAmbiguous int
	producedRejected  int
	deliveries        int
	acksOK            int
	acksExcluded      int
	messages          int
}

// buildOperations turns history records into porcupine operations.
//
// A produce is fanned out into one operation per path, because the
// obligation it creates is per path: a message produced to a topic with
// a replica child must arrive on both, and a checker that only modelled
// the parent would call a child that never delivered anything a clean
// run.
func buildOperations(ops []history.Record, paths *pathSet) ([]porcupine.Operation, buildStats) {
	var stats buildStats
	seenMsg := make(map[string]bool)
	out := make([]porcupine.Operation, 0, len(ops))

	for _, rec := range ops {
		if rec.Msg != "" && !seenMsg[rec.Msg] {
			seenMsg[rec.Msg] = true
			stats.messages++
		}
		switch rec.Op {
		case history.OpProduce:
			var kind opKind
			switch rec.Outcome {
			case history.OutcomeOK:
				kind = kindProduce
				stats.producedOK++
			case history.OutcomeAmbiguous:
				kind = kindProduceAmbiguous
				stats.producedAmbiguous++
			default:
				// The broker answered and refused it. It does not hold the
				// message, so no operation is emitted: a later delivery of
				// this id then has nothing to linearize against and fails,
				// which is the correct outcome.
				stats.producedRejected++
				continue
			}
			for _, path := range paths.pathsFor(rec.Msg) {
				out = append(out, porcupine.Operation{
					ClientId: rec.Client,
					Input:    modelInput{Key: partitionKey{Msg: rec.Msg, Path: path}, Kind: kind},
					Call:     rec.Call,
					Return:   rec.Ret,
				})
			}

		case history.OpDeliver:
			stats.deliveries++
			out = append(out, porcupine.Operation{
				ClientId: rec.Client,
				Input:    modelInput{Key: partitionKey{Msg: rec.Msg, Path: rec.Path}, Kind: kindDeliver},
				Call:     rec.Call,
				Return:   rec.Ret,
			})

		case history.OpAck:
			if rec.Outcome != history.OutcomeOK {
				// A 410 cannot be told apart from an ack whose response was
				// lost, and an errored ack may or may not have landed.
				// Excluded rather than guessed at.
				stats.acksExcluded++
				continue
			}
			stats.acksOK++
			out = append(out, porcupine.Operation{
				ClientId: rec.Client,
				Input:    modelInput{Key: partitionKey{Msg: rec.Msg, Path: rec.Path}, Kind: kindAck},
				Call:     rec.Call,
				Return:   rec.Ret,
			})
		}
	}
	return out, stats
}

// keyOps is every record for one (message, path) partition.
type keyOps struct {
	produceOK        bool
	produceAmbiguous bool
	deliveries       []history.Record
	acksOK           []history.Record
	// misrouted marks a partition that exists only because a message was
	// delivered on a path the run never declared for its topic.
	misrouted bool
}

func groupByKey(ops []history.Record, paths *pathSet) map[partitionKey]*keyOps {
	byKey := make(map[partitionKey]*keyOps)
	get := func(key partitionKey) *keyOps {
		entry := byKey[key]
		if entry == nil {
			entry = &keyOps{}
			byKey[key] = entry
		}
		return entry
	}
	for _, rec := range ops {
		switch rec.Op {
		case history.OpProduce:
			if rec.Outcome == history.OutcomeRejected {
				continue
			}
			for _, path := range paths.pathsFor(rec.Msg) {
				entry := get(partitionKey{Msg: rec.Msg, Path: path})
				if rec.Outcome == history.OutcomeOK {
					entry.produceOK = true
				} else {
					entry.produceAmbiguous = true
				}
			}
		case history.OpDeliver:
			entry := get(partitionKey{Msg: rec.Msg, Path: rec.Path})
			entry.deliveries = append(entry.deliveries, rec)
			if !paths.expected(rec.Msg, rec.Path) {
				entry.misrouted = true
			}
		case history.OpAck:
			if rec.Outcome != history.OutcomeOK {
				continue
			}
			entry := get(partitionKey{Msg: rec.Msg, Path: rec.Path})
			entry.acksOK = append(entry.acksOK, rec)
		}
	}
	return byKey
}

// buildFaultWindows turns fault records into windows, merging the pair
// the injector writes for each fault.
//
// The injector records a zero-width window when a fault begins and the
// real one when it ends, so that a fault interrupted part way through
// still leaves a mark. Both carry the same kind, target and start, so
// they collapse to the wider of the two and the fault is counted once.
func buildFaultWindows(faults []history.Record, grace time.Duration) []faultWindow {
	type faultID struct {
		kind, target string
		start        int64
	}
	windows := make([]faultWindow, 0, len(faults))
	index := make(map[faultID]int, len(faults))
	for _, rec := range faults {
		id := faultID{kind: rec.Kind, target: rec.Target, start: rec.Call}
		end := rec.Ret + int64(grace)
		if at, seen := index[id]; seen {
			windows[at].End = max(windows[at].End, end)
			continue
		}
		index[id] = len(windows)
		windows = append(windows, faultWindow{
			Kind:   rec.Kind,
			Target: rec.Target,
			Start:  rec.Call,
			End:    end,
		})
	}
	return windows
}

// analysePostAck finds deliveries that provably began after an ack
// returned, and attributes each to a fault window or to nothing.
func analysePostAck(byKey map[partitionKey]*keyOps, windows []faultWindow, opts checkOptions, res *result) {
	keys := sortedKeys(byKey)
	for _, key := range keys {
		entry := byKey[key]
		if len(entry.acksOK) == 0 || len(entry.deliveries) == 0 {
			continue
		}
		earliestAck := entry.acksOK[0].Ret
		for _, ack := range entry.acksOK[1:] {
			if ack.Ret < earliestAck {
				earliestAck = ack.Ret
			}
		}
		for _, delivery := range entry.deliveries {
			if delivery.Call <= earliestAck {
				continue
			}
			event := postAckRedelivery{
				Key:         key,
				AckReturned: earliestAck,
				Delivered:   delivery.Call,
				Gap:         time.Duration(delivery.Call - earliestAck),
			}
			if window, ok := explain(delivery, windows); ok {
				event.Fault = window.Kind + " " + window.Target
				res.PostAckExplained++
				if len(res.PostAckSamples) < opts.Samples {
					res.PostAckSamples = append(res.PostAckSamples, event)
				}
			} else {
				res.PostAckUnexplained++
				if len(res.Unexplained) < opts.Samples {
					res.Unexplained = append(res.Unexplained, event)
				}
			}
			res.PostAckTotal++
		}
	}
}

// explain finds a fault window overlapping the interval a redelivery was
// in flight for.
//
// The whole interval, not its start: a consume is a long poll, so a
// delivery that began a moment before a fault opened and returned inside
// it was handed out during the fault. Testing only the start instant
// reported those as unexplained anomalies.
func explain(delivery history.Record, windows []faultWindow) (faultWindow, bool) {
	for _, window := range windows {
		if delivery.Ret >= window.Start && delivery.Call <= window.End {
			return window, true
		}
	}
	return faultWindow{}, false
}

// coverage reports the fraction of [start, end] covered by at least one
// fault window, merging overlaps.
func coverage(windows []faultWindow, start, end int64) float64 {
	if end <= start || len(windows) == 0 {
		return 0
	}
	spans := make([][2]int64, 0, len(windows))
	for _, window := range windows {
		from, to := max(window.Start, start), min(window.End, end)
		if to > from {
			spans = append(spans, [2]int64{from, to})
		}
	}
	if len(spans) == 0 {
		return 0
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i][0] < spans[j][0] })
	var covered int64
	current := spans[0]
	for _, span := range spans[1:] {
		if span[0] > current[1] {
			covered += current[1] - current[0]
			current = span
			continue
		}
		current[1] = max(current[1], span[1])
	}
	covered += current[1] - current[0]
	return float64(covered) / float64(end-start)
}

// analyseLoss counts messages the broker accepted but never delivered,
// and messages delivered but never acked.
//
// Both are liveness findings. A message still sitting in a partition
// when the run ended was not lost, it was late, and the difference
// matters enough that they do not fail the run on their own.
func analyseLoss(byKey map[partitionKey]*keyOps, opts checkOptions, res *result) {
	for _, key := range sortedKeys(byKey) {
		entry := byKey[key]
		switch {
		case entry.produceOK && len(entry.deliveries) == 0:
			res.Undelivered++
			if len(res.UndeliveredSamples) < opts.Samples {
				res.UndeliveredSamples = append(res.UndeliveredSamples, key)
			}
		case len(entry.deliveries) > 0 && len(entry.acksOK) == 0:
			res.Unacked++
			if len(res.UnackedSamples) < opts.Samples {
				res.UnackedSamples = append(res.UnackedSamples, key)
			}
		}
		// A partition whose only produce was ambiguous and which was never
		// delivered is not counted: the broker may never have had it.
	}
}

// describeIllegal replays the model over each partition in real-time
// order to name one that cannot linearize. Porcupine reports that the
// history as a whole is illegal; this narrows it to a partition and the
// operation that broke, which is what a person needs to start reading
// logs.
//
// Real-time order is a sound witness here because it is itself a valid
// linearization candidate: if it fails and porcupine also says illegal,
// this partition is a genuine culprit. When no partition fails in
// real-time order the reason is a subtler reordering, and the empty
// answer says so rather than inventing one.
func describeIllegal(byKey map[partitionKey]*keyOps, opts checkOptions) string {
	for _, key := range sortedKeys(byKey) {
		entry := byKey[key]
		records := make([]history.Record, 0, len(entry.deliveries)+len(entry.acksOK))
		records = append(records, entry.deliveries...)
		records = append(records, entry.acksOK...)
		sort.SliceStable(records, func(i, j int) bool { return records[i].Call < records[j].Call })

		state := stateAbsent
		if entry.produceOK || entry.produceAmbiguous {
			state = statePending
		}
		for _, rec := range records {
			kind := kindDeliver
			if rec.Op == history.OpAck {
				kind = kindAck
			}
			ok, next := step(state, kind, opts.Strict)
			if !ok {
				return key.String() + ": " + kind.String() + " from " + state.String()
			}
			state = next.(leaseState)
		}
	}
	return ""
}

// analyseMisroutes counts deliveries on a path the run never declared.
//
// This is a separate finding rather than only a model rejection because
// it names the specific cross-topic leak, which the porcupine verdict
// alone would not.
func analyseMisroutes(byKey map[partitionKey]*keyOps, opts checkOptions, res *result) {
	for _, key := range sortedKeys(byKey) {
		if !byKey[key].misrouted {
			continue
		}
		res.Misrouted++
		if len(res.MisroutedSamples) < opts.Samples {
			res.MisroutedSamples = append(res.MisroutedSamples, key)
		}
	}
}

func sortedKeys(byKey map[partitionKey]*keyOps) []partitionKey {
	keys := make([]partitionKey, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Msg != keys[j].Msg {
			return keys[i].Msg < keys[j].Msg
		}
		return keys[i].Path < keys[j].Path
	})
	return keys
}
