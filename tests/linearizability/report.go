package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

// writeText prints the human-readable verdict.
//
// The shape is deliberate: the verdict and the one sentence that
// justifies it come first, because that is all most readers of a green
// nightly run will read. Everything after it is for the run that was not
// green.
func writeText(w io.Writer, res *result) {
	fmt.Fprintf(w, "%s\n", strings.Repeat("=", 68))
	fmt.Fprintf(w, "VERDICT: %s\n", res.Verdict)
	fmt.Fprintf(w, "%s\n", verdictSentence(res))
	fmt.Fprintf(w, "%s\n\n", strings.Repeat("=", 68))

	mode := "contract (at-least-once: post-ack redelivery must be explained by a fault)"
	if res.Strict {
		mode = "strict (no post-ack redelivery permitted at all)"
	}
	fmt.Fprintf(w, "model            %s\n", mode)
	fmt.Fprintf(w, "linearizability  %s over %d partitions\n", res.Porcupine, res.Partitions)
	if res.IllegalPartition != "" {
		fmt.Fprintf(w, "  first illegal  %s\n", res.IllegalPartition)
	}
	if res.RunEnd > res.RunStart {
		fmt.Fprintf(w, "window           %s to %s (%s)\n",
			stamp(res.RunStart), stamp(res.RunEnd),
			time.Duration(res.RunEnd-res.RunStart).Round(time.Second))
	}
	fmt.Fprintf(w, "faults           %d%s, grace %s\n", res.Faults, faultBreakdown(res), res.Grace)
	if res.Faults > 0 {
		// Where coverage is high, "explained by a fault" means little
		// more than "happened during the fault phase". Saying so is the
		// difference between a check and a rubber stamp.
		fmt.Fprintf(w, "fault coverage   %.0f%% of the run sat inside a fault window; the check discriminates in the rest\n",
			res.FaultCoverage*100)
	}
	fmt.Fprintln(w)

	fmt.Fprintf(w, "messages         %d over %d (message, path) partitions\n", res.Messages, res.Partitions)
	fmt.Fprintf(w, "operations       %d checked\n", res.Operations)
	fmt.Fprintf(w, "  produced       %d accepted, %d ambiguous, %d refused\n",
		res.ProducedOK, res.ProducedAmbiguous, res.ProducedRejected)
	fmt.Fprintf(w, "  delivered      %d\n", res.Deliveries)
	fmt.Fprintf(w, "  acked          %d confirmed, %d excluded as undecidable\n", res.AcksOK, res.AcksExcluded)
	fmt.Fprintln(w)

	fmt.Fprintf(w, "post-ack redeliveries  %d total: %d explained by a fault, %d unexplained\n",
		res.PostAckTotal, res.PostAckExplained, res.PostAckUnexplained)
	if len(res.Unexplained) > 0 {
		fmt.Fprintf(w, "\n  UNEXPLAINED (no fault was in flight, and none had been within the grace window):\n")
		for _, event := range res.Unexplained {
			fmt.Fprintf(w, "    %s\n      acked at %s, redelivered %s later at %s\n",
				event.Key, stamp(event.AckReturned), event.Gap.Round(time.Millisecond), stamp(event.Delivered))
		}
		if res.PostAckUnexplained > len(res.Unexplained) {
			fmt.Fprintf(w, "    ... and %d more\n", res.PostAckUnexplained-len(res.Unexplained))
		}
	}
	if len(res.PostAckSamples) > 0 {
		fmt.Fprintf(w, "\n  explained, sample:\n")
		for _, event := range res.PostAckSamples {
			fmt.Fprintf(w, "    %s  %s after ack, during %s\n",
				event.Key, event.Gap.Round(time.Millisecond), event.Fault)
		}
	}
	fmt.Fprintln(w)

	if res.Misrouted > 0 {
		fmt.Fprintf(w, "MISROUTED        %d delivery path(s) the run never declared for the message's topic\n", res.Misrouted)
		writeSamples(w, "  misrouted", res.MisroutedSamples, res.Misrouted)
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "liveness         %d accepted but never delivered, %d delivered but never acked\n",
		res.Undelivered, res.Unacked)
	writeSamples(w, "  undelivered", res.UndeliveredSamples, res.Undelivered)
	writeSamples(w, "  unacked", res.UnackedSamples, res.Unacked)

	if res.Truncated > 0 || res.Skewed > 0 {
		fmt.Fprintln(w)
		if res.Truncated > 0 {
			fmt.Fprintf(w, "note             %d truncated line(s) skipped; a run killed mid-write leaves one\n", res.Truncated)
		}
		if res.Skewed > 0 {
			fmt.Fprintf(w, "note             %d record(s) returned before they were called; the clock moved under the run\n", res.Skewed)
		}
	}
}

func writeSamples(w io.Writer, label string, samples []partitionKey, total int) {
	if len(samples) == 0 {
		return
	}
	names := make([]string, 0, len(samples))
	for _, key := range samples {
		names = append(names, key.String())
	}
	fmt.Fprintf(w, "%s: %s", label, strings.Join(names, ", "))
	if total > len(samples) {
		fmt.Fprintf(w, ", and %d more", total-len(samples))
	}
	fmt.Fprintln(w)
}

// verdictSentence states why, in one line, without making the reader
// cross-reference the counts below it.
func verdictSentence(res *result) string {
	switch res.Verdict {
	case verdictOK:
		if res.PostAckExplained == 1 {
			return "Every partition linearized. The one post-ack redelivery happened while a fault was in flight."
		}
		if res.PostAckExplained > 1 {
			return fmt.Sprintf("Every partition linearized. All %d post-ack redeliveries happened while a fault was in flight.",
				res.PostAckExplained)
		}
		return "Every partition linearized and nothing was redelivered after its ack."
	case verdictOverdue:
		return fmt.Sprintf("No safety problem. %s undelivered and %s unacked when the run ended.",
			count(res.Undelivered, "message was still", "messages were still"),
			count(res.Unacked, "was still", "were still"))
	case verdictAnomaly:
		return fmt.Sprintf("%s redelivered after a confirmed ack with no fault to account for it.",
			count(res.PostAckUnexplained, "message was", "messages were"))
	case verdictViolation:
		if res.Misrouted > 0 {
			return count(res.Misrouted, "message was delivered", "messages were delivered") +
				" on a path the run never declared for its topic."
		}
		return "A partition admits no valid ordering: the broker did something the delivery contract does not allow."
	case verdictUnknown:
		if res.Porcupine != string(porcupine.Unknown) {
			return fmt.Sprintf("Only %d operations were recorded, too few for a clean result to mean anything. The run proves nothing either way.",
				res.Operations)
		}
		return "The search did not finish within its timeout, so this run proves nothing either way."
	default:
		return ""
	}
}

// count renders "1 message was" and "3 messages were" without the
// "message(s)" wart, because these lines are the part of the report a
// person actually reads.
func count(n int, singular, plural string) string {
	if n == 1 {
		return "1 " + singular
	}
	return fmt.Sprintf("%d %s", n, plural)
}

func faultBreakdown(res *result) string {
	if len(res.FaultsByKind) == 0 {
		return ""
	}
	kinds := make([]string, 0, len(res.FaultsByKind))
	for kind := range res.FaultsByKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%d %s", res.FaultsByKind[kind], kind))
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// writeMarkdown renders the verdict for a CI job summary.
func writeMarkdown(w io.Writer, res *result) {
	fmt.Fprintf(w, "### Linearizability: %s %s\n\n", badge(res.Verdict), res.Verdict)
	fmt.Fprintf(w, "%s\n\n", verdictSentence(res))

	fmt.Fprintln(w, "| | |")
	fmt.Fprintln(w, "|---|---|")
	fmt.Fprintf(w, "| Model | %s |\n", map[bool]string{true: "strict", false: "contract"}[res.Strict])
	fmt.Fprintf(w, "| Linearizability | `%s` over %d partitions |\n", res.Porcupine, res.Partitions)
	fmt.Fprintf(w, "| Messages | %d |\n", res.Messages)
	fmt.Fprintf(w, "| Operations | %d |\n", res.Operations)
	fmt.Fprintf(w, "| Produced | %d accepted, %d ambiguous, %d refused |\n", res.ProducedOK, res.ProducedAmbiguous, res.ProducedRejected)
	fmt.Fprintf(w, "| Delivered | %d |\n", res.Deliveries)
	fmt.Fprintf(w, "| Acked | %d confirmed |\n", res.AcksOK)
	fmt.Fprintf(w, "| Faults injected | %d%s |\n", res.Faults, faultBreakdown(res))
	if res.Faults > 0 {
		fmt.Fprintf(w, "| Fault coverage | %.0f%% of the run |\n", res.FaultCoverage*100)
	}
	if res.Misrouted > 0 {
		fmt.Fprintf(w, "| Misrouted | **%d** |\n", res.Misrouted)
	}
	fmt.Fprintf(w, "| Post-ack redeliveries | %d explained, **%d unexplained** |\n", res.PostAckExplained, res.PostAckUnexplained)
	fmt.Fprintf(w, "| Liveness | %d undelivered, %d unacked |\n", res.Undelivered, res.Unacked)
	if res.RunEnd > res.RunStart {
		fmt.Fprintf(w, "| Window | %s |\n", time.Duration(res.RunEnd-res.RunStart).Round(time.Second))
	}

	if len(res.Unexplained) > 0 {
		fmt.Fprintf(w, "\n#### Unexplained\n\n")
		for _, event := range res.Unexplained {
			fmt.Fprintf(w, "- `%s` acked at %s, redelivered %s later\n",
				event.Key, stamp(event.AckReturned), event.Gap.Round(time.Millisecond))
		}
	}
	if res.IllegalPartition != "" {
		fmt.Fprintf(w, "\n#### First illegal partition\n\n```\n%s\n```\n", res.IllegalPartition)
	}
}

func badge(v verdict) string {
	switch v {
	case verdictOK:
		return "✅"
	case verdictOverdue:
		return "⏳"
	default:
		return "❌"
	}
}

// writeJSON writes the machine-readable result, for trend tracking
// across nightly runs.
func writeJSON(path string, res *result) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(res); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func stamp(ns int64) string {
	return time.Unix(0, ns).UTC().Format("15:04:05.000")
}
