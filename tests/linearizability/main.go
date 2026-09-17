// Command linearizability checks an operation history recorded by the
// load driver against a sequential specification of Narad's delivery
// contract.
//
// It answers a question the running counters cannot. A counter says a
// message came back after it was acked; this says whether any ordering
// of the concurrent calls could account for that, and whether a broker
// was being killed or partitioned at the moment it happened. A
// redelivery during a fault is the documented at-least-once contract. A
// redelivery with no fault in flight is an anomaly with no cause, and
// this exits non-zero for it, because a project that tolerates
// unexplained anomalies has stopped learning from its own test runs.
//
// Usage:
//
//	linearizability -history run.jsonl [-faults faults.jsonl] [flags]
//
// Exit codes: 0 for OK and OVERDUE, 1 for ANOMALY, VIOLATION and
// UNKNOWN, 2 for a usage or I/O error.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/debanganthakuria/narad/tests/linearizability/history"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "linearizability: %v\n", err)
		os.Exit(2)
	}
}

func run(args []string) error {
	var (
		historyPath   string
		faultsPath    string
		strict        bool
		grace         time.Duration
		timeout       time.Duration
		samples       int
		minOperations int
		jsonPath      string
		markdownPath  string
		visualizePath string
	)
	flags := flag.NewFlagSet("linearizability", flag.ContinueOnError)
	flags.StringVar(&historyPath, "history", "", "JSONL operation history written by the load driver (required)")
	flags.StringVar(&faultsPath, "faults", "", "JSONL fault records written by the fault injector; merged with the history")
	flags.BoolVar(&strict, "strict", false, "treat any post-ack redelivery as a linearizability violation (exactly-once-after-ack) rather than an anomaly to attribute to a fault")
	flags.DurationVar(&grace, "grace", 0, "how long after a fault ends its after-effects are still attributed to it; defaults to the run's visibility timeout plus 20s")
	flags.DurationVar(&timeout, "timeout", 5*time.Minute, "bound on the linearizability search")
	flags.IntVar(&samples, "samples", 20, "how many examples of each finding to print")
	flags.IntVar(&minOperations, "min-operations", 1, "report UNKNOWN rather than OK below this many checked operations; a run that recorded nothing proves nothing")
	flags.StringVar(&jsonPath, "json", "", "also write the result as JSON to this path")
	flags.StringVar(&markdownPath, "markdown", "", "also write a Markdown summary to this path, for a CI job summary")
	flags.StringVar(&visualizePath, "visualize", "", "on a violation, write porcupine's interactive HTML visualization here")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if historyPath == "" {
		flags.Usage()
		return fmt.Errorf("-history is required")
	}
	if samples < 0 {
		return fmt.Errorf("-samples must be >= 0")
	}

	paths := []string{historyPath}
	if faultsPath != "" {
		// A missing fault file is not an error: a run with no faults
		// injected is a legitimate, and stricter, thing to check.
		if _, err := os.Stat(faultsPath); err == nil {
			paths = append(paths, faultsPath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat %s: %w", faultsPath, err)
		}
	}

	log, err := history.Read(paths...)
	if err != nil {
		return err
	}

	opts := checkOptions{
		Strict:        strict,
		Grace:         grace,
		Timeout:       timeout,
		Samples:       samples,
		MinOperations: minOperations,
		Visualize:     visualizePath != "",
	}
	if opts.Grace == 0 {
		opts.Grace = defaultGrace(log)
	}
	if opts.MinOperations < 1 {
		opts.MinOperations = 1
	}

	res := check(log, opts)
	writeText(os.Stdout, res)

	if jsonPath != "" {
		if err := writeJSON(jsonPath, res); err != nil {
			return err
		}
	}
	if markdownPath != "" {
		file, err := os.Create(markdownPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", markdownPath, err)
		}
		writeMarkdown(file, res)
		if err := file.Close(); err != nil {
			return fmt.Errorf("write %s: %w", markdownPath, err)
		}
	}
	if visualizePath != "" && res.hasInfo && res.Verdict == verdictViolation {
		if err := porcupine.VisualizePath(res.model, res.info, visualizePath); err != nil {
			// A missing visualization must not mask the verdict that
			// earned it, so this is reported and not returned.
			fmt.Fprintf(os.Stderr, "linearizability: visualization: %v\n", err)
		} else {
			fmt.Printf("\nvisualization    %s\n", visualizePath)
		}
	}

	if res.failed() {
		os.Exit(1)
	}
	return nil
}

// defaultGrace derives the fault grace period from the run's visibility
// timeout.
//
// After a broker is killed, the messages it held leased come back only
// when those leases expire, which is up to one visibility timeout after
// it returns. Attributing a redelivery to a fault therefore has to allow
// for that whole window, plus a margin for the partition to be
// reassigned and a consumer to poll it. Too short and honest
// redeliveries look unexplained; too long and a real anomaly hides
// inside a fault's shadow.
func defaultGrace(log *history.Log) time.Duration {
	visibility := 30 * time.Second
	if log.HasMeta && log.Meta.VisibilityTimeoutMs > 0 {
		visibility = time.Duration(log.Meta.VisibilityTimeoutMs) * time.Millisecond
	}
	return visibility + graceMargin
}

// graceMargin is the allowance on top of the visibility timeout, for the
// partition to be reassigned and a consumer to poll it.
//
// It is deliberately modest. Grace is what makes a redelivery
// "explained", so an over-generous margin merges consecutive fault
// windows into one continuous excuse and the check stops discriminating.
// The report prints the resulting fault coverage so that trade-off is
// visible rather than assumed.
const graceMargin = 20 * time.Second
