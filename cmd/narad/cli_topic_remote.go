package main

// `narad topic ...` verbs of remote children: attach with --remote,
// pause, resume, skip and wait. A remote child is a fan-out child whose
// records go to a topic on another Narad cluster; its local record is a
// stub with no partitions.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// exitStalled is `narad topic wait`'s exit status for a link stuck in a
// state that needs a fix (auth_failed, rejected_record, ...).
const exitStalled = 2

// cliExit ends the process with a status; tests replace it.
var cliExit = os.Exit

// childrenPath is the children route of parent.
func childrenPath(parent string) string {
	return "/v1/topics/" + url.PathEscape(parent) + "/children"
}

func childPath(parent, child, verb string) string {
	return childrenPath(parent) + "/" + url.PathEscape(child) + "/" + verb
}

// doStatus sends a request and returns the status and body without
// turning an error status into an error, for a caller that reads the
// answer's fields (the counts of a refused delete).
func (c *httpClient) doStatus(method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.addr+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Narad-Client", "narad-cli")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	resp, err := c.h.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("transport: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, b, err
}

// remoteAttachBody is the attach body of a remote child.
func remoteAttachBody(child, remoteName, remoteTopic, from string, lanes int, delay time.Duration, dryRun bool) map[string]any {
	body := map[string]any{"child": child, "remote": remoteName}
	if remoteTopic != "" {
		body["remote_topic"] = remoteTopic
	}
	if delay > 0 {
		body["delay_ms"] = delay.Milliseconds()
	}
	if from != "" {
		body["from"] = from
	}
	if lanes > 0 {
		body["lanes"] = lanes
	}
	if dryRun {
		body["dry_run"] = true
	}
	return body
}

func topicPauseCmd() *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "pause <parent> <child>",
		Short: "stop a remote child sending (its cursors keep their positions)",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{}
			if reason != "" {
				body["reason"] = reason
			}
			return cliClient().postAndPrint(childPath(args[0], args[1], "pause"), body)
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why (at most 256 bytes; shown in the listing)")
	return cmd
}

func topicResumeCmd() *cobra.Command {
	var acceptTarget bool
	cmd := &cobra.Command{
		Use:   "resume <parent> <child>",
		Short: "resume a paused remote child, after checking the target from every node",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{}
			if acceptTarget {
				body["accept_target"] = true
			}
			return cliClient().postAndPrint(childPath(args[0], args[1], "resume"), body)
		},
	}
	cmd.Flags().BoolVar(&acceptTarget, "accept-target", false, "accept a recreated target topic (state target_replaced)")
	return cmd
}

func topicSkipCmd() *cobra.Command {
	var partition int
	var offset int64
	cmd := &cobra.Command{
		Use:   "skip <parent> <child> --partition P --offset O",
		Short: "let a remote child drop the one record its target refuses (rejected_record, record_too_large)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("partition") || !cmd.Flags().Changed("offset") {
				return errors.New("--partition and --offset are required (the listing's blocked_at names them)")
			}
			return cliClient().postAndPrint(childPath(args[0], args[1], "skip"), map[string]any{"partition": partition, "offset": offset})
		},
	}
	cmd.Flags().IntVar(&partition, "partition", 0, "parent partition of the stuck record")
	cmd.Flags().Int64Var(&offset, "offset", 0, "parent offset of the stuck record")
	return cmd
}

// childView is one child of the listing, as wait reads it.
type childView struct {
	Name          string `json:"name"`
	LagMessages   int64  `json:"lag_messages"`
	LagComplete   bool   `json:"lag_complete"`
	State         string `json:"state"`
	SourceDrained bool   `json:"source_drained"`
	BlockedAt     *struct {
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		State     string `json:"state"`
	} `json:"blocked_at"`
}

// stalled reports a link state `topic wait` stops on: one that needs a
// fix, not time (topic.RemoteStateNeedsFix), or paused.
func stalled(state string) bool {
	return topic.RemoteStateNeedsFix(state) || state == topic.RemoteStatePaused
}

func topicWaitCmd() *cobra.Command {
	var (
		lagZero, sourceDrained bool
		stable, timeout, every time.Duration
	)
	cmd := &cobra.Command{
		Use:   "wait <parent> <child> --lag-zero|--source-drained",
		Short: "wait until a remote child has shipped everything, or the parent's consumers passed its start",
		Long: `Poll the children listing until the condition holds (for --stable, if
given). Exits 0 when reached, 1 on --timeout, and 2 when the link is
stalled in a state that needs a fix (it prints the state and the stuck
record).`,
		Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			if lagZero == sourceDrained {
				return errors.New("give exactly one of --lag-zero and --source-drained")
			}
			c := cliClient()
			deadline := time.Now().Add(timeout)
			var heldSince time.Time
			for {
				v, err := fetchChild(c, args[0], args[1])
				if err != nil {
					return err
				}
				if stalled(v.State) {
					fmt.Printf("%s is stalled: state %s", args[1], v.State)
					if v.BlockedAt != nil {
						fmt.Printf(", blocked at partition %d offset %d (%s)", v.BlockedAt.Partition, v.BlockedAt.Offset, v.BlockedAt.State)
					}
					fmt.Println()
					cliExit(exitStalled)
					return nil
				}
				reached := v.LagComplete && v.LagMessages == 0
				if sourceDrained {
					reached = v.SourceDrained
				}
				now := time.Now()
				switch {
				case !reached:
					heldSince = time.Time{}
				case heldSince.IsZero():
					heldSince = now
				}
				if reached && now.Sub(heldSince) >= stable {
					fmt.Printf("%s: reached (lag %d, state %s)\n", args[1], v.LagMessages, v.State)
					return nil
				}
				if now.After(deadline) {
					return fmt.Errorf("timed out after %s (lag %d, complete %v, state %s)", timeout, v.LagMessages, v.LagComplete, v.State)
				}
				time.Sleep(every)
			}
		},
	}
	cmd.Flags().BoolVar(&lagZero, "lag-zero", false, "wait until every record committed to the parent is on the target")
	cmd.Flags().BoolVar(&sourceDrained, "source-drained", false, "wait until the parent's consumers acked past the link's start on every partition")
	cmd.Flags().DurationVar(&stable, "stable", 0, "the condition must hold this long")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "give up after this long (exit 1)")
	cmd.Flags().DurationVar(&every, "interval", 2*time.Second, "poll interval")
	return cmd
}

// fetchChild reads one child's entry from the children listing.
func fetchChild(c *httpClient, parent, child string) (childView, error) {
	status, body, err := c.doStatus(http.MethodGet, childrenPath(parent), nil)
	if err != nil {
		return childView{}, err
	}
	if status != http.StatusOK {
		return childView{}, fmt.Errorf("http %d: %s", status, formatErrorBody(body))
	}
	var listing struct {
		Children []childView `json:"children"`
	}
	if err := json.Unmarshal(body, &listing); err != nil {
		return childView{}, fmt.Errorf("parse children listing: %w", err)
	}
	for _, v := range listing.Children {
		if v.Name == child {
			return v, nil
		}
	}
	return childView{}, fmt.Errorf("%s is not a child of %s", child, parent)
}

// unshippedRefusal is a delete's 409 when records are unshipped.
type unshippedRefusal struct {
	Error           string            `json:"error"`
	LagMessages     *int64            `json:"lag_messages"`
	LagComplete     bool              `json:"lag_complete"`
	DispatchBacklog map[string]uint64 `json:"dispatch_backlog"`
	NotAnswering    []string          `json:"not_answering"`
	// BacklogOverScanLimit names the nodes that could not read their
	// ingress backlog to the end for the check.
	BacklogOverScanLimit []string `json:"backlog_over_scan_limit"`
}

// explainUnshipped turns a delete's refusal into what the operator does
// next; ok is false when body is not an unshipped refusal.
func explainUnshipped(body []byte, detachHint string) (string, bool) {
	var r unshippedRefusal
	if json.Unmarshal(body, &r) != nil || r.LagMessages == nil {
		return "", false
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s: %d records not yet on the remote (lag complete: %v)", r.Error, *r.LagMessages, r.LagComplete)
	for node, n := range r.DispatchBacklog {
		fmt.Fprintf(&b, "; %d accepted but not yet committed on %s", n, node)
	}
	for _, node := range r.NotAnswering {
		fmt.Fprintf(&b, "; %s did not answer", node)
	}
	for _, node := range r.BacklogOverScanLimit {
		fmt.Fprintf(&b, "; %s could not read its backlog to the end (let it drain, then retry)", node)
	}
	// Waiting for the lag helps only when there is lag or a backlog to
	// ship; a node that did not answer or could not read its backlog is
	// waited for by retrying.
	shipping := *r.LagMessages > 0 || !r.LagComplete
	for _, n := range r.DispatchBacklog {
		shipping = shipping || n > 0
	}
	if shipping {
		b.WriteString(".\nWait for them to ship (narad topic wait --lag-zero)")
	} else {
		b.WriteString(".\nRetry once the nodes named above answer and have drained")
	}
	if detachHint != "" {
		b.WriteString(", or abandon them with: " + detachHint)
	}
	return b.String(), true
}
