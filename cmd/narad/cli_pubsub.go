package main

// `narad pub` and `narad sub` — the interactive heart of the CLI.
//
// pub sends one message (arg, --file, or stdin) or a stream of them
// (--count/--rate) for quick load generation.
//
// sub has two personalities, and the difference matters because Narad
// is a QUEUE:
//
//   - queue mode (default): a real consumer. Long-polls, prints, acks.
//     Messages it takes are settled — it competes with production
//     consumers like any other worker.
//   - --peek: a bystander. Tails every partition with replay reads
//     starting at the current tail, printing everything that flows
//     through WITHOUT reserving or acking anything. Production
//     consumers are completely undisturbed. This is the "watch the
//     topic live" debugging tool.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

func newPubCmd() *cobra.Command {
	var (
		key       string
		partition int
		file      string
		count     int
		rate      int
	)
	cmd := &cobra.Command{
		Use:   "pub <topic> [message]",
		Short: "produce a message (from arg, --file, or stdin)",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			body, err := pubBody(args, file)
			if err != nil {
				return err
			}
			c := cliClient()
			path := "/v1/topics/" + url.PathEscape(args[0]) + "/produce"
			q := url.Values{}
			if key != "" {
				q.Set("key", key)
			}
			if partition >= 0 {
				q.Set("partition", fmt.Sprintf("%d", partition))
			}
			if enc := q.Encode(); enc != "" {
				path += "?" + enc
			}

			if count <= 1 {
				resp, err := c.doRaw(http.MethodPost, path, body)
				if err != nil {
					return err
				}
				resp.Body.Close()
				fmt.Fprintf(os.Stderr, "accepted (%d bytes)\n", len(body))
				return nil
			}

			var tick <-chan time.Time
			if rate > 0 {
				t := time.NewTicker(time.Second / time.Duration(rate))
				defer t.Stop()
				tick = t.C
			}
			start := time.Now()
			for i := 1; i <= count; i++ {
				if tick != nil {
					<-tick
				}
				resp, err := c.doRaw(http.MethodPost, path, body)
				if err != nil {
					return fmt.Errorf("message %d/%d: %w", i, count, err)
				}
				resp.Body.Close()
			}
			elapsed := time.Since(start)
			fmt.Fprintf(os.Stderr, "accepted %d messages in %s (%.0f msg/s)\n",
				count, elapsed.Round(time.Millisecond), float64(count)/elapsed.Seconds())
			return nil
		},
	}
	cmd.Flags().StringVarP(&key, "key", "k", "", "partitioning key")
	cmd.Flags().IntVar(&partition, "partition", -1, "pin to an exact partition")
	cmd.Flags().StringVarP(&file, "file", "f", "", "read the message body from a file")
	cmd.Flags().IntVar(&count, "count", 1, "send the message N times")
	cmd.Flags().IntVar(&rate, "rate", 0, "messages per second when --count > 1 (0 = as fast as possible)")
	return cmd
}

func pubBody(args []string, file string) ([]byte, error) {
	switch {
	case file != "":
		return os.ReadFile(file)
	case len(args) == 2 && args[1] != "-":
		return []byte(args[1]), nil
	default:
		return io.ReadAll(os.Stdin)
	}
}

// consumedMessage mirrors the consume response.
type consumedMessage struct {
	Topic           string          `json:"topic"`
	Partition       int             `json:"partition"`
	Offset          int64           `json:"offset"`
	Key             string          `json:"key"`
	Payload         json.RawMessage `json:"payload"`
	PayloadEncoding string          `json:"payload_encoding"`
	Timestamp       int64           `json:"timestamp"`
	ReceiptHandle   string          `json:"receipt_handle"`
}

func newSubCmd() *cobra.Command {
	var (
		peek      bool
		partition int
		from      int64
		raw       bool
		noAck     bool
	)
	cmd := &cobra.Command{
		Use:   "sub <topic>",
		Short: "stream messages from a topic into your terminal",
		Long: "Stream messages from a topic.\n\n" +
			"Default (queue mode): consume + ack like a real worker — messages you see are settled.\n" +
			"--peek: tail the topic non-destructively via replay reads; production consumers are undisturbed.",
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			if peek {
				return runPeek(ctx, args[0], partition, from, raw)
			}
			if from >= 0 {
				return fmt.Errorf("--from is a --peek flag (queue mode has no position)")
			}
			return runQueueSub(ctx, args[0], partition, raw, noAck)
		},
	}
	cmd.Flags().BoolVar(&peek, "peek", false, "non-destructive tail via replay (never reserves or acks)")
	cmd.Flags().IntVar(&partition, "partition", -1, "restrict to one partition")
	cmd.Flags().Int64Var(&from, "from", -1, "with --peek: start offset (requires --partition; default: current tail)")
	cmd.Flags().BoolVar(&raw, "raw", false, "print payloads only (pipe-friendly)")
	cmd.Flags().BoolVar(&noAck, "no-ack", false, "queue mode: do not ack (messages redeliver after the visibility timeout)")
	return cmd
}

// runQueueSub is a real consumer: long-poll, print, ack (with the ack
// retry discipline the docs insist on — a failed ack is retried, never
// dropped).
func runQueueSub(ctx context.Context, topic string, partition int, raw, noAck bool) error {
	c := cliClient()
	base := "/v1/topics/" + url.PathEscape(topic) + "/consume?wait=10s"
	if partition >= 0 {
		base += fmt.Sprintf("&partition=%d", partition)
	}
	if !raw {
		mode := "queue mode — messages below are being consumed and acked"
		if noAck {
			mode = "queue mode, --no-ack — leases will lapse and messages will redeliver"
		}
		fmt.Fprintln(os.Stderr, dim("subscribed to "+topic+" ("+mode+"); ctrl-c to stop"))
	}
	var n int
	defer func() { fmt.Fprintf(os.Stderr, "\n%d message(s)\n", n) }()

	for ctx.Err() == nil {
		msg, ok, err := fetchOne(ctx, c, base)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Fprintln(os.Stderr, dim("consume error (retrying): "+err.Error()))
			if !sleepCtxCLI(ctx, time.Second) {
				return nil
			}
			continue
		}
		if !ok {
			continue // empty long-poll; loop again
		}
		printMessage(msg, raw)
		n++
		if !noAck {
			ackWithRetry(ctx, c, topic, msg.ReceiptHandle)
		}
	}
	return nil
}

// runPeek tails partitions with replay reads. Cursors start at each
// partition's current tail (or --from) and chase the high watermark;
// a 410 (offset aged out of retention) snaps the cursor forward to the
// oldest retained offset.
func runPeek(ctx context.Context, topic string, partition int, from int64, raw bool) error {
	c := cliClient()
	if from >= 0 && partition < 0 {
		return fmt.Errorf("--from requires --partition")
	}

	cursors, err := peekStartCursors(c, topic, partition, from)
	if err != nil {
		return err
	}
	if !raw {
		fmt.Fprintln(os.Stderr, dim(fmt.Sprintf("peeking %s across %d partition(s) — read-only, no reservations; ctrl-c to stop", topic, len(cursors))))
	}
	var n int
	defer func() { fmt.Fprintf(os.Stderr, "\n%d message(s)\n", n) }()

	parts := make([]int, 0, len(cursors))
	for p := range cursors {
		parts = append(parts, p)
	}
	sort.Ints(parts)

	for ctx.Err() == nil {
		progressed := false
		for _, p := range parts {
			// Drain a bounded burst per partition so one hot partition
			// can't starve the sweep.
			for range 100 {
				path := fmt.Sprintf("/v1/topics/%s/consume?partition=%d&offset=%d", url.PathEscape(topic), p, cursors[p])
				msg, ok, err := fetchOne(ctx, c, path)
				if err != nil {
					if strings.Contains(err.Error(), "http 410") {
						if oldest, oerr := oldestOffset(c, topic, p); oerr == nil {
							fmt.Fprintln(os.Stderr, dim(fmt.Sprintf("p%d: offset %d aged out; jumping to %d", p, cursors[p], oldest)))
							cursors[p] = oldest
							continue
						}
					}
					if ctx.Err() != nil {
						return nil
					}
					fmt.Fprintln(os.Stderr, dim("peek error (retrying): "+err.Error()))
					break
				}
				if !ok {
					break // caught up on this partition
				}
				printMessage(msg, raw)
				n++
				cursors[p] = msg.Offset + 1
				progressed = true
			}
		}
		if !progressed && !sleepCtxCLI(ctx, 400*time.Millisecond) {
			return nil
		}
	}
	return nil
}

// peekStartCursors resolves each partition's starting offset from the
// topic's partition stats: the current tail, so peek shows "what flows
// from now on" rather than replaying history (use --from for history).
func peekStartCursors(c *httpClient, topic string, partition int, from int64) (map[int]int64, error) {
	resp, err := c.do(http.MethodGet, "/v1/topics/"+url.PathEscape(topic), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var info struct {
		PartitionStats []struct {
			Index         int   `json:"index"`
			NextOffset    int64 `json:"next_offset"`
			HighWatermark int64 `json:"high_watermark"`
		} `json:"partition_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("parse topic info: %w", err)
	}
	cursors := map[int]int64{}
	for _, ps := range info.PartitionStats {
		if partition >= 0 && ps.Index != partition {
			continue
		}
		start := ps.HighWatermark
		if from >= 0 {
			start = from
		}
		cursors[ps.Index] = start
	}
	if len(cursors) == 0 {
		if partition >= 0 {
			return nil, fmt.Errorf("no reachable partition %d on %s (stats are owner-local; is the owner up?)", partition, topic)
		}
		return nil, fmt.Errorf("no partition stats for %s", topic)
	}
	return cursors, nil
}

func oldestOffset(c *httpClient, topic string, partition int) (int64, error) {
	resp, err := c.do(http.MethodGet, "/v1/topics/"+url.PathEscape(topic), nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var info struct {
		PartitionStats []struct {
			Index        int   `json:"index"`
			OldestOffset int64 `json:"oldest_offset"`
		} `json:"partition_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, err
	}
	for _, ps := range info.PartitionStats {
		if ps.Index == partition {
			return ps.OldestOffset, nil
		}
	}
	return 0, fmt.Errorf("partition %d not in stats", partition)
}

// fetchOne performs one consume request. ok=false means an empty poll
// (204 or a caught-up replay read).
func fetchOne(ctx context.Context, c *httpClient, path string) (consumedMessage, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.addr+path, nil)
	if err != nil {
		return consumedMessage{}, false, err
	}
	resp, err := c.send(req)
	if err != nil {
		return consumedMessage{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return consumedMessage{}, false, nil
	}
	var msg consumedMessage
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		return consumedMessage{}, false, fmt.Errorf("parse message: %w", err)
	}
	return msg, true, nil
}

// ackWithRetry settles a message, retrying transient failures — the
// documented ack discipline, baked in so `narad sub` can never cause a
// redelivery storm. 410 means the lease already lapsed; that's a fact,
// not a failure.
func ackWithRetry(ctx context.Context, c *httpClient, topic, handle string) {
	path := "/v1/topics/" + url.PathEscape(topic) + "/ack?receipt_handle=" + url.QueryEscape(handle)
	for attempt := 1; ctx.Err() == nil; attempt++ {
		err := c.postNoBody(path, nil)
		if err == nil {
			return
		}
		if strings.Contains(err.Error(), "http 410") {
			fmt.Fprintln(os.Stderr, dim("lease lapsed before ack (410) — message may redeliver"))
			return
		}
		if attempt >= 5 {
			fmt.Fprintln(os.Stderr, dim("ack failed after retries: "+err.Error()))
			return
		}
		if !sleepCtxCLI(ctx, time.Duration(attempt)*200*time.Millisecond) {
			return
		}
	}
}

// printMessage renders one message. Raw mode prints the decoded payload
// bytes only; otherwise a dim metadata header precedes the payload, and
// base64-flagged binary is decoded before display decisions.
func printMessage(msg consumedMessage, raw bool) {
	payload := []byte(msg.Payload)
	if msg.PayloadEncoding == "base64" {
		var quoted string
		if err := json.Unmarshal(msg.Payload, &quoted); err == nil {
			if decoded, err := base64.StdEncoding.DecodeString(quoted); err == nil {
				payload = decoded
			}
		}
	} else if len(payload) > 0 && payload[0] == '"' {
		// A JSON string payload (plain text produced as-is): unquote for
		// human display; --raw keeps the exact bytes for pipelines.
		var s string
		if err := json.Unmarshal(payload, &s); err == nil && !raw {
			payload = []byte(s)
		}
	}

	if raw {
		os.Stdout.Write(payload)
		fmt.Println()
		return
	}

	meta := fmt.Sprintf("[p%d @%d]", msg.Partition, msg.Offset)
	if msg.Key != "" {
		meta += " key=" + msg.Key
	}
	if msg.Timestamp > 0 {
		meta += " " + time.UnixMilli(msg.Timestamp).Format("15:04:05.000")
	}
	if msg.PayloadEncoding == "base64" {
		meta += fmt.Sprintf(" (binary, %d bytes)", len(payload))
		fmt.Println(dim(meta))
		fmt.Printf("%x\n", payload)
		return
	}
	fmt.Println(dim(meta) + " " + string(payload))
}

func sleepCtxCLI(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
