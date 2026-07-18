package main

// Phase-2 commands: bounded history reads (replay), user/grant
// management, a self-serve benchmark, and a cluster overview table.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// cmdContext is a signal-aware context for interruptible commands.
func cmdContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx
}

func newReplayCmd() *cobra.Command {
	var (
		partition int
		from, to  int64
		raw       bool
	)
	cmd := &cobra.Command{
		Use:   "replay <topic>",
		Short: "read a bounded range of retained history (read-only, no reservations)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if partition < 0 {
				return fmt.Errorf("--partition is required (replay is a positional read)")
			}
			c := cliClient()
			start, end := from, to
			if start < 0 || end < 0 {
				oldest, hwm, err := partitionRange(c, args[0], partition)
				if err != nil {
					return err
				}
				if start < 0 {
					start = oldest
				}
				if end < 0 {
					end = hwm
				}
			}
			n := 0
			for off := start; off < end; off++ {
				path := fmt.Sprintf("/v1/topics/%s/consume?partition=%d&offset=%d", url.PathEscape(args[0]), partition, off)
				msg, ok, err := fetchOne(cmdContext(), c, path)
				if err != nil {
					if strings.Contains(err.Error(), "http 410") {
						fmt.Fprintln(os.Stderr, dim(fmt.Sprintf("offset %d aged out of retention; skipping forward", off)))
						continue
					}
					return err
				}
				if !ok {
					break // reached the visible tail early
				}
				printMessage(msg, raw)
				n++
			}
			fmt.Fprintf(os.Stderr, "%d message(s) replayed from p%d [%d, %d)\n", n, partition, start, end)
			return nil
		},
	}
	cmd.Flags().IntVar(&partition, "partition", -1, "partition to replay (required)")
	cmd.Flags().Int64Var(&from, "from", -1, "start offset (default: oldest retained)")
	cmd.Flags().Int64Var(&to, "to", -1, "end offset, exclusive (default: current high watermark)")
	cmd.Flags().BoolVar(&raw, "raw", false, "print payloads only")
	return cmd
}

func partitionRange(c *httpClient, topic string, partition int) (oldest, hwm int64, err error) {
	resp, err := c.do(http.MethodGet, "/v1/topics/"+url.PathEscape(topic), nil)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	var info struct {
		PartitionStats []struct {
			Index         int   `json:"index"`
			OldestOffset  int64 `json:"oldest_offset"`
			HighWatermark int64 `json:"high_watermark"`
		} `json:"partition_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, 0, err
	}
	for _, ps := range info.PartitionStats {
		if ps.Index == partition {
			return ps.OldestOffset, ps.HighWatermark, nil
		}
	}
	return 0, 0, fmt.Errorf("partition %d not in stats (owner down, or index out of range)", partition)
}

func newUserCmd() *cobra.Command {
	user := &cobra.Command{
		Use:   "user",
		Short: "manage users and grants (admin)",
	}

	var password string
	var grants []string
	add := &cobra.Command{
		Use:   "add <username>",
		Short: "create a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			g, err := parseGrants(grants)
			if err != nil {
				return err
			}
			body := map[string]any{"username": args[0], "password": password}
			if len(g) > 0 {
				body["grants"] = g
			}
			return cliClient().postAndPrint("/v1/users", body)
		},
	}
	add.Flags().StringVar(&password, "user-password", "", "the new user's password (required)")
	add.Flags().StringArrayVar(&grants, "grant", nil, `grant as action:pattern[,pattern] — e.g. --grant "produce:orders-*" (actions: produce, consume, create, admin)`)
	_ = add.MarkFlagRequired("user-password")

	var setGrants []string
	grant := &cobra.Command{
		Use:   "grant <username>",
		Short: "replace a user's grants",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			g, err := parseGrants(setGrants)
			if err != nil {
				return err
			}
			resp, err := cliClient().do(http.MethodPut, "/v1/users/"+url.PathEscape(args[0])+"/grants", map[string]any{"grants": g})
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			return printResponse(resp)
		},
	}
	grant.Flags().StringArrayVar(&setGrants, "grant", nil, "grant as action:pattern[,pattern]; repeatable")

	ls := &cobra.Command{
		Use:   "ls",
		Short: "list users",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return cliClient().getAndPrint("/v1/users")
		},
	}

	rm := &cobra.Command{
		Use:   "rm <username>",
		Short: "delete a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return cliClient().deleteRequest("/v1/users/" + url.PathEscape(args[0]))
		},
	}

	user.AddCommand(add, grant, ls, rm)
	return user
}

// parseGrants turns "action:pat1,pat2" strings into the API's grant
// shape. A bare "admin" (no patterns) is allowed — admin is global.
func parseGrants(specs []string) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(specs))
	for _, spec := range specs {
		action, patterns, found := strings.Cut(spec, ":")
		g := map[string]any{"action": strings.TrimSpace(action)}
		if found && patterns != "" {
			parts := strings.Split(patterns, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			g["patterns"] = parts
		}
		switch strings.TrimSpace(action) {
		case "produce", "consume", "create", "admin":
		default:
			return nil, fmt.Errorf("unknown action %q in grant %q (want produce|consume|create|admin)", action, spec)
		}
		out = append(out, g)
	}
	return out, nil
}

func newBenchCmd() *cobra.Command {
	var (
		count   int
		size    int
		workers int
		consume bool
	)
	cmd := &cobra.Command{
		Use:   "bench <topic>",
		Short: "measure produce (and optionally consume+ack) throughput from this machine",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			payload := []byte(`{"bench":"` + strings.Repeat("x", max(size-12, 1)) + `"}`)
			path := "/v1/topics/" + url.PathEscape(args[0]) + "/produce"

			fmt.Fprintf(os.Stderr, "producing %d × %dB with %d workers...\n", count, len(payload), workers)
			latencies := make([]time.Duration, count)
			var idx, failed int64
			var mu sync.Mutex
			start := time.Now()
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c := cliClient() // per-worker client: no transport contention
					for {
						mu.Lock()
						i := idx
						idx++
						mu.Unlock()
						if i >= int64(count) {
							return
						}
						t0 := time.Now()
						resp, err := c.doRaw(http.MethodPost, path, payload)
						if err != nil {
							mu.Lock()
							failed++
							mu.Unlock()
							continue
						}
						resp.Body.Close()
						latencies[i] = time.Since(t0)
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			reportBench("produce", count-int(failed), int(failed), elapsed, latencies)

			if !consume {
				return nil
			}
			fmt.Fprintf(os.Stderr, "consuming+acking %d with %d workers...\n", count, workers)
			var consumed int64
			cstart := time.Now()
			ctx := cmdContext()
			var cwg sync.WaitGroup
			for w := 0; w < workers; w++ {
				cwg.Add(1)
				go func() {
					defer cwg.Done()
					c := cliClient()
					base := "/v1/topics/" + url.PathEscape(args[0]) + "/consume?wait=2s"
					empty := 0
					for ctx.Err() == nil {
						msg, ok, err := fetchOne(ctx, c, base)
						if err != nil {
							continue
						}
						if !ok {
							empty++
							if empty >= 2 {
								return // drained
							}
							continue
						}
						empty = 0
						ackWithRetry(ctx, c, args[0], msg.ReceiptHandle)
						mu.Lock()
						consumed++
						mu.Unlock()
					}
				}()
			}
			cwg.Wait()
			celapsed := time.Since(cstart)
			fmt.Fprintf(os.Stderr, "consume: %d msgs in %s (%.0f msg/s)\n",
				consumed, celapsed.Round(time.Millisecond), float64(consumed)/celapsed.Seconds())
			return nil
		},
	}
	cmd.Flags().IntVar(&count, "count", 10_000, "messages to produce")
	cmd.Flags().IntVar(&size, "size", 256, "payload bytes")
	cmd.Flags().IntVar(&workers, "workers", 8, "concurrent workers")
	cmd.Flags().BoolVar(&consume, "consume", false, "drain the topic with consume+ack after producing")
	return cmd
}

func reportBench(op string, okCount, failed int, elapsed time.Duration, latencies []time.Duration) {
	valid := latencies[:0]
	for _, l := range latencies {
		if l > 0 {
			valid = append(valid, l)
		}
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i] < valid[j] })
	pct := func(p float64) time.Duration {
		if len(valid) == 0 {
			return 0
		}
		i := int(p * float64(len(valid)-1))
		return valid[i].Round(100 * time.Microsecond)
	}
	fmt.Fprintf(os.Stderr, "%s: %d ok, %d failed in %s — %.0f msg/s; latency p50=%s p95=%s p99=%s\n",
		op, okCount, failed, elapsed.Round(time.Millisecond),
		float64(okCount)/elapsed.Seconds(), pct(0.50), pct(0.95), pct(0.99))
}

func newServerReportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "report",
		Short: "one table: every topic's partitions, messages, size, and owner spread",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			c := cliClient()
			resp, err := c.do(http.MethodGet, "/v1/topics?limit=1000", nil)
			if err != nil {
				return err
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return err
			}
			var page topicPage
			if err := json.Unmarshal(body, &page); err != nil {
				return err
			}
			if len(page.Topics) == 0 {
				fmt.Println("no topics")
				return nil
			}
			fmt.Printf("%-28s %6s %12s %10s %7s  %s\n", bold("TOPIC"), "PARTS", "MESSAGES", "SIZE", "OWNERS", "ROLE")
			var totalMsgs, totalBytes int64
			for _, t := range page.Topics {
				iresp, err := c.do(http.MethodGet, "/v1/topics/"+url.PathEscape(t.Name), nil)
				if err != nil {
					fmt.Printf("%-28s  (stats unavailable: %v)\n", t.Name, err)
					continue
				}
				var info struct {
					PartitionStats []struct {
						HighWatermark int64  `json:"high_watermark"`
						SizeBytes     int64  `json:"size_bytes"`
						OwnerNode     string `json:"owner_node"`
					} `json:"partition_stats"`
				}
				err = json.NewDecoder(iresp.Body).Decode(&info)
				iresp.Body.Close()
				if err != nil {
					return err
				}
				var msgs, bytes int64
				owners := map[string]struct{}{}
				for _, ps := range info.PartitionStats {
					msgs += ps.HighWatermark
					bytes += ps.SizeBytes
					if ps.OwnerNode != "" {
						owners[ps.OwnerNode] = struct{}{}
					}
				}
				totalMsgs += msgs
				totalBytes += bytes
				role := "-"
				if t.Parent != "" {
					role = "child of " + t.Parent
				} else if t.Role == "parent" {
					role = "parent"
				}
				fmt.Printf("%-28s %6d %12d %10s %7d  %s\n", t.Name, t.Partitions, msgs, humanBytes(bytes), len(owners), role)
			}
			fmt.Printf("%-28s %6s %12d %10s\n", dim("total"), "", totalMsgs, humanBytes(totalBytes))
			return nil
		},
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
