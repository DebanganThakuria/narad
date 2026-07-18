package main

// `narad topic ...` — topic lifecycle with human units: durations for
// retention/visibility (12h, 30s) instead of raw milliseconds, and
// create-as-child via --parent/--delay.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"
)

func newTopicCmd() *cobra.Command {
	topic := &cobra.Command{
		Use:     "topic",
		Aliases: []string{"topics"},
		Short:   "create, inspect, and manage topics",
	}
	topic.AddCommand(topicAddCmd(), topicLsCmd(), topicInfoCmd(), topicEditCmd(),
		topicRmCmd(), topicAttachCmd(), topicDetachCmd(), topicChildrenCmd())
	return topic
}

func topicAddCmd() *cobra.Command {
	var (
		partitions          int
		retention           time.Duration
		visibility          time.Duration
		parent              string
		delay               time.Duration
		maxInFlight, maxAck int
	)
	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "create a topic (or a fan-out/delay child with --parent)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{"name": args[0]}
			if partitions > 0 {
				body["partitions"] = partitions
			}
			if retention > 0 {
				body["retention_ms"] = retention.Milliseconds()
			}
			if visibility > 0 {
				body["visibility_timeout_ms"] = visibility.Milliseconds()
			}
			if maxInFlight > 0 {
				body["max_in_flight_per_partition"] = maxInFlight
			}
			if maxAck > 0 {
				body["max_acked_ahead_per_partition"] = maxAck
			}
			if parent != "" {
				body["parent"] = parent
				if delay > 0 {
					body["fanout_delay_ms"] = delay.Milliseconds()
				}
			} else if delay > 0 {
				return fmt.Errorf("--delay requires --parent (delay lives on fan-out children)")
			}
			return cliClient().postAndPrint("/v1/topics", body)
		},
	}
	cmd.Flags().IntVar(&partitions, "partitions", 0, "partition count (0 = server default)")
	cmd.Flags().DurationVar(&retention, "retention", 0, "retention window, e.g. 12h (0 = server default)")
	cmd.Flags().DurationVar(&visibility, "visibility", 0, "visibility timeout, e.g. 30s (0 = server default)")
	cmd.Flags().StringVar(&parent, "parent", "", "create as a fan-out child of this topic (replica pattern)")
	cmd.Flags().DurationVar(&delay, "delay", 0, "delivery delay for a delayed child (requires --parent)")
	cmd.Flags().IntVar(&maxInFlight, "max-in-flight", 0, "per-partition in-flight cap")
	cmd.Flags().IntVar(&maxAck, "max-acked-ahead", 0, "per-partition out-of-order ack cap")
	return cmd
}

// topicPage mirrors the list response shape the CLI needs.
type topicPage struct {
	NextPageToken string      `json:"next_page_token"`
	Topics        []topicItem `json:"topics"`
}

type topicItem struct {
	Name          string `json:"name"`
	Partitions    int    `json:"partitions"`
	RetentionMs   int64  `json:"retention_ms"`
	Role          string `json:"role"`
	Parent        string `json:"parent"`
	FanoutDelayMs int64  `json:"fanout_delay_ms"`
}

func topicLsCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "list topics",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			c := cliClient()
			var all []topicItem
			token := ""
			for {
				path := "/v1/topics?limit=1000"
				if token != "" {
					path += "&page_token=" + url.QueryEscape(token)
				}
				resp, err := c.do(http.MethodGet, path, nil)
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
					return fmt.Errorf("parse topics response: %w", err)
				}
				all = append(all, page.Topics...)
				if page.NextPageToken == "" {
					break
				}
				token = page.NextPageToken
			}
			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(all)
			}
			if len(all) == 0 {
				fmt.Println("no topics")
				return nil
			}
			fmt.Printf("%-32s %10s %12s  %s\n", bold("NAME"), "PARTITIONS", "RETENTION", "ROLE")
			for _, t := range all {
				role := t.Role
				switch {
				case t.Parent != "" && t.FanoutDelayMs > 0:
					role = fmt.Sprintf("child of %s (delay %s)", t.Parent, time.Duration(t.FanoutDelayMs)*time.Millisecond)
				case t.Parent != "":
					role = "child of " + t.Parent
				case role == "parent":
					role = "parent"
				default:
					role = "-"
				}
				retention := "forever"
				if t.RetentionMs > 0 {
					retention = (time.Duration(t.RetentionMs) * time.Millisecond).String()
				}
				fmt.Printf("%-32s %10d %12s  %s\n", t.Name, t.Partitions, retention, role)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "raw JSON output")
	return cmd
}

func topicInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <name>",
		Short: "topic config + per-partition stats",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return cliClient().getAndPrint("/v1/topics/" + url.PathEscape(args[0]))
		},
	}
}

func topicEditCmd() *cobra.Command {
	var (
		retention, visibility time.Duration
		partitions            int
	)
	cmd := &cobra.Command{
		Use:   "edit <name>",
		Short: "alter retention, visibility, or partition count",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{}
			if retention > 0 {
				body["retention_ms"] = retention.Milliseconds()
			}
			if visibility > 0 {
				body["visibility_timeout_ms"] = visibility.Milliseconds()
			}
			if partitions > 0 {
				body["partitions"] = partitions
			}
			if len(body) == 0 {
				return fmt.Errorf("nothing to change (see --help)")
			}
			return cliClient().patchAndPrint("/v1/topics/"+url.PathEscape(args[0]), body)
		},
	}
	cmd.Flags().DurationVar(&retention, "retention", 0, "new retention window")
	cmd.Flags().DurationVar(&visibility, "visibility", 0, "new visibility timeout")
	cmd.Flags().IntVar(&partitions, "partitions", 0, "new partition count (grow only)")
	return cmd
}

func topicRmCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "rm <name>",
		Short: "delete a topic and ALL its data",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if !force {
				fmt.Printf("delete topic %q and all its data? [y/N] ", args[0])
				var answer string
				_, _ = fmt.Scanln(&answer)
				if answer != "y" && answer != "Y" && answer != "yes" {
					return fmt.Errorf("aborted")
				}
			}
			if err := cliClient().deleteRequest("/v1/topics/" + url.PathEscape(args[0])); err != nil {
				return err
			}
			fmt.Printf("deleted %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "skip confirmation")
	return cmd
}

func topicAttachCmd() *cobra.Command {
	var delay time.Duration
	cmd := &cobra.Command{
		Use:   "attach <parent> <child>",
		Short: "attach an existing topic as a fan-out (or delayed) child",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{"child": args[1]}
			if delay > 0 {
				body["delay_ms"] = delay.Milliseconds()
			}
			return cliClient().postAndPrint("/v1/topics/"+url.PathEscape(args[0])+"/children", body)
		},
	}
	cmd.Flags().DurationVar(&delay, "delay", 0, "delivery delay, e.g. 30s")
	return cmd
}

func topicDetachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "detach <parent> <child>",
		Short: "detach a child (the child topic and its data remain)",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return cliClient().deleteRequest("/v1/topics/" + url.PathEscape(args[0]) + "/children/" + url.PathEscape(args[1]))
		},
	}
}

func topicChildrenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "children <parent>",
		Short: "list a parent's children with fan-out lag",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return cliClient().getAndPrint("/v1/topics/" + url.PathEscape(args[0]) + "/children")
		},
	}
}
