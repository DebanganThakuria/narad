package main

// `narad topic ...` — topic lifecycle with human units: durations for
// retention/visibility (12h, 30s) instead of raw milliseconds,
// create-as-child via --parent/--delay, and JSON Schema registration
// via --schema (inline JSON, @file, or - for stdin).

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
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
		topicRmCmd(), topicAttachCmd(), topicDetachCmd(), topicChildrenCmd(), topicSchemaCmd(),
		topicPauseCmd(), topicResumeCmd(), topicSkipCmd(), topicWaitCmd())
	return topic
}

// schemaSource reads the value of a --schema flag: "@path" reads a
// file, "-" reads stdin, anything else is the schema document itself.
// The result must be a JSON value; the server decides whether it is a
// schema.
func schemaSource(flag string) (json.RawMessage, error) {
	var raw []byte
	var err error
	switch {
	case flag == "":
		return nil, nil
	case flag == "-":
		raw, err = io.ReadAll(os.Stdin)
	case strings.HasPrefix(flag, "@"):
		raw, err = os.ReadFile(flag[1:])
	default:
		raw = []byte(flag)
	}
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if !json.Valid(raw) {
		return nil, fmt.Errorf("--schema is not valid JSON (pass the document inline, @file, or - for stdin)")
	}
	return json.RawMessage(raw), nil
}

func topicAddCmd() *cobra.Command {
	var (
		partitions          int
		retention           time.Duration
		visibility          time.Duration
		parent              string
		delay               time.Duration
		maxInFlight, maxAck int
		schemaFlag          string
	)
	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "create a topic (or a fan-out/delay child with --parent)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{"name": args[0]}
			if schema, err := schemaSource(schemaFlag); err != nil {
				return err
			} else if schema != nil {
				body["schema"] = schema
			}
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
	cmd.Flags().StringVar(&schemaFlag, "schema", "", "JSON Schema every message must satisfy: inline JSON, @file, or - for stdin")
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
	Remote        *struct {
		Name  string `json:"name"`
		Topic string `json:"topic"`
	} `json:"remote"`
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
				case t.Remote != nil:
					// A stub has no partitions of its own: say where its
					// records go so it does not look broken.
					role = fmt.Sprintf("remote %s/%s (child of %s)", t.Remote.Name, t.Remote.Topic, t.Parent)
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
		schemaFlag            string
		schemaBase            int
	)
	cmd := &cobra.Command{
		Use:   "edit <name>",
		Short: "alter retention, partition count, or register a new schema version",
		Long: `Alter a topic. Each flag is one field of PATCH /v1/topics/{name}.

--schema registers a new schema version (inline JSON, @file, or - for
stdin). The server checks it is backwards compatible with the current
version; re-registering the current schema is a no-op. --schema-base-version
makes the update conditional: it is applied only if the topic's current
schema version is exactly that number, and fails with 409 otherwise, so two
operators editing the schema at the same time cannot silently stack.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			body := map[string]any{}
			if retention > 0 {
				body["retention_ms"] = retention.Milliseconds()
			}
			if visibility > 0 {
				return fmt.Errorf("visibility is fixed at create time and cannot be edited")
			}
			if partitions > 0 {
				body["partitions"] = partitions
			}
			if schema, err := schemaSource(schemaFlag); err != nil {
				return err
			} else if schema != nil {
				body["schema"] = schema
				if schemaBase > 0 {
					body["schema_base_version"] = schemaBase
				}
			} else if schemaBase > 0 {
				return fmt.Errorf("--schema-base-version requires --schema")
			}
			if len(body) == 0 {
				return fmt.Errorf("nothing to change (see --help)")
			}
			return cliClient().patchAndPrint("/v1/topics/"+url.PathEscape(args[0]), body)
		},
	}
	cmd.Flags().DurationVar(&retention, "retention", 0, "new retention window")
	cmd.Flags().DurationVar(&visibility, "visibility", 0, "(not editable; visibility is fixed at create time)")
	cmd.Flags().IntVar(&partitions, "partitions", 0, "new partition count (grow only)")
	cmd.Flags().StringVar(&schemaFlag, "schema", "", "new JSON Schema version: inline JSON, @file, or - for stdin")
	cmd.Flags().IntVar(&schemaBase, "schema-base-version", 0, "apply --schema only if the current schema version is exactly this")
	return cmd
}

// topicSchemaCmd prints a topic's schema history: every version in
// order and which one is current. --current prints only the latest
// document, which is what a producer pastes into its own validator.
func topicSchemaCmd() *cobra.Command {
	var current bool
	cmd := &cobra.Command{
		Use:   "schema <name>",
		Short: "show a topic's schema history (or just the current schema with --current)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := "/v1/topics/" + url.PathEscape(args[0]) + "/schema"
			if !current {
				return cliClient().getAndPrint(path)
			}
			c := cliClient()
			resp, err := c.do(http.MethodGet, path, nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return printResponse(resp)
			}
			var history struct {
				Version  int `json:"version"`
				Versions []struct {
					Version int             `json:"version"`
					Schema  json.RawMessage `json:"schema"`
				} `json:"versions"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
				return fmt.Errorf("parse schema response: %w", err)
			}
			if history.Version == 0 {
				fmt.Println("no schema")
				return nil
			}
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, history.Versions[len(history.Versions)-1].Schema, "", "  "); err != nil {
				return err
			}
			fmt.Println(pretty.String())
			return nil
		},
	}
	cmd.Flags().BoolVar(&current, "current", false, "print only the current schema document")
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
			// --force skips the prompt and nothing else: it never abandons
			// a remote child's unshipped records (only topic detach
			// --force does).
			c := cliClient()
			status, body, err := c.doStatus(http.MethodDelete, "/v1/topics/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			if status == http.StatusConflict {
				hint := "narad topic detach <parent> <child> --force"
				if t, ok := topicInfo(c, args[0]); ok && t.Remote != nil {
					hint = fmt.Sprintf("narad topic detach %s %s --force", t.Parent, t.Name)
				}
				if msg, ok := explainUnshipped(body, hint); ok {
					return errors.New(msg)
				}
			}
			if status >= 400 {
				return fmt.Errorf("http %d: %s", status, formatErrorBody(body))
			}
			fmt.Printf("deleted %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "skip confirmation (never abandons a remote child's unshipped records; see topic detach --force)")
	return cmd
}

// topicInfo reads a topic record; ok is false when it cannot.
func topicInfo(c *httpClient, name string) (topicItem, bool) {
	status, body, err := c.doStatus(http.MethodGet, "/v1/topics/"+url.PathEscape(name), nil)
	if err != nil || status != http.StatusOK {
		return topicItem{}, false
	}
	var t topicItem
	return t, json.Unmarshal(body, &t) == nil
}

func topicAttachCmd() *cobra.Command {
	var (
		delay                         time.Duration
		remoteName, remoteTopic, from string
		lanes                         int
		dryRun                        bool
	)
	cmd := &cobra.Command{
		Use:   "attach <parent> <child>",
		Short: "attach an existing topic as a fan-out (or delayed) child, or with --remote create a remote child",
		Long: `Attach an existing topic as a fan-out child of parent. With --remote,
create <child> as a remote child instead: a stub whose records go to a
topic on another Narad cluster through that remote (admin only).`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if remoteName == "" {
				for _, f := range []string{"remote-topic", "from", "lanes", "dry-run"} {
					if cmd.Flags().Changed(f) {
						return fmt.Errorf("--%s needs --remote", f)
					}
				}
				body := map[string]any{"child": args[1]}
				if delay > 0 {
					body["delay_ms"] = delay.Milliseconds()
				}
				return cliClient().postAndPrint(childrenPath(args[0]), body)
			}
			return cliClient().postAndPrint(childrenPath(args[0]),
				remoteAttachBody(args[1], remoteName, remoteTopic, from, lanes, delay, dryRun))
		},
	}
	cmd.Flags().DurationVar(&delay, "delay", 0, "delivery delay, e.g. 30s")
	cmd.Flags().StringVar(&remoteName, "remote", "", "create a remote child sending to this remote")
	cmd.Flags().StringVar(&remoteTopic, "remote-topic", "", "topic on the remote (default: the parent's name)")
	cmd.Flags().StringVar(&from, "from", "", "start point: attach (default), unconsumed or earliest")
	cmd.Flags().IntVar(&lanes, "lanes", 0, "parallel ordered streams per parent partition, 1 to 8 (default 1)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "run every check and resolve the start offsets; write nothing")
	return cmd
}

func topicDetachCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "detach <parent> <child>",
		Short: "detach a child (a local child keeps its data; a remote child's stub is deleted)",
		Long: `Detach a child. A local child becomes a standalone topic and keeps its
data. A remote child's stub is deleted, and only once every record of
the parent is on the remote: the server refuses (409) while records are
unshipped, unless --force abandons them.`,
		Args: cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			path := childrenPath(args[0]) + "/" + url.PathEscape(args[1])
			if force {
				path += "?force=true"
			}
			status, body, err := cliClient().doStatus(http.MethodDelete, path, nil)
			if err != nil {
				return err
			}
			if status == http.StatusConflict {
				if msg, ok := explainUnshipped(body, fmt.Sprintf("narad topic detach %s %s --force", args[0], args[1])); ok {
					return errors.New(msg)
				}
			}
			if status >= 400 {
				return fmt.Errorf("http %d: %s", status, formatErrorBody(body))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "for a remote child: delete even with unshipped records, abandoning them")
	return cmd
}

func topicChildrenCmd() *cobra.Command {
	var partitions bool
	cmd := &cobra.Command{
		Use:   "children <parent>",
		Short: "list a parent's children with fan-out lag (and each remote child's state)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := childrenPath(args[0])
			if partitions {
				path += "?partitions=true"
			}
			return cliClient().getAndPrint(path)
		},
	}
	cmd.Flags().BoolVar(&partitions, "partitions", false, "one row per parent partition for each remote child")
	return cmd
}
