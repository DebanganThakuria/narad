package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

// runClient dispatches `narad client <category> <action> ...`. Output:
// JSON on stdout, human progress on stderr (gh-style).
//
// Flags must precede positional args (stdlib `flag` stops at the
// first non-flag).
func runClient(args []string) error {
	if len(args) == 0 {
		return clientUsageErr("missing subcommand")
	}

	switch args[0] {
	case "-h", "--help", "help":
		printClientUsage(os.Stdout)
		return nil
	case "topics":
		return runClientTopics(args[1:])
	case "produce":
		return runClientProduce(args[1:])
	case "consume":
		return runClientConsume(args[1:])
	case "ack":
		return runClientAck(args[1:])
	default:
		return clientUsageErr(fmt.Sprintf("unknown subcommand %q", args[0]))
	}
}

func runClientTopics(args []string) error {
	if len(args) == 0 {
		return clientUsageErr("missing topics subcommand")
	}
	switch args[0] {
	case "list":
		return runClientTopicsList(args[1:])
	case "create":
		return runClientTopicsCreate(args[1:])
	case "get":
		return runClientTopicsGet(args[1:])
	case "delete":
		return runClientTopicsDelete(args[1:])
	case "alter":
		return runClientTopicsAlter(args[1:])
	default:
		return clientUsageErr(fmt.Sprintf("unknown topics subcommand %q", args[0]))
	}
}

func runClientTopicsList(args []string) error {
	fs := newClientFlagSet("topics list")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	limit := fs.Int("limit", 0, "max page size (0 = server default, 100; cap 1000)")
	pageToken := fs.String("page-token", "", "cursor returned by the previous page")
	if err := fs.Parse(args); err != nil {
		return err
	}

	q := url.Values{}
	if *limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limit))
	}
	if *pageToken != "" {
		q.Set("page_token", *pageToken)
	}
	path := "/v1/topics"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return newHTTPClient(*addr).getAndPrint(path)
}

func runClientTopicsCreate(args []string) error {
	fs := newClientFlagSet("topics create <name>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	partitions := fs.Int("partitions", 0, "partition count (0 = server default)")
	retentionMs := fs.Int64("retention-ms", 0, "retention duration in ms (0 = server default)")
	visibilityMs := fs.Int64("visibility-timeout-ms", 0, "consumer visibility timeout in ms (0 = server default)")
	maxInFlight := fs.Int64("max-in-flight-per-partition", 0, "per-partition reservation cap (0 = server default)")
	maxAckedAhead := fs.Int64("max-acked-ahead-per-partition", 0, "per-partition out-of-order ack cap (0 = server default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client topics create [flags] <name>")
	}

	body := map[string]any{"name": fs.Arg(0)}
	if *partitions > 0 {
		body["partitions"] = *partitions
	}
	if *retentionMs > 0 {
		body["retention_ms"] = *retentionMs
	}
	if *visibilityMs > 0 {
		body["visibility_timeout_ms"] = *visibilityMs
	}
	if *maxInFlight > 0 {
		body["max_in_flight_per_partition"] = *maxInFlight
	}
	if *maxAckedAhead > 0 {
		body["max_acked_ahead_per_partition"] = *maxAckedAhead
	}
	return newHTTPClient(*addr).postAndPrint("/v1/topics", body)
}

func runClientTopicsGet(args []string) error {
	fs := newClientFlagSet("topics get <name>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client topics get <name>")
	}
	return newHTTPClient(*addr).getAndPrint("/v1/topics/" + url.PathEscape(fs.Arg(0)))
}

func runClientTopicsDelete(args []string) error {
	fs := newClientFlagSet("topics delete <name>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client topics delete <name>")
	}
	if err := newHTTPClient(*addr).deleteRequest("/v1/topics/" + url.PathEscape(fs.Arg(0))); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "deleted")
	return nil
}

func runClientTopicsAlter(args []string) error {
	fs := newClientFlagSet("topics alter <name>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	partitions := fs.Int("partitions", 0, "new partition count (must exceed current)")
	retentionMs := fs.Int64("retention-ms", -1, "new retention duration in ms (0 = inherit default)")
	maxInFlight := fs.Int64("max-in-flight-per-partition", -1, "new per-partition reservation cap (0 = inherit default)")
	maxAckedAhead := fs.Int64("max-acked-ahead-per-partition", -1, "new per-partition out-of-order ack cap (0 = inherit default)")
	schemaFile := fs.String("schema-file", "", `path to JSON Schema file ("-" reads from stdin)`)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client topics alter [--partitions N | --retention-ms N | --max-in-flight-per-partition N | --max-acked-ahead-per-partition N | --schema-file F] <name>")
	}

	body := map[string]any{}
	if *partitions > 0 {
		body["partitions"] = *partitions
	}
	if *retentionMs >= 0 {
		body["retention_ms"] = *retentionMs
	}
	if *maxInFlight >= 0 {
		body["max_in_flight_per_partition"] = *maxInFlight
	}
	if *maxAckedAhead >= 0 {
		body["max_acked_ahead_per_partition"] = *maxAckedAhead
	}
	if *schemaFile != "" {
		raw, err := readSchemaFile(*schemaFile)
		if err != nil {
			return err
		}
		body["schema"] = json.RawMessage(raw)
	}

	if len(body) == 0 {
		return clientUsageErr("at least one of --partitions, --retention-ms, --max-in-flight-per-partition, --max-acked-ahead-per-partition, or --schema-file is required")
	}

	return newHTTPClient(*addr).patchAndPrint(
		"/v1/topics/"+url.PathEscape(fs.Arg(0)),
		body,
	)
}

func readSchemaFile(path string) ([]byte, error) {
	var (
		raw []byte
		err error
	)
	if path == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	if !json.Valid(raw) {
		return nil, clientUsageErr("schema file is not valid JSON")
	}
	return raw, nil
}

func runClientProduce(args []string) error {
	fs := newClientFlagSet("produce <topic>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	key := fs.String("key", "", "partitioning key (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client produce [--key K] <topic>  (body from stdin)")
	}

	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read stdin: %w", err)
	}
	path := "/v1/topics/" + url.PathEscape(fs.Arg(0)) + "/produce"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
	}
	return newHTTPClient(*addr).postRawAndPrint(path, body)
}

func runClientConsume(args []string) error {
	fs := newClientFlagSet("consume <topic>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	wait := fs.Duration("wait", 0, "long-poll duration")
	partition := fs.Int("partition", -1, "specific partition to read from (default: any)")
	offset := fs.Int64("offset", -1, "explicit offset to replay (requires --partition)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client consume [flags] <topic>")
	}

	q := url.Values{}
	if *wait > 0 {
		q.Set("wait", wait.String())
	}
	if *partition >= 0 {
		q.Set("partition", fmt.Sprintf("%d", *partition))
	}
	if *offset >= 0 {
		q.Set("offset", fmt.Sprintf("%d", *offset))
	}

	path := "/v1/topics/" + url.PathEscape(fs.Arg(0)) + "/consume"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return newHTTPClient(*addr).getAndPrint(path)
}

func runClientAck(args []string) error {
	fs := newClientFlagSet("ack <topic>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	handle := fs.String("handle", "", `receipt handle from a prior consume; if empty, read from stdin`)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client ack [--handle H] <topic>  (handle from stdin if omitted)")
	}

	h := strings.TrimSpace(*handle)
	if h == "" {
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read handle from stdin: %w", err)
		}
		h = strings.TrimSpace(string(raw))
	}
	if h == "" {
		return clientUsageErr("receipt handle required (use --handle or pipe via stdin)")
	}

	path := "/v1/topics/" + url.PathEscape(fs.Arg(0)) + "/ack?receipt_handle=" + url.QueryEscape(h)
	if err := newHTTPClient(*addr).postNoBody(path, nil); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "acked")
	return nil
}

// defaultAddr is the server base URL used when --addr is not given:
// $NARAD_ADDR if set, else the local default port.
func defaultAddr() string {
	if v := os.Getenv("NARAD_ADDR"); v != "" {
		return v
	}
	return "http://localhost:7942"
}

// clientUsageErr marks a client invocation error caused by bad usage
// rather than a failed request.
func clientUsageErr(msg string) error {
	return errors.New(msg)
}

func newClientFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet("client "+name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: narad client %s [flags]\n\nFlags:\n", name)
		fs.PrintDefaults()
	}
	return fs
}

func printClientUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  narad client <subcommand> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  topics list [--limit N] [--page-token T]   list topics (keyset paginated)")
	fmt.Fprintln(w, "  topics create [flags] <name>               create a topic")
	fmt.Fprintln(w, "    --partitions N                             partition count (0 = default)")
	fmt.Fprintln(w, "    --retention-ms N                           retention duration in ms (0 = default)")
	fmt.Fprintln(w, "    --visibility-timeout-ms N                  consumer visibility timeout in ms")
	fmt.Fprintln(w, "    --max-in-flight-per-partition N            per-partition reservation cap")
	fmt.Fprintln(w, "    --max-acked-ahead-per-partition N          per-partition out-of-order ack cap")
	fmt.Fprintln(w, "  topics get <name>                          show topic + partition stats")
	fmt.Fprintln(w, "  topics delete <name>                       delete topic and all data")
	fmt.Fprintln(w, "  topics alter <name>                        any combination of:")
	fmt.Fprintln(w, "    --partitions N                             increase partition count")
	fmt.Fprintln(w, "    --retention-ms N                           update retention")
	fmt.Fprintln(w, "    --max-in-flight-per-partition N            update reservation cap")
	fmt.Fprintln(w, "    --max-acked-ahead-per-partition N          update out-of-order ack cap")
	fmt.Fprintln(w, "    --schema-file F | --schema-file -          register a JSON Schema (file or stdin)")
	fmt.Fprintln(w, "  produce [--key K] <topic>                  produce a record (body from stdin)")
	fmt.Fprintln(w, "  consume [flags] <topic>                    --wait D --partition P --offset O")
	fmt.Fprintln(w, "  ack [--handle H] <topic>                   ack a message (handle from stdin if omitted)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Common flags:")
	fmt.Fprintln(w, "  --addr URL   server URL (default: http://localhost:7942 or $NARAD_ADDR)")
}
