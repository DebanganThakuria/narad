package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	return newHTTPClient(*addr).getAndPrint("/v1/topics")
}

func runClientTopicsCreate(args []string) error {
	fs := newClientFlagSet("topics create <name>")
	addr := fs.String("addr", defaultAddr(), "HTTP base URL")
	partitions := fs.Int("partitions", 0, "partition count (0 = server default)")
	rf := fs.Int("replication-factor", 0, "replication factor (0 = server default)")
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
	if *rf > 0 {
		body["replication_factor"] = *rf
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client topics alter --partitions N <name>")
	}
	if *partitions <= 0 {
		return clientUsageErr("--partitions is required and must be > 0")
	}
	return newHTTPClient(*addr).patchAndPrint(
		"/v1/topics/"+url.PathEscape(fs.Arg(0)),
		map[string]int{"partitions": *partitions},
	)
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
	if !json.Valid(body) {
		return clientUsageErr("stdin is not valid JSON")
	}
	return newHTTPClient(*addr).postAndPrint(
		"/v1/topics/"+url.PathEscape(fs.Arg(0))+"/produce",
		map[string]any{"key": *key, "message": json.RawMessage(body)},
	)
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
	partition := fs.Int("partition", -1, "partition (required)")
	offset := fs.Int64("offset", -1, "offset to ack (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return clientUsageErr("usage: narad client ack --partition P --offset O <topic>")
	}
	if *partition < 0 || *offset < 0 {
		return clientUsageErr("--partition and --offset are required and must be >= 0")
	}
	if err := newHTTPClient(*addr).postNoBody(
		"/v1/topics/"+url.PathEscape(fs.Arg(0))+"/ack",
		map[string]any{"partition": *partition, "offset": *offset},
	); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "acked")
	return nil
}

type httpClient struct {
	addr string
	h    *http.Client
}

func newHTTPClient(addr string) *httpClient {
	return &httpClient{
		addr: strings.TrimRight(addr, "/"),
		h:    &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *httpClient) do(method, path string, body any) (*http.Response, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.addr+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.h.Do(req)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

func (c *httpClient) getAndPrint(path string) error {
	resp, err := c.do(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printResponse(resp)
}

func (c *httpClient) postAndPrint(path string, body any) error {
	resp, err := c.do(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printResponse(resp)
}

func (c *httpClient) patchAndPrint(path string, body any) error {
	resp, err := c.do(http.MethodPatch, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return printResponse(resp)
}

func (c *httpClient) postNoBody(path string, body any) error {
	resp, err := c.do(http.MethodPost, path, body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *httpClient) deleteRequest(path string) error {
	resp, err := c.do(http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// printResponse pretty-prints a JSON response body to stdout. 204
// (no body) and non-JSON bodies are passed through verbatim.
func printResponse(resp *http.Response) error {
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		_, _ = os.Stdout.Write(body)
		if !bytes.HasSuffix(body, []byte("\n")) {
			fmt.Println()
		}
		return nil
	}
	pretty.WriteByte('\n')
	_, err = pretty.WriteTo(os.Stdout)
	return err
}

func defaultAddr() string {
	if v := os.Getenv("NARAD_ADDR"); v != "" {
		return v
	}
	return "http://localhost:7942"
}

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
	fmt.Fprintln(w, "  topics list                                list all topics")
	fmt.Fprintln(w, "  topics create [flags] <name>               create a topic")
	fmt.Fprintln(w, "  topics get <name>                          show topic + partition stats")
	fmt.Fprintln(w, "  topics delete <name>                       delete topic and all data")
	fmt.Fprintln(w, "  topics alter --partitions N <name>         increase partition count")
	fmt.Fprintln(w, "  produce [--key K] <topic>                  produce a record (body from stdin)")
	fmt.Fprintln(w, "  consume [flags] <topic>                    --wait D --partition P --offset O")
	fmt.Fprintln(w, "  ack --partition P --offset O <topic>")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Common flags:")
	fmt.Fprintln(w, "  --addr URL   server URL (default: http://localhost:7942 or $NARAD_ADDR)")
}
