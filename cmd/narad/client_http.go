package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

// httpClient is the thin HTTP transport behind the client subcommands.
// user/password, when set, are sent as HTTP Basic auth on every request.
type httpClient struct {
	addr     string
	user     string
	password string
	h        *http.Client
}

func newHTTPClient(addr string) *httpClient {
	return &httpClient{
		addr: strings.TrimRight(addr, "/"),
		h:    &http.Client{Timeout: 60 * time.Second, Transport: newCLITransport()},
	}
}

// newCLITransport is http.DefaultTransport with a connection pool sized
// for many concurrent workers against one broker. The default keeps only
// two idle connections per host, so a bench or sub run with dozens of
// workers opened a fresh TCP connection for almost every request: tens
// of thousands of TIME_WAIT sockets, ephemeral-port exhaustion reported
// as failures, and a handshake added to every latency sample.
func newCLITransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 1024
	t.MaxIdleConnsPerHost = 1024
	t.IdleConnTimeout = 90 * time.Second
	t.DialContext = newSpreadDialer(t.DialContext, net.DefaultResolver.LookupIPAddr)
	return t
}

// newSpreadDialer wraps dial so that consecutive connections to a name
// that resolves to several addresses (a Kubernetes headless service, a
// multi-A record for the brokers) are spread across those addresses
// round-robin instead of all landing on the first one. With a pooled
// transport every worker keeps its connection for the whole run, so
// where a connection lands at dial time decides which broker carries
// that worker's load; letting the resolver's first answer (or a
// ClusterIP's per-connection roll of the dice) decide it put 5x the
// produce traffic on one of three brokers in a load test. Names that
// resolve to a single address, and literal IPs, dial as before.
//
// The rotation counter is process-wide, not per transport: bench and
// sub build one client per worker (each with its own transport) so the
// workers do not contend on one transport's locks, and a per-transport
// counter would give every worker's single dial the same first address,
// which is exactly the pinning this dialer exists to remove.
func newSpreadDialer(dial func(ctx context.Context, network, addr string) (net.Conn, error), lookup func(ctx context.Context, host string) ([]net.IPAddr, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || net.ParseIP(host) != nil {
			return dial(ctx, network, addr)
		}
		ips, err := lookup(ctx, host)
		if err != nil || len(ips) <= 1 {
			return dial(ctx, network, addr)
		}
		// Sort the answer: CoreDNS rotates the order of a headless
		// service's records per query, and a rotating index over a
		// rotating list can favour one address (one broker measured a
		// third fewer requests than the other two).
		addrs := make([]string, len(ips))
		for j, ip := range ips {
			addrs[j] = ip.IP.String()
		}
		slices.Sort(addrs)
		i := spreadDialCounter.Add(1) - 1
		return dial(ctx, network, net.JoinHostPort(addrs[i%uint64(len(addrs))], port))
	}
}

// spreadDialCounter is shared by every spread dialer in the process; see
// newSpreadDialer.
var spreadDialCounter atomic.Uint64

// newContextHTTPClient builds a client from resolved connection
// settings (context/env/flags), carrying credentials.
func newContextHTTPClient(c cliContext) *httpClient {
	hc := newHTTPClient(c.Server)
	hc.user, hc.password = c.User, c.Password
	return hc
}

// do sends a JSON request. A non-nil body is marshalled and sent as
// application/json.
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
	return c.send(req)
}

// doRaw sends body verbatim as application/octet-stream.
func (c *httpClient) doRaw(method, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, c.addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return c.send(req)
}

// send executes the request and converts 4xx/5xx responses into errors
// carrying the server's error message.
func (c *httpClient) send(req *http.Request) (*http.Response, error) {
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	resp, err := c.h.Do(req)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, formatErrorBody(b))
	}
	return resp, nil
}

// formatErrorBody renders the server's error body for human display.
// The server sends `{"error":"..."}` for handled errors; we surface
// just the message in that case. Anything else is shown verbatim
// (trimmed) so unexpected payloads still reach the operator.
func formatErrorBody(b []byte) string {
	trimmed := strings.TrimSpace(string(b))
	if trimmed == "" {
		return "<empty body>"
	}
	var env struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(b, &env); err == nil && env.Error != "" {
		return env.Error
	}
	return trimmed
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

func (c *httpClient) postRawAndPrint(path string, body []byte) error {
	resp, err := c.doRaw(http.MethodPost, path, body)
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
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
	resp.Body.Close()
	return nil
}

func (c *httpClient) deleteRequest(path string) error {
	resp, err := c.do(http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
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
