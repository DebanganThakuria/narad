package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
		h:    &http.Client{Timeout: 60 * time.Second},
	}
}

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
