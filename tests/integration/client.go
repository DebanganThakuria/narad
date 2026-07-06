package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

// maxResponseBytes caps how much of a response body the driver reads.
const maxResponseBytes = 4 << 20

// nextNode returns the next base URL in round-robin order.
func (lb *roundRobinClient) nextNode() string {
	return lb.nodes[int(lb.next.Add(1)-1)%len(lb.nodes)]
}

// do sends a JSON-encoded body to the next node in round-robin order.
func (lb *roundRobinClient) do(ctx context.Context, method, path string, body any, out any, want ...int) (int, []byte, error) {
	return lb.doTo(ctx, lb.nextNode(), method, path, body, out, want...)
}

// doRaw sends raw bytes to the next node in round-robin order.
func (lb *roundRobinClient) doRaw(ctx context.Context, method, path string, body []byte, out any, want ...int) (int, []byte, error) {
	return lb.doRawTo(ctx, lb.nextNode(), method, path, body, out, want...)
}

// doTo sends a JSON-encoded body to a specific node.
func (lb *roundRobinClient) doTo(ctx context.Context, node, method, path string, body any, out any, want ...int) (int, []byte, error) {
	var reader io.Reader
	contentType := ""
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
		contentType = "application/json"
	}
	return lb.send(ctx, node, method, path, reader, contentType, out, want)
}

// doRawTo sends raw bytes to a specific node.
func (lb *roundRobinClient) doRawTo(ctx context.Context, node, method, path string, body []byte, out any, want ...int) (int, []byte, error) {
	return lb.send(ctx, node, method, path, bytes.NewReader(body), "application/octet-stream", out, want)
}

// send issues the request and enforces the caller's accepted status set.
// When out is non-nil and the body is non-empty, the body is decoded as
// JSON into out.
func (lb *roundRobinClient) send(ctx context.Context, node, method, path string, body io.Reader, contentType string, out any, want []int) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, node+path, body)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if lb.username != "" {
		req.SetBasicAuth(lb.username, lb.password)
	}
	resp, err := lb.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if !slices.Contains(want, resp.StatusCode) {
		return resp.StatusCode, respBody, fmt.Errorf("%s %s returned status %d: %s", method, node+path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out != nil && len(bytes.TrimSpace(respBody)) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, respBody, fmt.Errorf("decode %s %s: %w: %s", method, path, err, string(respBody))
		}
	}
	return resp.StatusCode, respBody, nil
}
