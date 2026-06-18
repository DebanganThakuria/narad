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

func (lb *roundRobinClient) do(ctx context.Context, method, path string, body any, out any, want ...int) (int, []byte, error) {
	node := lb.nodes[int(lb.next.Add(1)-1)%len(lb.nodes)]
	return lb.doTo(ctx, node, method, path, body, out, want...)
}

func (lb *roundRobinClient) doTo(ctx context.Context, node, method, path string, body any, out any, want ...int) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, node+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := lb.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if !wantedStatus(resp.StatusCode, want) {
		return resp.StatusCode, respBody, fmt.Errorf("%s %s returned status %d: %s", method, node+path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out != nil && len(bytes.TrimSpace(respBody)) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, respBody, fmt.Errorf("decode %s %s: %w: %s", method, path, err, string(respBody))
		}
	}
	return resp.StatusCode, respBody, nil
}

func wantedStatus(status int, want []int) bool {
	return slices.Contains(want, status)
}
