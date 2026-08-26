package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// naradT drives Narad over its plain HTTP API. 202 means the message
// is group-commit fsynced on the ingress WAL.
type naradT struct {
	base string
	h    *http.Client
}

func newNarad(base string) (*naradT, error) {
	return &naradT{
		base: base,
		h: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 128,
			},
		},
	}, nil
}

func (n *naradT) name() string { return "narad" }

func (n *naradT) durability() string {
	return "fsync (group commit) before 202; single node, single copy"
}

func (n *naradT) setup(ctx context.Context) error {
	body := bytes.NewReader([]byte(`{"name":"bench","partitions":6}`))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.base+"/v1/topics", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.h.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("create topic: status %d", resp.StatusCode)
	}
	return nil
}

func (n *naradT) produceOne(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		n.base+"/v1/topics/bench/produce", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := n.h.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("produce: status %d", resp.StatusCode)
	}
	return nil
}

func (n *naradT) consumeSome(ctx context.Context, batch int) (int, error) {
	got := 0
	for i := 0; i < batch; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			n.base+"/v1/topics/bench/consume?wait=1s", nil)
		if err != nil {
			return got, err
		}
		resp, err := n.h.Do(req)
		if err != nil {
			return got, err
		}
		if resp.StatusCode == http.StatusNoContent {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return got, nil
		}
		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return got, fmt.Errorf("consume: status %d", resp.StatusCode)
		}
		var msg struct {
			ReceiptHandle string `json:"receipt_handle"`
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err := json.Unmarshal(body, &msg); err != nil {
			return got, err
		}
		areq, err := http.NewRequestWithContext(ctx, http.MethodPost,
			n.base+"/v1/topics/bench/ack?receipt_handle="+url.QueryEscape(msg.ReceiptHandle), nil)
		if err != nil {
			return got, err
		}
		aresp, err := n.h.Do(areq)
		if err != nil {
			return got, err
		}
		io.Copy(io.Discard, aresp.Body)
		aresp.Body.Close()
		got++
	}
	return got, nil
}

func (n *naradT) close() {}
