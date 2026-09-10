package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// xnode mode: produce-to-deliver latency for a consumer parked in a long
// poll on one node while the message lands on another. Three partitions
// (the minimum) with every message keyed to one of them, so the owner is
// a single known node A. Four placements are timed, one
// message at a time, from one client clock:
//
//	local     consumer on A, produce to A
//	cross     consumer on B, produce to A  (the owner has it; B must be woken)
//	cross-B   consumer on B, produce to B  (B forwards to A, then wakes itself)
//	cross-C   consumer on B, produce to C  (C forwards to A, A wakes B)
//
// Each sample: park the consumer (wait=5s), give it a moment to be
// registered, stamp t0, produce, record the 202 time, then the time the
// consumer's response arrived. e2e = consumer return - t0.
func runXnode(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	lb := &roundRobinClient{
		nodes:    cfg.nodes,
		client:   &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second}},
		username: cfg.username,
		password: cfg.password,
	}
	if len(cfg.nodes) < 3 {
		return fmt.Errorf("xnode needs 3 nodes, have %d", len(cfg.nodes))
	}
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	topicName := cfg.runID + "-xnode"
	req := newTopicRecord(topicName, 3, cfg.visibilityTimeout) // 3 = the minimum; every message is keyed to one partition
	if err := retry(ctx, 20, 250*time.Millisecond, func() error {
		_, _, err := lb.do(ctx, http.MethodPost, "/v1/topics", req, nil, http.StatusCreated, http.StatusConflict)
		return err
	}); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	defer func() {
		_, _, _ = lb.do(context.Background(), http.MethodDelete, "/v1/topics/"+url.PathEscape(topicName), nil, nil, http.StatusNoContent, http.StatusOK, http.StatusNotFound)
	}()

	// Probe: produce one keyed message and see which partition it lands
	// in, then read that partition's owner.
	consumeProbe := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=5s"
	if err := retry(ctx, 20, 250*time.Millisecond, func() error {
		_, _, err := lb.doRaw(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/produce?key=k", []byte(`{"probe":true}`), nil, http.StatusAccepted)
		return err
	}); err != nil {
		return fmt.Errorf("probe produce: %w", err)
	}
	var probe consumeResponse
	if err := retry(ctx, 8, 500*time.Millisecond, func() error {
		st, _, err := lb.do(ctx, http.MethodGet, consumeProbe, nil, &probe, http.StatusOK, http.StatusNoContent)
		if err != nil {
			return err
		}
		if st != http.StatusOK {
			return fmt.Errorf("probe consume: empty (a fresh topic's partitions may still be unassigned)")
		}
		return nil
	}); err != nil {
		return err
	}
	_, _, _ = lb.do(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/ack?receipt_handle="+url.QueryEscape(probe.ReceiptHandle), nil, nil, http.StatusNoContent, http.StatusGone)
	owners, err := partitionOwners(ctx, lb, topicName)
	if err != nil {
		return err
	}
	owner, ok := owners[probe.Partition]
	if !ok {
		return fmt.Errorf("no owner for partition %d in %v", probe.Partition, owners)
	}
	// Map the owner's member id to one of our base URLs: by the address
	// the cluster advertises for it (host:port of its HTTP listener), or
	// failing that by the id appearing in the URL (pod DNS names).
	ownerAddr := ""
	var members struct {
		Members []struct {
			ID   string `json:"id"`
			Addr string `json:"addr"`
		} `json:"members"`
	}
	if _, _, err := lb.do(ctx, http.MethodGet, "/v1/cluster/members", nil, &members, http.StatusOK); err == nil {
		for _, m := range members.Members {
			if m.ID == owner {
				ownerAddr = m.Addr
			}
		}
	}
	var a, b, c string
	for _, n := range cfg.nodes {
		switch {
		case (ownerAddr != "" && strings.Contains(n, ownerAddr)) || (ownerAddr == "" && strings.Contains(n, owner)):
			a = n
		case b == "":
			b = n
		default:
			c = n
		}
	}
	if a == "" {
		return fmt.Errorf("owner %q not found among nodes %v", owner, cfg.nodes)
	}
	short := func(n string) string {
		h := strings.TrimPrefix(n, "http://")
		return strings.SplitN(h, ".", 2)[0]
	}
	fmt.Printf("xnode: topic=%s key k -> partition %d owner=%s (A) B=%s C=%s samples=%d\n", topicName, probe.Partition, short(a), short(b), short(c), cfg.messages)

	type scenario struct {
		name           string
		consumeOn      string
		produceTo      string
		e2e, produce   []time.Duration
		misses, errors int
	}
	scenarios := []*scenario{
		{name: "local   (consume A, produce A)", consumeOn: a, produceTo: a},
		{name: "cross   (consume B, produce A)", consumeOn: b, produceTo: a},
		{name: "cross-B (consume B, produce B)", consumeOn: b, produceTo: b},
		{name: "cross-C (consume B, produce C)", consumeOn: b, produceTo: c},
	}
	consumePath := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=5s"
	producePath := "/v1/topics/" + url.PathEscape(topicName) + "/produce?key=k"
	seq := 0
	for _, sc := range scenarios {
		for i := 0; i < cfg.messages+1; i++ { // +1: the first sample warms connections and is dropped
			seq++
			type res struct {
				at     time.Time
				status int
				msg    consumeResponse
				err    error
			}
			done := make(chan res, 1)
			go func() {
				var m consumeResponse
				st, _, err := lb.doTo(ctx, sc.consumeOn, http.MethodGet, consumePath, nil, &m, http.StatusOK, http.StatusNoContent)
				done <- res{at: time.Now(), status: st, msg: m, err: err}
			}()
			time.Sleep(40 * time.Millisecond) // let the long poll park before the message exists
			body, _ := json.Marshal(map[string]any{"id": fmt.Sprintf("%s/%d", cfg.runID, seq), "seq": seq})
			t0 := time.Now()
			if _, _, err := lb.doRawTo(ctx, sc.produceTo, http.MethodPost, producePath, body, nil, http.StatusAccepted); err != nil {
				sc.errors++
				<-done
				continue
			}
			tp := time.Now()
			r := <-done
			if r.err != nil {
				sc.errors++
				continue
			}
			if r.status == http.StatusNoContent {
				// The poll returned empty (parked before the produce, timed
				// out, or raced). Fetch the message so the next sample is
				// clean, but do not time it.
				sc.misses++
				var m consumeResponse
				if st, _, err := lb.doTo(ctx, sc.consumeOn, http.MethodGet, consumePath, nil, &m, http.StatusOK, http.StatusNoContent); err == nil && st == http.StatusOK {
					_, _, _ = lb.doTo(ctx, sc.consumeOn, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/ack?receipt_handle="+url.QueryEscape(m.ReceiptHandle), nil, nil, http.StatusNoContent, http.StatusGone)
				}
				continue
			}
			if want := fmt.Sprintf("%s/%d", cfg.runID, seq); r.msg.Payload.ID != want {
				// A leftover from an earlier miss, not the message just
				// produced: its timing says nothing about this sample. Ack
				// it and drain until the produced message (or nothing) comes
				// back, so one stale record cannot cascade through every
				// later sample.
				sc.misses++
				_, _, _ = lb.doTo(ctx, sc.consumeOn, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/ack?receipt_handle="+url.QueryEscape(r.msg.ReceiptHandle), nil, nil, http.StatusNoContent, http.StatusGone)
				for range 5 {
					var m consumeResponse
					st, _, err := lb.doTo(ctx, sc.consumeOn, http.MethodGet, "/v1/topics/"+url.PathEscape(topicName)+"/consume?wait=1s", nil, &m, http.StatusOK, http.StatusNoContent)
					if err != nil || st != http.StatusOK {
						break
					}
					_, _, _ = lb.doTo(ctx, sc.consumeOn, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/ack?receipt_handle="+url.QueryEscape(m.ReceiptHandle), nil, nil, http.StatusNoContent, http.StatusGone)
					if m.Payload.ID == want {
						break
					}
				}
				continue
			}
			if i > 0 {
				sc.e2e = append(sc.e2e, r.at.Sub(t0))
				sc.produce = append(sc.produce, tp.Sub(t0))
			}
			_, _, _ = lb.doTo(ctx, sc.consumeOn, http.MethodPost, "/v1/topics/"+url.PathEscape(topicName)+"/ack?receipt_handle="+url.QueryEscape(r.msg.ReceiptHandle), nil, nil, http.StatusNoContent, http.StatusGone)
		}
	}
	pct := func(d []time.Duration, p float64) time.Duration {
		if len(d) == 0 {
			return 0
		}
		s := append([]time.Duration(nil), d...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s[min(len(s)-1, int(float64(len(s))*p))]
	}
	fmt.Printf("%-34s %8s %8s %8s %8s %8s | %8s %8s | %s\n", "scenario", "n", "e2e p50", "p90", "p99", "max", "prod p50", "prod p99", "misses/errors")
	for _, sc := range scenarios {
		fmt.Printf("%-34s %8d %8s %8s %8s %8s | %8s %8s | %d/%d\n", sc.name, len(sc.e2e),
			pct(sc.e2e, 0.5).Round(100*time.Microsecond), pct(sc.e2e, 0.9).Round(100*time.Microsecond), pct(sc.e2e, 0.99).Round(100*time.Microsecond), pct(sc.e2e, 1).Round(100*time.Microsecond),
			pct(sc.produce, 0.5).Round(100*time.Microsecond), pct(sc.produce, 0.99).Round(100*time.Microsecond), sc.misses, sc.errors)
	}
	for _, sc := range scenarios {
		if len(sc.e2e) == 0 || (sc.misses+sc.errors)*10 > cfg.messages {
			return fmt.Errorf("xnode %s: %d timed samples, %d misses, %d errors: the cross-node path is not delivering", sc.name, len(sc.e2e), sc.misses, sc.errors)
		}
	}
	return nil
}
