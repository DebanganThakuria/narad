package cluster

import (
	"net/http"
	"strconv"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func consumeRPCRequestFromHTTP(r *http.Request, topicName string, pinnedPartition *int, localOnly bool, maxWait time.Duration) (nodewire.ConsumeRequest, error) {
	q := r.URL.Query()
	req := nodewire.ConsumeRequest{
		Topic:     topicName,
		LocalOnly: localOnly,
	}
	if pinnedPartition != nil {
		req.Partition = *pinnedPartition
		req.HasPartition = true
	} else if raw := q.Get("partition"); raw != "" {
		partition, err := strconv.Atoi(raw)
		if err != nil {
			return nodewire.ConsumeRequest{}, err
		}
		req.Partition = partition
		req.HasPartition = true
	}
	if raw := q.Get("offset"); raw != "" {
		offset, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nodewire.ConsumeRequest{}, err
		}
		req.Offset = offset
		req.HasOffset = true
	}
	if !localOnly {
		wait, err := consumeWaitFromHTTP(r, maxWait)
		if err != nil {
			return nodewire.ConsumeRequest{}, err
		}
		req.WaitNanos = int64(wait)
	}
	return req, nil
}

// consumeWaitFromHTTP extracts the long-poll wait budget from a consume
// request's query. Malformed values are rejected so callers surfacing the
// error keep today's HTTP 400 behavior; negative waits degrade to 0. The
// result is clamped to maxWait (when > 0): the wait stretches forwarded RPC
// deadlines and server-side parks, so an unclamped client-supplied value
// (e.g. wait=24h) could pin resources for an unbounded duration.
func consumeWaitFromHTTP(r *http.Request, maxWait time.Duration) (time.Duration, error) {
	raw := r.URL.Query().Get("wait")
	if raw == "" {
		return 0, nil
	}
	wait, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if wait < 0 {
		wait = 0
	}
	if maxWait > 0 && wait > maxWait {
		wait = maxWait
	}
	return wait, nil
}
