package cluster

import (
	"net/http"
	"strconv"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func consumeRPCRequestFromHTTP(r *http.Request, topicName string, pinnedPartition *int, localOnly bool) (nodewire.ConsumeRequest, error) {
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
		if raw := q.Get("wait"); raw != "" {
			wait, err := time.ParseDuration(raw)
			if err != nil {
				return nodewire.ConsumeRequest{}, err
			}
			if wait < 0 {
				wait = 0
			}
			req.WaitNanos = int64(wait)
		}
	}
	return req, nil
}
