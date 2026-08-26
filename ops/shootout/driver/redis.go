package main

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisT drives Redis Streams: XADD to produce, XREADGROUP+XACK to
// consume. The container runs appendonly=yes, so the XADD reply means
// "in the AOF buffer" — fsync happens on the everysec cadence, and
// there is no replica. The weakest confirmation in the shootout.
type redisT struct {
	c *redis.Client
}

func newRedis(addr string) (*redisT, error) {
	return &redisT{c: redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})}, nil
}

func (r *redisT) name() string { return "redis-streams" }

func (r *redisT) durability() string {
	return "AOF everysec (≤1s loss window on crash); no replication"
}

func (r *redisT) setup(ctx context.Context) error {
	err := r.c.XGroupCreateMkStream(ctx, "bench", "g", "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (r *redisT) produceOne(ctx context.Context, payload []byte) error {
	return r.c.XAdd(ctx, &redis.XAddArgs{
		Stream: "bench",
		Values: map[string]any{"d": payload},
	}).Err()
}

func (r *redisT) consumeSome(ctx context.Context, batch int) (int, error) {
	res, err := r.c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "g",
		Consumer: "c",
		Streams:  []string{"bench", ">"},
		Count:    int64(batch),
		Block:    time.Second,
	}).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	got := 0
	for _, stream := range res {
		ids := make([]string, 0, len(stream.Messages))
		for _, m := range stream.Messages {
			ids = append(ids, m.ID)
		}
		if len(ids) == 0 {
			continue
		}
		if err := r.c.XAck(ctx, "bench", "g", ids...).Err(); err != nil {
			return got, err
		}
		got += len(ids)
	}
	return got, nil
}

func (r *redisT) close() { r.c.Close() }
