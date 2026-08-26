package main

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaT drives Kafka with acks=all (single broker, RF=1) and a
// consumer group with per-batch offset commits. The produce ack means
// "in the broker's page cache" — Kafka does not fsync per message by
// default; that's the durability asterisk, not a misconfiguration.
type kafkaT struct {
	prod *kgo.Client
	cons *kgo.Client
}

func newKafka(bootstrap string) (*kafkaT, error) {
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic("bench"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup("g"),
		kgo.ConsumeTopics("bench"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		prod.Close()
		return nil, err
	}
	return &kafkaT{prod: prod, cons: cons}, nil
}

func (k *kafkaT) name() string { return "kafka" }

func (k *kafkaT) durability() string {
	return "acks=all to page cache (no per-message fsync — Kafka default); single broker RF=1"
}

func (k *kafkaT) setup(ctx context.Context) error {
	// Auto-topic-creation on first produce; force it now so the
	// produce phase doesn't pay the creation latency.
	return k.prod.ProduceSync(ctx, &kgo.Record{Value: []byte("warmup")}).FirstErr()
}

func (k *kafkaT) produceOne(ctx context.Context, payload []byte) error {
	return k.prod.ProduceSync(ctx, &kgo.Record{Value: payload}).FirstErr()
}

func (k *kafkaT) consumeSome(ctx context.Context, batch int) (int, error) {
	fctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	fetches := k.cons.PollRecords(fctx, batch)
	if err := fetches.Err0(); err != nil && fetches.Empty() {
		if fctx.Err() != nil {
			return 0, nil // poll timeout: treat as empty
		}
		return 0, err
	}
	n := fetches.NumRecords()
	if n == 0 {
		return 0, nil
	}
	if err := k.cons.CommitUncommittedOffsets(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func (k *kafkaT) close() {
	k.prod.Close()
	k.cons.Close()
}
