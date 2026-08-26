package main

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// natsT drives NATS JetStream: synchronous Publish (wait for the
// PubAck) into a file-storage R1 stream, pull-consume + AckSync. The
// PubAck means "in the stream" — file storage fsyncs on the server's
// sync_interval (default 2 minutes), not per message.
type natsT struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	cons jetstream.Consumer
}

func newNATS(u string) (*natsT, error) {
	nc, err := nats.Connect(u)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &natsT{nc: nc, js: js}, nil
}

func (n *natsT) name() string { return "nats-jetstream" }

func (n *natsT) durability() string {
	return "PubAck after write to file-storage R1 stream; fsync every 2m (server default), single node"
}

func (n *natsT) setup(ctx context.Context) error {
	s, err := n.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "BENCH",
		Subjects: []string{"bench"},
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	})
	if err != nil {
		return err
	}
	n.cons, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "g",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   30 * time.Second,
	})
	return err
}

func (n *natsT) produceOne(ctx context.Context, payload []byte) error {
	_, err := n.js.Publish(ctx, "bench", payload)
	return err
}

func (n *natsT) consumeSome(ctx context.Context, batch int) (int, error) {
	msgs, err := n.cons.Fetch(batch, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return 0, err
	}
	got := 0
	for m := range msgs.Messages() {
		if err := m.DoubleAck(ctx); err != nil {
			return got, err
		}
		got++
	}
	return got, msgs.Error()
}

func (n *natsT) close() { n.nc.Close() }
