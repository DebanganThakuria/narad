package main

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// pulsarT drives Pulsar standalone: synchronous Send (waits for the
// broker ack) and a shared subscription with per-message acks. BIG
// asterisk: standalone mode runs BookKeeper with journal fsync
// DISABLED, so the ack is weaker than clustered Pulsar's default
// (which fsyncs the journal before acking). The number is standalone's,
// not Pulsar-the-cluster's.
type pulsarT struct {
	c    pulsar.Client
	prod pulsar.Producer
	cons pulsar.Consumer
}

func newPulsar(u string) (*pulsarT, error) {
	c, err := pulsar.NewClient(pulsar.ClientOptions{URL: u})
	if err != nil {
		return nil, err
	}
	return &pulsarT{c: c}, nil
}

func (p *pulsarT) name() string { return "pulsar-standalone" }

func (p *pulsarT) durability() string {
	return "broker ack; standalone disables journal fsync (clustered default fsyncs) — read as Pulsar-lite"
}

func (p *pulsarT) setup(ctx context.Context) error {
	prod, err := p.c.CreateProducer(pulsar.ProducerOptions{Topic: "bench"})
	if err != nil {
		return err
	}
	p.prod = prod
	cons, err := p.c.Subscribe(pulsar.ConsumerOptions{
		Topic:            "bench",
		SubscriptionName: "g",
		Type:             pulsar.Shared,
	})
	if err != nil {
		return err
	}
	p.cons = cons
	return nil
}

func (p *pulsarT) produceOne(ctx context.Context, payload []byte) error {
	_, err := p.prod.Send(ctx, &pulsar.ProducerMessage{Payload: payload})
	return err
}

func (p *pulsarT) consumeSome(ctx context.Context, batch int) (int, error) {
	got := 0
	for i := 0; i < batch; i++ {
		rctx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := p.cons.Receive(rctx)
		cancel()
		if err != nil {
			return got, nil // timeout: report what we have
		}
		if err := p.cons.Ack(msg); err != nil {
			return got, err
		}
		got++
	}
	return got, nil
}

func (p *pulsarT) close() {
	if p.prod != nil {
		p.prod.Close()
	}
	if p.cons != nil {
		p.cons.Close()
	}
	p.c.Close()
}
