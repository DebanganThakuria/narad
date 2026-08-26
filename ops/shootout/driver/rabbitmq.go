package main

import (
	"time"
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// rabbitT drives RabbitMQ with a durable quorum queue, persistent
// messages, and publisher confirms — the confirm means the message is
// written and fsynced by the queue's Raft log (single member here).
// Each producer worker gets its own channel in confirm mode; consumers
// share one channel with manual per-message acks.
type rabbitT struct {
	conn *amqp.Connection
	mu   sync.Mutex
	pubs chan *amqp.Channel

	cmu   sync.Mutex
	cchan *amqp.Channel
	deliv <-chan amqp.Delivery
}

func newRabbit(u string) (*rabbitT, error) {
	conn, err := amqp.Dial(u)
	if err != nil {
		return nil, err
	}
	return &rabbitT{conn: conn, pubs: make(chan *amqp.Channel, 64)}, nil
}

func (r *rabbitT) name() string { return "rabbitmq-quorum" }

func (r *rabbitT) durability() string {
	return "publisher confirm after quorum-queue fsync (single member); persistent messages"
}

func (r *rabbitT) setup(ctx context.Context) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	_, err = ch.QueueDeclare("bench", true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	})
	return err
}

func (r *rabbitT) pubChannel() (*amqp.Channel, error) {
	select {
	case ch := <-r.pubs:
		return ch, nil
	default:
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}
	return ch, nil
}

func (r *rabbitT) produceOne(ctx context.Context, payload []byte) error {
	ch, err := r.pubChannel()
	if err != nil {
		return err
	}
	conf, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", "bench", false, false,
		amqp.Publishing{ContentType: "application/octet-stream", DeliveryMode: amqp.Persistent, Body: payload})
	if err != nil {
		ch.Close()
		return err
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		ch.Close()
		return err
	}
	if !acked {
		ch.Close()
		return fmt.Errorf("publish nacked")
	}
	select {
	case r.pubs <- ch:
	default:
		ch.Close()
	}
	return nil
}

func (r *rabbitT) consumeSome(ctx context.Context, batch int) (int, error) {
	r.cmu.Lock()
	if r.cchan == nil {
		ch, err := r.conn.Channel()
		if err != nil {
			r.cmu.Unlock()
			return 0, err
		}
		if err := ch.Qos(64, 0, false); err != nil {
			ch.Close()
			r.cmu.Unlock()
			return 0, err
		}
		d, err := ch.ConsumeWithContext(ctx, "bench", "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			r.cmu.Unlock()
			return 0, err
		}
		r.cchan, r.deliv = ch, d
	}
	deliv := r.deliv
	r.cmu.Unlock()

	got := 0
	timer := time.After(time.Second)
	for got < batch {
		select {
		case <-ctx.Done():
			return got, ctx.Err()
		case <-timer:
			return got, nil
		case m, ok := <-deliv:
			if !ok {
				return got, fmt.Errorf("delivery channel closed")
			}
			if err := m.Ack(false); err != nil {
				return got, err
			}
			got++
		}
	}
	return got, nil
}

func (r *rabbitT) close() {
	close(r.pubs)
	for ch := range r.pubs {
		ch.Close()
	}
	r.conn.Close()
}
