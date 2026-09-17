# Go SDK

`curl` is fine for learning the API. For a service, use the client:

```sh
go get github.com/debanganthakuria/narad-go
```

```go
import narad "github.com/debanganthakuria/narad-go"
```

It lives in [its own repository](https://github.com/DebanganThakuria/narad-go)
and depends on nothing but the Go standard library, so importing it does
not pull the broker's dependencies into your binary.

## Three calls

```go
client, err := narad.New("localhost:7942")
if err != nil {
    return err
}
defer client.Close()

// Produce. A struct becomes JSON; []byte and string go as they are.
err = client.Produce(ctx, "orders", order)

// Consume. Long-polls, runs a worker pool, manages the lease, acks.
err = client.Consume(ctx, "orders", narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        var order Order
        if err := msg.Into(&order); err != nil {
            return err
        }
        return process(ctx, order)
    }))
```

Everything else is optional.

## Connecting

Give it every node you have, comma separated. The client spreads work
across them and routes around the ones that are failing, which is what
makes a node restart invisible to your code.

```go
client, err := narad.New("narad-0:7942,narad-1:7942,narad-2:7942",
    narad.WithAuth("svc-orders", os.Getenv("NARAD_PASSWORD")),
    narad.WithLogger(slog.Default()),
)
```

A `Client` is safe for concurrent use and owns its connection pool and
circuit breakers, so make one at startup and keep it. One created per
request throws both away.

## Producing

```go
// Anything that is not []byte or string is marshalled to JSON.
err := client.Produce(ctx, "orders", Order{ID: "ord_1", Amount: 4999})

// A key keeps a customer's messages on one partition.
err = client.Produce(ctx, "orders", order, narad.WithKey(order.CustomerID))
```

Returning nil means the message was fsynced before the broker answered.
It does not mean anyone has consumed it.

A produce whose reply was lost may still have been committed, so
retrying it can duplicate. The client retries by default, because a
duplicate is recoverable and a lost message is not:

```go
if err := client.Produce(ctx, "payments", payment); err != nil {
    if narad.Uncertain(err) {
        // It may be committed. Check before sending it again.
    }
    return err
}
```

When a duplicate is the worse outcome, `narad.WithCautiousRetries()`
hands that choice back to you.

## Consuming

`Consume` blocks until its context ends and handles the lease lifecycle,
which is the part that is easy to get subtly wrong:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

err := client.Consume(ctx, "orders", narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        var order Order
        if err := msg.Into(&order); err != nil {
            return err
        }
        return process(ctx, order)
    }),
    narad.WithWorkers(16),
)
```

Returning nil acks the message. Returning an error hands it straight
back for redelivery rather than waiting out the visibility timeout.

| What it does | Why |
|---|---|
| Long-polls | An idle consumer waits on the broker rather than spinning |
| Renews the lease while a handler runs | Slow work keeps its message instead of losing it mid-flight |
| Cancels the handler if the lease is lost | Somebody else may have the message now; carrying on would do the work twice |
| Recovers from a panicking handler | One bad message does not take the consumer down |
| Drains on shutdown | In-flight handlers get a bounded grace period to finish and ack |

!!! warning "Handlers must be idempotent"
    Narad delivers [at least once](guarantees-and-errors.md) and does not
    deduplicate. A broker restart can redeliver a message whose ack was
    still in the batch being persisted. This is not something to plan
    around later.

Watch the context in slow handlers. Once it is cancelled, the lease is
gone and another consumer may already be doing the work:

```go
for _, chunk := range chunks {
    if ctx.Err() != nil {
        return ctx.Err()
    }
    transcode(chunk)
}
```

### Taking one message

When a loop is not what you want:

```go
msg, err := client.Receive(ctx, "orders")
if err != nil {
    return err
}
if err := process(ctx, msg); err != nil {
    return msg.Nack(ctx) // back to the queue at once
}
return msg.Ack(ctx)
```

`Receive` waits until a message arrives or the context ends, so give the
context a deadline if you do not want to wait forever.

## Envelopes

Publish plainly, or wrap the message so it carries an id, a timestamp, an
attempt count and the last error:

```go
err := client.Produce(ctx, "orders", order,
    narad.WithEnvelope(),
    narad.WithHeaders(map[string]string{"trace": traceID}),
)
```

The id is a UUIDv7, so ids sort in the order they were created, and it
appears in every error and log line the client writes about that message.
`msg.Into(&v)` unwraps the envelope for you; `msg.Envelope()` gives you
the rest of it.

!!! note "Attempts advance only when you republish"
    The broker never rewrites a stored payload, so ordinary redelivery
    hands back the same bytes and the same count. `client.Retry` is what
    moves it: it publishes a fresh copy with the count raised and the
    error recorded, then acks the original.

That pairs with [delay children](fanout-and-delay.md) to give you backoff
and a parking queue without any of it living in the broker:

```go
env, err := msg.Envelope()
if err == nil && env.Attempts >= 5 {
    return client.Retry(ctx, msg, cause, "orders-parked")
}
return client.Retry(ctx, msg, cause, "orders-retry-30s")
```

### Envelopes and schemas together

A [topic schema](schemas.md) describes your message, and an envelope is a
different shape, so producing one to a schema-enforcing topic is
rejected. Wrap the schema instead and the broker validates both:

```go
_, err := client.EnsureTopic(ctx, "orders",
    narad.WithPartitionCount(6),
    narad.WithRetention(24*time.Hour),
    narad.WithEnvelopeSchema(schema),
)
```

Your `$defs` are lifted to the top of the stored schema so internal
`$ref`s keep resolving. Producers must then always use an envelope.

## Topics

```go
// What a service wants at startup: every replica races, one wins.
topic, err := client.EnsureTopic(ctx, "orders",
    narad.WithPartitionCount(6),
    narad.WithRetention(24*time.Hour),
    narad.WithVisibilityTimeout(30*time.Second),
)

topic, err = client.Topic(ctx, "orders")   // with per-partition state
topics, err := client.Topics(ctx)          // every page
err = client.DeleteTopic(ctx, "orders")
```

## Replay

Reading at an offset does not take the message off the queue, so it
holds no lease and must not be acked:

```go
for offset := int64(0); ; offset++ {
    msg, err := client.ReadAt(ctx, "orders", 0, offset)
    if errors.Is(err, narad.ErrOffsetGone) {
        continue // aged out of retention, skip forward
    }
    if err != nil {
        return err
    }
    if msg == nil {
        break // caught up with the end of the log
    }
    audit(msg)
}
```

## Errors

Three questions, answered without matching on strings:

```go
narad.Retryable(err)              // worth trying again
narad.Uncertain(err)              // might have happened anyway
errors.Is(err, narad.ErrNotFound) // which assumption was wrong
```

The sentinels are `ErrBadRequest`, `ErrUnauthenticated`, `ErrForbidden`,
`ErrNotFound`, `ErrExists`, `ErrLeaseLost`, `ErrNoLease`,
`ErrOffsetGone`, `ErrTooLarge`, `ErrThrottled`, `ErrUnavailable`,
`ErrServer`, `ErrNoNodes`, `ErrClosed` and `ErrNoEnvelope`.
`*narad.Error` carries the status, the broker's message and the node that
answered; `*narad.ConnError` covers requests that never got a reply.

The one worth handling explicitly is a lost lease, because it means the
work is going to somebody else:

```go
if err := msg.Ack(ctx); errors.Is(err, narad.ErrLeaseLost) {
    // Too slow. The message is back in the queue and will be done
    // again. Nothing to undo, since handlers are idempotent.
    return nil
}
```

## Metrics and logs

Logging takes any logger with `slog`'s method set, so the standard
library needs no adapter:

```go
narad.WithLogger(slog.Default())
```

Prometheus lives in a separate module, so the client stays free of
dependencies:

```sh
go get github.com/debanganthakuria/narad-go/prometheus
```

```go
metrics := naradprom.New(prometheus.DefaultRegisterer)
client, err := narad.New(addr, narad.WithEvents(metrics.Observe))
```

That gives you `narad_requests_total`,
`narad_request_duration_seconds`, `narad_retries_total` and
`narad_node_up`. `narad.WithEvents` takes any function, so you can report
somewhere else instead.

## Tuning

Nothing here is required. The defaults suit an ordinary service.

```go
client, err := narad.New(addr,
    narad.WithTimeout(10*time.Second),        // per attempt, not per long poll
    narad.WithRetries(5),                     // total attempts
    narad.WithBackoff(50*time.Millisecond, 5*time.Second),
    narad.WithBreaker(5, 2*time.Second),      // failures, cooldown
    narad.WithIdleConnections(64),            // per node
    narad.WithCautiousRetries(),              // never retry an uncertain write
)
```

Retries use full jitter, spread across the whole interval. Without it,
every client talking to a broker that starts shedding load retries in the
same millisecond and knocks it over again just as it recovers. Circuit
breakers are per node, so one dead node costs you that node rather than
the cluster.

## Reference

Full API documentation is on
[pkg.go.dev](https://pkg.go.dev/github.com/debanganthakuria/narad-go),
and the source is at
[DebanganThakuria/narad-go](https://github.com/DebanganThakuria/narad-go).
