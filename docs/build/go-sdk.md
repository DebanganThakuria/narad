# Go SDK: produce and consume from Go

Produce, consume and replay from a Go service with a client that manages leases, retries and failing nodes for you.

Before you start: Go 1.23 or later, and the address and credentials of a Narad cluster ([Connect and authenticate](connect.md)).

## Install the client {#install}

```sh
go get github.com/debanganthakuria/narad-go
```

```text title="Output"
go: downloading github.com/debanganthakuria/narad-go v0.0.0-20260918072218-377c85395037
go: added github.com/beorn7/perks v1.0.1
go: added github.com/cespare/xxhash/v2 v2.3.0
go: added github.com/debanganthakuria/narad-go v0.0.0-20260918072218-377c85395037
go: added github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822
go: added github.com/prometheus/client_golang v1.20.5
go: added github.com/prometheus/client_model v0.6.1
go: added github.com/prometheus/common v0.55.0
go: added github.com/prometheus/procfs v0.15.1
go: added golang.org/x/sys v0.22.0
go: added google.golang.org/protobuf v1.34.2
```

- The client lives in its own repository, [DebanganThakuria/narad-go](https://github.com/DebanganThakuria/narad-go), with its own README and API documentation in the source.
- It has no tagged release yet, so `go get` records a pseudo-version of the main branch, as above. Your `go.mod` pins it until you update on purpose.
- Its one direct dependency is the Prometheus client library, which the built-in [metrics](#metrics) use; the rest of the output is that library's dependencies.

Every example below uses this import and this message type:

```go
import narad "github.com/debanganthakuria/narad-go"

type Order struct {
    ID     string `json:"order_id"`
    Amount int    `json:"amount"`
}
```

## Connect to the cluster {#connect}

```go
client, err := narad.New("narad-0:7942,narad-1:7942,narad-2:7942",
    narad.WithAuth("billing-service", os.Getenv("NARAD_PASS")),
    narad.WithLogger(slog.Default()),
)
if err != nil {
    return err
}
defer client.Close()
```

- Give it every node you have, comma separated. The client spreads requests across them and routes around nodes that are failing, which is what makes a node restart invisible to your code. One address, such as a load balancer's, also works.
- The scheme defaults to `http`. Write `https://` in the address when TLS terminates in front of Narad.
- A `Client` is safe for concurrent use and owns its connection pool and per-node circuit breakers. Create one at startup and keep it; one per request throws both away.
- The client sets the `X-Narad-Client` header on every request, so the [cross-site guard](connect.md#required-headers) never refuses it.

## Produce messages {#produce}

```go
err := client.Produce(ctx, "orders",
    Order{ID: "ord_123", Amount: 4999},
    narad.WithKey("customer-42"))
```

- A struct or map is sent as JSON; a `[]byte` or a `string` is sent as it is. Prefer JSON: the broker returns valid JSON byte for byte, while a bare string comes back as a JSON string.
- `nil` means the broker wrote the message to disk before it answered. It does not mean anyone has consumed it.
- `narad.WithPartition(n)` pins a partition, as `?partition=` does over HTTP.

A produce whose reply was lost may still have been written, so retrying it can create a duplicate. The client retries by default, because a duplicate is recoverable and a lost message is not. `narad.Uncertain(err)` tells you when a failed produce may have been written anyway:

```go
if err := client.Produce(ctx, "payments", payment); err != nil {
    if narad.Uncertain(err) {
        // It may have been written. Reconcile before sending it again.
    }
    return err
}
```

When a duplicate is the worse outcome, `narad.WithCautiousRetries()` stops the client from retrying requests that may already have been applied.

## Run a consumer loop {#consume-loop}

`Consume` takes messages and hands them to your handler until its context ends:

```go
ctx, stop := signal.NotifyContext(context.Background(),
    syscall.SIGINT, syscall.SIGTERM)
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

Returning `nil` acks the message. Returning an error hands it straight back for redelivery instead of waiting out the visibility timeout. `Consume` returns `nil` when the context is cancelled, which is a clean shutdown.

| What `Consume` does | Why |
|---|---|
| Long-polls, 10 seconds per poll by default | An idle consumer waits on the broker instead of spinning |
| Renews the lease while a handler runs | Slow work keeps its message instead of losing it midway |
| Cancels the handler's context if the lease is lost | Another consumer may have the message now; carrying on would do the work twice |
| Recovers from a panicking handler | One bad message does not take the consumer down |
| Gives in-flight handlers 30 seconds on shutdown | They can finish and ack; anything still running after that is redelivered |

--8<-- "contract/at-least-once.md"

Check the context in slow handlers. Once it is cancelled, the lease may be gone and another consumer may already be doing the work:

```go
for _, chunk := range chunks {
    if ctx.Err() != nil {
        return ctx.Err()
    }
    transcode(chunk)
}
```

### Take one message

When a loop is not what you want, take a single message and settle it yourself:

```go
msg, err := client.Receive(ctx, "orders")
if err != nil {
    return err
}
var order Order
if err := msg.Into(&order); err != nil {
    return msg.Nack(ctx) // back to the queue at once
}
if err := process(ctx, order); err != nil {
    return msg.Nack(ctx)
}
return msg.Ack(ctx)
```

`Receive` waits until a message arrives or the context ends, so give the context a deadline if you do not want to wait forever. The message's lease lasts the topic's visibility timeout; call `msg.Extend(ctx)` to renew it during slow work.

## Wrap messages in envelopes {#envelopes}

An envelope carries your message together with an id, the time it was produced, an attempt count, the last error and any headers you add:

```go
err := client.Produce(ctx, "orders", order,
    narad.WithEnvelope(),
    narad.WithHeaders(map[string]string{"trace": traceID}),
)
```

- The id is a UUIDv7, so ids sort in the order they were created, and it appears in every error and log line the client writes about that message.
- `msg.Into(&v)` unwraps the envelope for you, so a handler reads enveloped and plain messages the same way. `msg.Envelope()` returns the envelope's own fields.
- `Attempts` starts at 1. Redelivery never changes it, because the broker never rewrites a stored message.

`client.Retry` is what moves the count. It produces a copy with `Attempts` raised and the error recorded, then acks the original, in that order. Point it at a delay child for backoff, or at another topic to park the message:

```go
env, err := msg.Envelope()
if err == nil && env.Attempts >= 5 {
    return client.Retry(ctx, msg, cause, "orders-parked")
}
return client.Retry(ctx, msg, cause, "orders-retry-30s")
```

[Handle retries and dead letters](handling-retries.md#backoff-topics) explains the retry topics this builds on.

An envelope is a different shape from your message, so producing one to a topic with a [schema](schemas.md) is refused. `WithEnvelopeSchema` registers your schema wrapped in the envelope's, and the broker then checks both. Every producer to that topic must then use an envelope.

```go
_, err := client.EnsureTopic(ctx, "orders",
    narad.WithPartitionCount(6),
    narad.WithEnvelopeSchema(schema),
)
```

## Manage topics {#topics}

```go
// At startup: every replica races to create it, and one wins.
topic, err := client.EnsureTopic(ctx, "orders",
    narad.WithPartitionCount(6),
    narad.WithRetention(24*time.Hour),
    narad.WithVisibilityTimeout(30*time.Second),
)

topic, err = client.Topic(ctx, "orders") // with per-partition state
topics, err := client.Topics(ctx)        // follows every page
err = client.DeleteTopic(ctx, "orders")
```

`EnsureTopic` treats a topic that already exists as success and returns what is there; `CreateTopic` reports `narad.ErrExists` instead. `narad.WithParent` and `narad.WithDelay` create a [fan-out or delay child](fanout-and-delay.md#create-child), and `SetSchema` registers a new [schema version](schemas.md#evolve).

## Replay history {#replay}

Reading at an offset takes nothing off the queue, so the message holds no lease and must not be acked:

```go
err := client.ReadFrom(ctx, "orders", 0, 0, narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        return audit(msg)
    }))
```

`ReadFrom` reads one partition from an offset to the current end of its log. `ReadAt` reads a single offset and returns `nil` at the end of the log and `narad.ErrOffsetGone` for an offset retention deleted. `Replay` reads every partition of a topic. [Replay messages from an offset](replay.md) covers the same reads over HTTP and the CLI.

## Handle errors {#errors}

Three questions, answered without matching on strings:

```go
narad.Retryable(err)              // worth trying again
narad.Uncertain(err)              // might have happened anyway
errors.Is(err, narad.ErrNotFound) // which assumption was wrong
```

| Sentinel | HTTP status |
|---|---|
| `ErrBadRequest` | `400`, `415` |
| `ErrUnauthenticated` | `401` |
| `ErrForbidden` | `403` |
| `ErrNotFound` | `404` |
| `ErrExists` | `409` |
| `ErrLeaseLost` | `410` on ack, extend or nack |
| `ErrOffsetGone` | `410` on a replay read |
| `ErrTooLarge` | `413` |
| `ErrThrottled` | `429` |
| `ErrUnavailable` | `421`, `503` |
| `ErrServer` | other `5xx` |

`ErrNoLease` (settling a replayed message), `ErrNoEnvelope`, `ErrNoNodes` (every node's circuit breaker is open) and `ErrClosed` come from the client itself. `*narad.Error` carries the status, the broker's message and the node that answered; `*narad.ConnError` covers requests that never got a reply. [Status codes and errors](../reference/status-codes.md) explains each status.

A lost lease is the error worth handling on purpose, because the work is going to someone else:

```go
if err := msg.Ack(ctx); errors.Is(err, narad.ErrLeaseLost) {
    // Too slow: the message is back in the queue and will be
    // processed again. Nothing to undo, since handlers are idempotent.
    return nil
}
```

## Metrics and logs {#metrics}

Logging takes any logger with `slog`'s method set, so the standard library needs no adapter: `narad.WithLogger(slog.Default())`. The client logs little: retries and node state changes at warn, and failures a consumer cannot report any other way at error.

Prometheus metrics are built in:

```go
metrics := narad.NewMetrics(prometheus.DefaultRegisterer)
client, err := narad.New(addr, narad.WithEvents(metrics.Observe))
```

That exports `narad_requests_total`, `narad_request_duration_seconds`, `narad_retries_total` and `narad_node_up`. Pass `narad.WithMetricsPrefix("payments")` to `NewMetrics` to put a prefix in front of each name, which two clients in one process need. `narad.WithEvents` takes any function, so you can report somewhere else instead.

## Tune the client {#tuning}

Nothing here is required; the defaults suit an ordinary service.

| Option | Default | What it sets |
|---|---|---|
| `WithTimeout` | 10 s | How long one attempt may take. Long polls are not cut short by it |
| `WithRetries` | 4 | Attempts per request, the first included. 1 turns retries off |
| `WithBackoff` | 50 ms, 5 s | The first retry delay and its ceiling, spread at random across the interval |
| `WithBreaker` | 5 failures, 2 s | How many failures in a row take a node out of rotation, and for how long |
| `WithIdleConnections` | 64 per node | Idle connections kept for reuse; long polls hold one each |
| `WithWorkers` (consume) | 1 | Messages handled at once |
| `WithWait` (consume) | 10 s | How long one poll waits for a message |
| `WithShutdownGrace` (consume) | 30 s | How long `Consume` waits for handlers after its context ends |

Retries are spread at random because, when a broker sheds load, every client retries; without the spread they all retry in the same millisecond and knock it over again. Breakers are per node, so one dead node costs that node and not the cluster.

## Next steps

- [Handle retries and dead letters](handling-retries.md): build retry tiers and a dead-letter topic around `client.Retry`.
- [Consume and acknowledge messages](consuming.md): what the client does for you, over plain HTTP.
- [narad-go on GitHub](https://github.com/DebanganThakuria/narad-go): the source, the README and every option's documentation.
