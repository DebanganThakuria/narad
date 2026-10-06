---
description: "Take messages from a topic, process them, and acknowledge each one so that Narad never delivers it again."
search:
  boost: 2
---

# Consume and acknowledge messages

Take messages from a topic, process them, and acknowledge each one so that Narad never delivers it again.

Before you start: your user needs a `consume` grant that matches the topic. The same grant covers acks, extends and nacks.

## Consume a message {#consume}

=== "curl"

    ```sh title="Take the next message from orders"
    curl -i -u "$AUTH" "$NARAD/v1/topics/orders/consume?wait=10s"
    ```

    ```http title="Response"
    HTTP/1.1 200 OK
    Content-Length: 180
    Content-Type: application/json
    Date: Mon, 28 Sep 2026 19:21:18 GMT

    {
      "topic": "orders",
      "partition": 4,
      "offset": 0,
      "key": "customer-42",
      "payload": {
        "order_id": "ord_123",
        "amount": 4999
      },
      "timestamp": 1790623277,
      "receipt_handle": "4:0:3327358780392153864"
    }
    ```

=== "Go SDK"

    ```go
    err := client.Consume(ctx, "orders", narad.HandlerFunc(
        func(ctx context.Context, msg *narad.Message) error {
            var order Order
            if err := msg.Into(&order); err != nil {
                return err
            }
            return process(ctx, order)
        }))
    ```

    `Consume` long-polls, renews the lease while the handler runs, acks when it returns `nil` and gives the message back when it returns an error. [Go SDK](go-sdk.md#consume-loop) has the details.

=== "CLI"

    ```sh
    narad sub orders
    ```

    ```text title="Output"
    [p4 @0] key=customer-42 03:06:49 {"order_id": "ord_123", "amount": 4999}
    ```

    `narad sub` consumes and acks every message it prints, so it competes with your real consumers. Use `narad sub orders --peek` to watch without taking anything; see [Replay messages](replay.md#peek).

What each part does:

- `wait=10s` holds the request open for up to 10 seconds until a message arrives. If none does, the answer is [`204`](../reference/status-codes.md#status-204) with no body. Without `wait`, the answer comes at once. Loop on it: an idle consumer costs one small request per `wait`.
- The node caps `wait` at the operator's `http.max_consume_wait`, 10 seconds by default. A longer `wait` is not an error; the answer comes at the cap and carries an `X-Narad-Wait-Clamped: 10s` header, so an early `204` has a visible reason.
- `receipt_handle` proves you hold the message. Treat it as opaque, send it back to ack, and never parse it.
- From now on the message is hidden from other consumers for the topic's visibility timeout, 30 seconds by default. That is your [lease](../reference/glossary.md#lease): finish and ack within it, or [extend it](#extend).
- `partition=N` takes messages from that partition only. Every parameter and response field is in the [HTTP API reference](../reference/http-api.md#consume).

Run as many consumer processes as you like against the same topic. There are no consumer groups or partition assignments to configure: Narad hands each message to one consumer at a time.

--8<-- "contract/no-ordering.md"

The [message lifecycle](../get-started/concepts.md#message-lifecycle) shows every state a message passes through, from produce to ack.

--8<-- "contract/at-least-once.md"

<figure class="nr-dia nr-dia--doc" id="fig-consuming-lease-timeline">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--sky">
--8<-- "diagrams/consuming-lease-timeline.html"
</div>
<figcaption>Each consume starts a 30 s lease. An ack inside it settles the message, an extend restarts a full 30 s from now, and a nack gives the message back at once; once the lease runs out, the message belongs to the next consumer and a late ack gets <code>410 Gone</code>.</figcaption>
</figure>

## Acknowledge a message {#ack}

=== "curl"

    ```sh title="Ack the message you consumed"
    curl -i -u "$AUTH" -X POST \
      "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE" \
      -H "Content-Type: application/json"
    ```

    ```http title="Response"
    HTTP/1.1 204 No Content
    Date: Mon, 28 Sep 2026 19:21:27 GMT
    ```

=== "Go SDK"

    ```go
    err := msg.Ack(ctx)
    ```

    `Consume` acks for you. Call `Ack` yourself only on a message you took with `Receive`.

`$HANDLE` is the `receipt_handle` from the consume response, here `4:0:3327358780392153864`. A `204` means the message is settled and will not be delivered again. Acks may arrive in any order.

An ack that comes after the lease ran out, or for a message that is already acked, gets [`410`](../reference/status-codes.md#status-410):

```http title="Response"
HTTP/1.1 410 Gone
Content-Length: 67
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:21:27 GMT

{"error":"receipt handle no longer matches an active reservation"}
```

A `410` is not something to fix or retry. The message went back to the queue and may already be with another consumer, so your work on it may run twice; that is why handlers must be idempotent. An ack that gets [`502`](../reference/status-codes.md#status-502) or [`503`](../reference/status-codes.md#status-503) is different: it may not have reached the partition's owner, and you must [retry it](handling-retries.md#retry-acks).

## Extend a lease {#extend}

A slow job can keep its message by renewing the lease instead of raising the topic's timeout for everyone:

```sh title="Restart the visibility window"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE&extend=true" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:21:27 GMT
```

- A `204` gives the message a full visibility timeout again, counted from now. Extend about every third of the timeout while you work, then ack with the same handle.
- A `410` means the lease already ran out. Stop working on the message; it belongs to someone else now.
- The Go SDK's `Consume` extends for you while a handler runs.

## Give a message back {#nack}

When this consumer cannot handle a message right now, because it is shutting down or a dependency is down, hand it back at once instead of waiting out the lease:

```sh title="Nack the message"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE&extend=0" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:21:45 GMT
```

The message can be consumed again immediately, and a consumer waiting in a long poll gets it right away, with a new receipt handle. In the Go SDK, `msg.Nack(ctx)` does the same, and so does a `Consume` handler that returns an error.

A nack does not count anything. A message that always fails is nacked and delivered again forever; [Handle retries and dead letters](handling-retries.md#envelope) shows how to bound the attempts.

## Consume a batch {#consume-batch}

**New in v3.1.0.**

```sh title="Take up to 10 messages in one request"
curl -i -u "$AUTH" -H 'X-Narad-Client: curl' "$NARAD/v1/topics/orders/consume?wait=10s&max=10"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 373
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:22:32 GMT

{
  "messages": [
    {
      "topic": "orders",
      "partition": 3,
      "offset": 0,
      "key": "customer-7",
      "payload": {
        "order_id": "ord_126",
        "amount": 800
      },
      "timestamp": 1790623352,
      "receipt_handle": "3:0:6999714737712221180"
    },
    {
      "topic": "orders",
      "partition": 4,
      "offset": 4,
      "key": "customer-42",
      "payload": {
        "order_id": "ord_125",
        "amount": 1250
      },
      "timestamp": 1790623352,
      "receipt_handle": "4:4:9069778768564137076"
    }
  ]
}
```

- A batch consume must send an `X-Narad-Client` header, with any value, or it gets [`400`](../reference/status-codes.md#status-400). The Go SDK and the CLI send it. A consume reserves messages, and a page on another site could send a `GET` with an operator's cached Basic credentials; the header forces a CORS preflight, which Narad never approves. A single consume does not need it.
- `max` is 1 to 100. Each message in `messages` is exactly what a single consume returns, with its own receipt handle and its own lease, and you settle each one on its own or [in a batch](#ack-batch). Any `max`, `max=1` included, gets this shape; without `max` you get one message, as before.
- A batch is what is ready now. The request is never held to fill it, so expect fewer than `max` messages. When nothing is ready it long-polls like a single consume and answers `204` if `wait` runs out.
- A batch stops early when its messages add up to about 4 MiB, and a response carries at most 8 MiB. The first message always goes, whatever its size.
- `max` works with `partition=N` but not with `offset`, which [replays](replay.md) one message; that combination gets [`400`](../reference/status-codes.md#status-400).
- A batch request counts as `max` requests (or as the whole limit, when `max` is larger) against your per-node limit on concurrent consumes, and each message counts against its partition's `max_in_flight_per_partition`.
- A node on v3.0.1 or earlier ignores `max` and answers one message in the single-message shape. Switch a client over once every node it can reach is upgraded.

## Acknowledge a batch {#ack-batch}

**New in v3.1.0.**

Leave out the `receipt_handle` parameter and send the handles in a JSON body instead:

```sh title="Ack three messages in one request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/ack" \
  -H "Content-Type: application/json" \
  -d '{"receipt_handles": ["3:0:6999714737712221180",
                          "4:4:9069778768564137076",
                          "4:0:3327358780392153864"]}'
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 124
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:22:37 GMT

{
  "results": [
    {
      "status": 204
    },
    {
      "status": 204
    },
    {
      "status": 410,
      "error": "receipt handle no longer matches an active reservation"
    }
  ]
}
```

- Send 1 to 100 handles in a body of at most 64 KiB. `extend=true` or `extend=0` in the query applies to every handle.
- The answer is `200` with one result per handle, in request order. Each `status` is what a single ack of that handle would have answered, and `error` explains any status other than `204`. One stale handle never fails the others; here the third was already acked.
- Retry only the handles whose result is `502` or `503`.
- A body that is malformed as a whole (invalid JSON, no handles, more than 100) gets [`400`](../reference/status-codes.md#status-400), and a body over 64 KiB gets [`413`](../reference/status-codes.md#status-413).
- A node on v3.0.1 or earlier answers `400` with `receipt_handle required`. As with batch consume, switch over once every node is upgraded.

## Payload encoding {#the-payload-comes-back-the-way-you-sent-it}

The `payload` field comes back in the form that carries the produced bytes exactly.
{: #payload-encoding }

| You produced | You consume |
|---|---|
| A JSON value, such as `{"a":1}`, `[1,2]`, `"hi"` or `42` | That JSON, byte for byte |
| Text that is not JSON, such as `hello world` | A JSON string: `"payload":"hello world"` |
| Binary data, such as an image or protobuf | A base64 string, flagged with `"payload_encoding":"base64"` |

Two consumes of the text and binary messages from [Produce messages](producing.md#binary-payloads):

```json title="Response bodies"
{
  "topic": "orders",
  "partition": 4,
  "offset": 2,
  "key": "customer-42",
  "payload": "hello world",
  "timestamp": 1790623346,
  "receipt_handle": "4:2:3685415247247279671"
}

{
  "topic": "orders",
  "partition": 4,
  "offset": 3,
  "key": "customer-42",
  "payload": "iVBORw0KGgo=",
  "payload_encoding": "base64",
  "timestamp": 1790623346,
  "receipt_handle": "4:3:5733584771904418009"
}
```

The rule for consumers: if `payload_encoding` is `"base64"`, decode the payload; otherwise use it as it is.

Keys follow the same idea. `key` is present only when the message has one, as a JSON string.

**New in v3.1.0.** A key that is not valid UTF-8 comes back base64-encoded with `"key_encoding":"base64"` beside it. During a rolling upgrade, a message whose partition owner still runs v3.0.1 comes back the old way, without `key_encoding`.

## Flow control {#flow-control}

Three limits keep one consumer from taking more than it can finish:

- **`max_in_flight_per_partition`** (topic setting, default 1024): once that many messages from one partition are out and unacked, the partition hands out nothing more until acks arrive.
- **`max_acked_ahead_per_partition`** (topic setting, default 1024): the number of acks a partition holds for messages after the oldest unacked one. At the limit, the partition hands out only that oldest message until it is acked. Acks for messages you already hold are always accepted.
- **Concurrent consumes per identity**, 1024 per node by default: past it, a consume gets [`429`](../reference/status-codes.md#status-429). A long poll counts for as long as it waits.

The two topic settings can be [changed at any time](topics.md#alter). Lag and in-flight counts per partition are in the [metrics reference](../reference/metrics.md#queue-health).

## Next steps

- [Handle retries and dead letters](handling-retries.md): bound retries, back off, and retry failed acks.
- [Replay messages from an offset](replay.md): read history without taking leases.
- [Go SDK: produce and consume from Go](go-sdk.md): a consumer loop that manages leases for you.
