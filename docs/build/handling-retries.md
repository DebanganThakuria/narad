---
description: "Bound retries, space them out and park messages that keep failing, using only topics, leases and acks in your consumer."
search:
  boost: 2
---

# Handle retries and dead letters

Bound retries, space them out and park messages that keep failing, using only topics, leases and acks in your consumer.

Before you start: a consumer that consumes and acks ([Consume and acknowledge messages](consuming.md)), and a `create` grant for the extra topics the patterns below use.

Narad has no retry engine and no built-in dead-letter queue. Every retry policy is built from three things it does have: the [lease](../reference/glossary.md#lease), the ack and the topic. The policy then lives in your consumer, where you can read it, log it and change it without a broker upgrade.

--8<-- "contract/at-least-once.md"

| You want | Use |
|---|---|
| Survive a consumer crash | Nothing: [the lease retries](#lease) |
| Ride out a brief failure | [Retry in process](#in-process), extending the lease |
| Let another worker try | [Nack](#nack) |
| A limit on attempts | [A counter in an envelope](#envelope) |
| A pause between attempts | [Retry topics with a delay](#backoff-topics) |
| A place for messages that keep failing | [A dead-letter topic](#dead-letter) |

## Retry failed acks {#retry-acks}

This one is not optional. An ack that fails is not a failed job: the work is done, and only the ack did not arrive. Retry the ack itself with a short backoff until it gets `204` or `410`:

```sh title="Ack with retries on 5xx and timeouts"
curl -i --retry 5 --retry-max-time 30 -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:35:28 GMT
```

- `$HANDLE` is the `receipt_handle` from the consume response. curl's `--retry` resends on a timeout and on `408`, `429`, `500`, `502`, `503` and `504`, and not on `410`.
- [`503`](../reference/status-codes.md#status-503) on an ack means the partition's owner is unreachable right now. [`502`](../reference/status-codes.md#status-502) means the node you reached forwarded the ack and got no answer in time. Both are safe to retry: an ack applied twice gets `410` the second time.
- [`410`](../reference/status-codes.md#status-410) means the lease already ran out and the message went back to the queue. Do not retry it; the work may run twice, which your handler already tolerates.
- The Go SDK's `msg.Ack` retries for you, and treats a `410` that follows a lost reply as the success it almost certainly was.

An ack you drop becomes a redelivery when the lease runs out, and the redelivered message needs an ack of its own. In July 2026 one of our own test harnesses did not retry failed acks and caused 623,000 duplicate deliveries over seven hours. Nothing was lost: each dropped ack came back as one more delivery when its lease ran out.

## Let the lease retry {#lease}

If a consumer crashes, hangs or never acks, the message becomes available again when its visibility timeout ends (30 seconds by default), and another consumer takes it. That needs no code.

- Spacing: fixed, equal to the visibility timeout.
- Attempts: unlimited. A message that always fails comes back forever until someone notices, so use this alone only while prototyping.

## Retry in process {#in-process}

For a brief failure, such as one timed-out call to a dependency, call it again inside the handler while you still hold the lease:

```text
for attempt in 1..3:
    if handle(msg) succeeds: ack; done
fall through to a nack or a requeue (below)
```

If the retries could outlast the lease, [extend it](consuming.md#extend) between attempts. In-process retries fix brief failures only; a dependency that is down for ten minutes needs one of the patterns below.

## Nack to another consumer {#nack}

`POST /ack?receipt_handle=$HANDLE&extend=0` gives the message back at once, and the next consume can take it ([Give a message back](consuming.md#nack)). Use it when this worker is the problem, for example while it shuts down, rather than the message.

A redelivered message is the same bytes as before, so nothing counts attempts for you. The next pattern does.

## Count attempts in an envelope {#envelope}

Wrap the payload when you produce it, with a counter beside it:

```json
{"payload": {"order_id": "ord_123"}, "delivery_count": 0}
```

When processing fails, produce a copy with the counter raised, then ack the original:

```sh title="Step 1: produce the copy"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders-retry-30s/produce?key=customer-42" \
  -H "Content-Type: application/json" \
  -d '{"payload": {"order_id": "ord_123"}, "delivery_count": 1}'
```

```http title="Response"
HTTP/1.1 202 Accepted
Date: Mon, 28 Sep 2026 19:35:28 GMT
Content-Length: 0
```

```sh title="Step 2: ack the original"
curl -i --retry 5 --retry-max-time 30 -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:35:28 GMT
```

This example sends the copy to the retry topic of the [next section](#backoff-topics). To retry at once instead, produce the copy to `orders` itself.

!!! warning "Produce the copy first, then ack"
    A crash between the two steps in this order leaves a duplicate, which your idempotent handler absorbs. The reverse order can ack the original and then lose the copy, and the message is gone.

What the counter gives you:

- **A limit on attempts.** The count travels with the message, so no broker state is needed. When it reaches your limit, send the message to a [dead-letter topic](#dead-letter) instead.
- **No blocked queue.** The copy joins the end of the topic, so the messages behind the failed one move on.
- **Your own policy.** A different limit per error type, or a dead-letter topic per failure class, is your JSON and your code.

The Go SDK has an envelope of its own, with an `attempts` count that starts at 1, and `client.Retry` does both steps for you. Use one envelope shape per topic; [Go SDK](go-sdk.md#envelopes) has the details.

## Back off with retry topics {#backoff-topics}

A copy produced back to `orders` is consumable again within milliseconds. When a failure needs time, such as a rate-limited API or a database failing over, put a [delay child](fanout-and-delay.md#delay-children) between attempts.

You cannot produce to a delay child: it gets `409`, because a direct write would skip the delay. A delay child's only source is its parent, and it receives everything the parent receives. So each backoff tier is a pair: a parent that carries nothing but retries, and its delay child.

```sh title="Create a 30-second retry tier"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders-retry-30s"}'

curl -i -u "$AUTH" -X POST "$NARAD/v1/topics" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders-retry-30s-run", "parent": "orders-retry-30s",
       "fanout_delay_ms": 30000}'
```

Both answer `201 Created`; the second response shows the link:

```json title="Response body of the second request"
{
  "name": "orders-retry-30s-run",
  "id": "cdb49d56b22576ff",
  "partitions": 3,
  "retention_ms": 604800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790624128,
  "owner": "billing-service",
  "role": "child",
  "parent": "orders-retry-30s",
  "attach_epoch": "97bef517460d2710",
  "fanout_delay_ms": 30000,
  "attach_offsets": [
    0,
    0,
    0
  ]
}
```

Your consumers read both `orders` and `orders-retry-30s-run`. On failure they produce the copy to `orders-retry-30s`, as in the envelope steps above. Thirty seconds after the copy was produced, it can be consumed from the child:

```sh title="Consume the delayed copy"
curl -i -u "$AUTH" \
  "$NARAD/v1/topics/orders-retry-30s-run/consume?wait=10s"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 212
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:36:01 GMT

{
  "topic": "orders-retry-30s-run",
  "partition": 1,
  "offset": 0,
  "key": "customer-42",
  "payload": {
    "payload": {
      "order_id": "ord_123"
    },
    "delivery_count": 1
  },
  "timestamp": 1790624158,
  "receipt_handle": "1:0:2190934716274811394"
}
```

The delay survives restarts of every node, and no timer in your process holds the message. For several tiers, create one pair per tier and pick the tier from the counter:

| Parent (produce here) | Delay child (consume here) | Delay | Use for |
|---|---|---|---|
| `orders-retry-30s` | `orders-retry-30s-run` | 30 s | `delivery_count` 1 to 2 |
| `orders-retry-5m` | `orders-retry-5m-run` | 5 min | `delivery_count` 3 to 4 |
| `orders-retry-30m` | `orders-retry-30m-run` | 30 min | `delivery_count` 5 and up |

- Do not attach several delay children to one parent to get tiers. Every child copies every message of its parent, so each failed message would come back once per tier.
- Tiers are fixed delays, not a delay per message. Two or three tiers cover most policies.
- The parent's retention must be at least the delay plus one hour. The default of 7 days covers delays of up to 6 days and 23 hours.

## Park failures in a dead-letter topic {#dead-letter}

A dead-letter topic is an ordinary topic, such as `orders-dlq`, that holds messages which reached your attempt limit:

```text
on failure:
    if msg.delivery_count >= MAX: produce msg to orders-dlq; ack; done
    produce {payload, delivery_count + 1} to orders-retry-30s; ack
```

- Alert on its backlog: `narad_consumer_lag_messages` for `orders-dlq` above zero means messages are waiting. The gauge is described in the [metrics reference](../reference/metrics.md#queue-health).
- Inspect it without taking anything with [replay](replay.md).
- Once the bug is fixed, consume from it and produce each message back to `orders`.

A typical production setup combines the patterns: retry in process for brief failures, then requeue through a 30-second tier, then park in the dead-letter topic after five attempts, with an alert on the dead-letter topic.

## Next steps

- [Fan out and delay messages](fanout-and-delay.md): the delay children that retry tiers are made of.
- [Replay messages from an offset](replay.md): read a dead-letter topic without consuming it.
- [Status codes and errors](../reference/status-codes.md): which answers to retry, for every endpoint.
