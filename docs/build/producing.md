---
description: "Send a message to a topic with one HTTP POST; the request body is the message, stored byte for byte."
search:
  boost: 2
---

# Produce messages

Send a message to a topic with one HTTP `POST`; the request body is the message, stored byte for byte.

Before you start: the topic must exist ([Manage topics](topics.md)), and your user needs a `produce` grant that matches it.

## Produce one message {#produce-one}

=== "curl"

    ```sh title="Produce a message to the orders topic"
    curl -i -u "$AUTH" -X POST \
      "$NARAD/v1/topics/orders/produce?key=customer-42" \
      -H "Content-Type: application/json" \
      -d '{"order_id": "ord_123", "amount": 4999}'
    ```

    ```http title="Response"
    HTTP/1.1 202 Accepted
    Date: Mon, 28 Sep 2026 19:21:17 GMT
    Content-Length: 0
    ```

=== "Go SDK"

    ```go
    err := client.Produce(ctx, "orders",
        Order{ID: "ord_123", Amount: 4999},
        narad.WithKey("customer-42"))
    ```

    A struct is sent as JSON; a `[]byte` or a `string` is sent as it is. `nil` means the broker answered `202`.

=== "CLI"

    ```sh
    narad pub orders '{"order_id": "ord_123", "amount": 4999}' \
      --key customer-42
    ```

    ```text title="Output"
    accepted (39 bytes)
    ```

What to put in place of each part:

- `$NARAD` and `$AUTH` are the base URL and credentials from [Connect and authenticate](connect.md). Any node accepts any produce.
- `orders` is the topic. A topic that does not exist gets [`404`](../reference/status-codes.md#status-404).
- `customer-42` is the optional [key](../reference/glossary.md#key), described [below](#keys).
- `Content-Type` must be `application/json` or `application/octet-stream`, or the request gets [`415`](../reference/status-codes.md#status-415); see [required headers](connect.md#required-headers). It does not change how the body is stored.

--8<-- "contract/produce-202.md"

What to do with each answer:

- **`202`**: nothing. The message will be delivered; sending it again creates a second copy.
- **A timeout, a dropped connection or a `5xx`**: the message may or may not have been accepted. Retry with backoff, and let consumers absorb the duplicate a retry can create.
- **A `4xx`**: fix the request. Sending it unchanged gets the same answer, except [`429`](../reference/status-codes.md#status-429), which asks you to slow down.

[Status codes and errors](../reference/status-codes.md#retry-rules) lists every code a produce can get and whether to retry it.

## Keys {#keys}

The key goes in the query string. Messages with the same key go to the same [partition](../reference/glossary.md#partition) in normal operation, and [fan-out](fanout-and-delay.md) keeps them together in each child topic.

--8<-- "contract/no-ordering.md"

- A key does not have to be text. Percent-encode any bytes into `?key=`; a key that is not valid UTF-8 is stored as sent.
- `?partition=N` pins the message to one partition and overrides the key. Most applications never need it. A partition the topic does not have gets [`400`](../reference/status-codes.md#status-400) with `invalid argument: partition out of range`.
- Keys come back to consumers as described under [payload encoding](consuming.md#payload-encoding).

### Messages without a key

**New in v3.1.0.**

With no key and no `partition`, Narad spreads messages over the topic's partitions in turn. Each node keeps its own rotation per topic, starting at a random partition, so a producer that writes several topics still spreads each of them evenly. The message is stored with no key, and consumers get no `key` field for it.

v3.0.1 and earlier invent a key of the form `key-<n>` for a message sent without one, and consumers see that key. Messages stored that way keep it after an upgrade.

## Binary payloads {#binary-payloads}

The body can be any bytes up to 1 MiB (1,048,576 bytes; one byte more gets [`413`](../reference/status-codes.md#status-413)): JSON, protobuf, plain text or an image. Send binary data with `application/octet-stream` and `--data-binary`:

```sh title="Produce a PNG file"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/produce?key=customer-42" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @receipt.png
```

```http title="Response"
HTTP/1.1 202 Accepted
Date: Mon, 28 Sep 2026 19:44:48 GMT
Content-Length: 0
```

- Use `--data-binary`, not `-d`: curl's `-d @file` strips newlines and carriage returns from the file.
- Nothing is encoded on the client. A consumer gets JSON back verbatim, other text as a JSON string, and binary as base64 with a flag; [payload encoding](consuming.md#payload-encoding) shows each case.
- If the topic has a [schema](schemas.md), the body must be JSON that validates against it, or the produce gets [`400`](../reference/status-codes.md#status-400).
- An empty body gets [`400`](../reference/status-codes.md#status-400) with `message required`.

## Produce a batch {#produce-batch}

**New in v3.1.0.**

A batch sends up to 100 messages in one request (1,000 from the next release, unreleased), and they share one write to disk:

```sh title="Produce two messages in one request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/produce/batch" \
  -H "Content-Type: application/json" \
  -d '{"messages": [
        {"key": "customer-42",
         "payload": {"order_id": "ord_125", "amount": 1250}},
        {"key": "customer-7",
         "payload": {"order_id": "ord_126", "amount": 800}}
      ]}'
```

```http title="Response"
HTTP/1.1 202 Accepted
Content-Length: 15
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:22:32 GMT

{"accepted":2}
```

The `202` makes the same promise as a single produce, for every message in the batch.

- **Each message** has a `payload`, and optionally a `key` and a `partition`. A payload that is JSON is stored exactly as written, so the string `"hi"` is stored as four bytes, quotes included. Anything else goes as a base64 string with `"payload_encoding": "base64"`, and is stored as the decoded bytes. A key that is not valid UTF-8 goes as base64 with `"key_encoding": "base64"`. The full field list is in the [HTTP API reference](../reference/http-api.md#produce-batch).
- **All or nothing.** Every message is checked before any is stored. If one fails, the request gets the status a single produce of that message would get, with its position at the front of the error (`message 3: ...`), and nothing is stored.
- **Ambiguous failures cover the whole batch.** After a timeout or a `5xx`, some or all of the batch may have been accepted, so a retry can duplicate part of it.
- **Order.** Messages that share a key reach their partition in batch order in normal operation. That is how it usually behaves, not a guarantee.
- **Limits.** More than 100 messages, or a `key` or `partition` in the query string, gets [`400`](../reference/status-codes.md#status-400). The whole body counts against the 1 MiB cap.
- **Larger and compressed batches (unreleased).** A node on master takes up to 1,000 messages in a body of up to 16 MiB, with each message's decoded payload at most 1 MiB, the single-produce cap, so anything a single produce accepts fits in a batch. A larger payload gets [`413`](../reference/status-codes.md#status-413) for the whole batch (`message 3: message too large`), and more than 1,000 messages `400`. The body may be sent with `Content-Encoding: zstd` or `gzip`, decoded under the same cap; another encoding gets [`415`](../reference/status-codes.md#status-415). A body over 1 MiB first takes its share of the node's budget for large bodies, `http.max_batch_body_bytes_in_flight` (256 MiB by default), and gets [`503`](../reference/status-codes.md#status-503) with `Retry-After: 1` while it is full; retry it. Bodies of 1 MiB or less never touch it. A v3.1.0 node refuses all of these, so keep to 100 messages, 1 MiB and no compression while a client can reach one. Larger batches roll the write-ahead log's segments more often, so the case where a failed batch still delivers its leading messages comes up a little more often; a retry duplicates, and nothing is lost.
- **Speed.** A batch waits for one disk sync however many messages it carries. Measured at the write-ahead log on macOS with one caller, that came to about 51 µs per message in batches of 100 and 0.5 ms per message in batches of 10, against about 4.7 ms for a single produce. For a single message, a plain produce is cheaper.
- **Older nodes.** A node on v3.0.1 or earlier answers [`404`](../reference/status-codes.md#status-404) to a batch. Keep single produces for as long as a client can reach such a node.

## Produce throughput {#throughput}

- Send produces concurrently, on one connection or many. A node handles them in parallel and groups them into shared disk syncs.
- Batch messages that are ready at the same time.
- Keep payloads small. The 1 MiB cap is a ceiling, not a target, and a large payload slows every step it passes through.
- Compressed or encrypted payloads are fine. If the operator turns on Narad's disk compression, it will not shrink them further.
- An operator can cap concurrent produces per user and node with `http.max_produce_in_flight_per_identity` (from v3.1.0; off by default). Past the cap a produce gets [`429`](../reference/status-codes.md#status-429).

## Next steps

- [Consume and acknowledge messages](consuming.md): take the message back out and settle it.
- [Status codes and errors](../reference/status-codes.md): every answer a produce can get, and which to retry.
- [Enforce schemas on a topic](schemas.md): have the broker refuse payloads that do not fit.
